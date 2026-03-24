(ns tidb.mv-autosched
  (:refer-clojure :exclude [test])
  (:require [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]
             [store :as store]]
            [jepsen.checker.timeline :as timeline]
            [knossos.op :as op]
            [tidb.artifact :as artifact]
            [tidb.mview :as mv]
            [tidb.sql :as c]))

(def refresh-start-delay 2)
(def row-refresh-seconds 5)
(def agg-refresh-seconds 7)
(def purge-start-delay 2)
(def purge-next-seconds 11)
(def quiet-snapshot-count 7)
(def quiet-snapshot-interval-seconds 6)

(def group-count 4)
(def write-fns [:insert :update-value :update-value :move-group :delete])

(defonce autosched-schema-setup-by-test
  (atom {}))

(defn setup-key
  [test]
  [(or (:name test) ::unnamed-test)
   (or (some-> (:start-time test) str)
       (System/identityHashCode test))])

(defn setup-schema-once!
  [test f]
  (let [k (setup-key test)]
    (loop []
      (let [[mode setup-promise]
            (locking autosched-schema-setup-by-test
              (if-let [setup-promise (get @autosched-schema-setup-by-test k)]
                [:wait setup-promise]
                (let [setup-promise (promise)]
                  (swap! autosched-schema-setup-by-test assoc k setup-promise)
                  [:run setup-promise])))]
        (case mode
          :run
          (try
            (let [setup (f)]
              (deliver setup-promise {:ok setup})
              setup)
            (catch Throwable t
              (locking autosched-schema-setup-by-test
                (swap! autosched-schema-setup-by-test dissoc k))
              (deliver setup-promise {:error t})
              (throw t)))

          :wait
          (let [{:keys [ok error]} @setup-promise]
            (if error
              (throw error)
              ok)))))))

(defn process-id
  [process]
  (cond
    (integer? process) process
    (number? process)  (long process)
    :else              0))

(defn key-for-process
  [process]
  (inc (process-id process)))

(defn pad-for
  [token]
  (subs (str token "........................................................") 0 32))

(defn autosched-write
  [process seq]
  (let [id      (key-for-process process)
        f       (rand-nth write-fns)
        token   (str "mv-autosched-" process "-" seq "-" (rand-int 1000000))
        base-g1 (mod id group-count)
        move-g1 (mod (+ id seq 1) group-count)
        g1      (if (= :move-group f) move-g1 base-g1)
        v1      (+ (* 100000 id) seq)]
    {:type  :invoke
     :f     f
     :value {:id         id
             :g1         g1
             :v1         v1
             :version    seq
             :last-token token
             :deleted    (= :delete f)
             :pad        (pad-for token)}}))

(defn generator
  []
  (let [seqs (atom {})]
    (reify gen/Generator
      (op [_ _ process]
        (let [process (process-id process)
              seq     (get (swap! seqs update process (fnil inc 0)) process)]
          (autosched-write process seq))))))

(defn quiet-generator
  []
  ; `gen/each` gives every client process its own quiet-phase sequence instead
  ; of sharing a single global sequence across all clients.
  (gen/each
   (apply gen/concat
          (vec
           (mapcat (fn [i]
                     (cond-> []
                       (pos? i) (conj (gen/sleep quiet-snapshot-interval-seconds))
                       true     (conj (gen/once {:type :invoke :f :snapshot}))))
                   (range quiet-snapshot-count))))))

(defn snapshot-result
  [op]
  (or (:result op)
      (:value op)))

(defn ambiguous-write-error?
  [t]
  (let [chain   (take-while some? (iterate #(.getCause %) t))
        message (->> chain
                     (map #(.getMessage %))
                     (remove nil?)
                     (str/join " | "))
        type    (some (comp :type ex-data) chain)
        stack   (mapcat #(seq (.getStackTrace %)) chain)
        driver-batch-npe?
        (and (instance? NullPointerException t)
             (some (fn [^StackTraceElement frame]
                     (and (= "org.mariadb.jdbc.ClientSidePreparedStatement"
                             (.getClassName frame))
                          (= "executeBatch" (.getMethodName frame))))
                   stack))]
    (or (instance? java.sql.SQLTimeoutException t)
        (instance? java.sql.SQLNonTransientConnectionException t)
        (some #(instance? java.sql.BatchUpdateException %) chain)
        (some #(instance? java.sql.SQLTransientException %) chain)
        (some #(instance? InterruptedException %) chain)
        (= :connect-timed-out type)
        driver-batch-npe?
        (re-find #"Interrupted awaiting response|timed(?: |-)?out|Connection is closed|closed connection|Connection reset|broken pipe|Socket" message))))

(defn retryable-write-error?
  [t]
  (or (ambiguous-write-error? t)
      (instance? NullPointerException t)))

(def op-timeout-ms
  (+ c/socket-timeout 5000))

(defn setup-retryable-error?
  [t]
  (let [message (str (.getMessage t))]
    (or (ambiguous-write-error? t)
        (instance? java.sql.BatchUpdateException t)
        (re-find #"Resolve lock timeout|Information schema is changed|Region is unavailable" message))))

(defn close-conn-holder!
  [conn-holder]
  (loop []
    (when-let [conn @conn-holder]
      (if (compare-and-set! conn-holder conn nil)
        (c/abort! conn)
        (recur)))))

(defn run-with-op-timeout!
  [conn-holder node f]
  (let [worker (future
                 (try
                   {:ok (f)}
                   (catch Throwable t
                     {:error t})))
        result (deref worker op-timeout-ms ::timeout)]
    (if (= ::timeout result)
      (do
        (close-conn-holder! conn-holder)
        (future-cancel worker)
        (throw (ex-info (str "operation timed out after " op-timeout-ms " ms")
                        {:type :op-timed-out
                         :node node
                         :timeout-ms op-timeout-ms})))
      (if-let [t (:error result)]
        (throw t)
        (:ok result)))))

(defn ensure-conn!
  [conn-holder node test]
  (or @conn-holder
      (reset! conn-holder (c/open node test))))

(defn with-reconnect!
  [conn-holder node test retryable-error? f]
  (loop [tries 3]
    (let [result (try
                   (let [conn (ensure-conn! conn-holder node test)]
                     {:ok (run-with-op-timeout! conn-holder node #(f conn))})
                   (catch Throwable t
                     {:error t}))]
      (if-let [t (:error result)]
        (if (and (pos? tries) (retryable-error? t))
          (do
            (close-conn-holder! conn-holder)
            (recur (dec tries)))
          (throw t))
        (:ok result)))))

(defn stored-row-matches?
  [row {:keys [id g1 v1 version last-token deleted pad]}]
  (and row
       (= (long id) (long (:id row)))
       (= (long g1) (long (:g1 row)))
       (= (long v1) (long (:v1 row)))
       (= (long version) (long (:version row)))
       (= last-token (:last_token row))
       (= (if deleted 1 0) (long (:deleted row)))
       (= pad (:pad row))))

(defn verification-nodes
  [node test]
  (->> (cons node (:nodes test))
       (remove nil?)
       distinct))

(defn resolved-write-op
  [op resolution node]
  (assoc op
         :type :ok
         :resolved? true
         :result {:resolution resolution
                  :node node}))

(defn exception-summary
  [^Throwable t]
  (or (.getMessage t)
      (str t)))

(defn verify-write-on-node!
  [node test op]
  (let [conn (c/open node test)]
    (try
      (let [row (mv/query-stored-row conn (get-in op [:value :id]))]
        (when (stored-row-matches? row (:value op))
          (resolved-write-op op :read-back node)))
      (finally
        (c/close! conn)))))

(defn verify-write!
  [node test op]
  (some (fn [verify-node]
          (try
            (verify-write-on-node! verify-node test op)
            (catch Throwable _
              nil)))
        (verification-nodes node test)))

(defn upsert-sql
  []
  (str "INSERT INTO " mv/base-table " "
       "(id, g1, v1, version, last_token, deleted, pad) "
       "VALUES (?, ?, ?, ?, ?, ?, ?) "
       "ON DUPLICATE KEY UPDATE "
       "g1 = VALUES(g1), "
       "v1 = VALUES(v1), "
       "version = VALUES(version), "
       "last_token = VALUES(last_token), "
       "deleted = VALUES(deleted), "
       "pad = VALUES(pad)"))

(defn apply-write-on-conn!
  [node test conn op]
  (let [{:keys [id g1 v1 version last-token deleted pad]} (:value op)
        params [(upsert-sql)
                id g1 v1 version last-token (if deleted 1 0) pad]]
    (c/execute! conn params {:transaction? false})
    (assoc op :type :ok)))

(defn replay-write-on-node!
  [node test op]
  (let [conn-holder (atom nil)]
    (try
      (with-reconnect! conn-holder node test retryable-write-error?
        #(apply-write-on-conn! node test % op))
      (resolved-write-op op :replay node)
      (finally
        (close-conn-holder! conn-holder)))))

(defn replay-write!
  [node test op]
  (some (fn [replay-node]
          (try
            (replay-write-on-node! replay-node test op)
            (catch Throwable t
              (when (retryable-write-error? t)
                (verify-write! replay-node test op)))))
        (verification-nodes node test)))

(defn resolve-write!
  [node test op]
  (or (verify-write! node test op)
      (replay-write! node test op)))

(defn apply-write!
  [node test conn op]
  (try
    (apply-write-on-conn! node test conn op)
    (catch Throwable t
      (if (retryable-write-error? t)
        (or (resolve-write! node test op)
            (assoc op :type :info :error :indeterminate-write :exception (exception-summary t)))
        (throw t)))))

(defn apply-write-with-reconnect!
  [conn-holder node test op]
  (try
    (with-reconnect! conn-holder node test retryable-write-error?
      #(apply-write-on-conn! node test % op))
    (catch Throwable t
      (if (retryable-write-error? t)
        (or (resolve-write! node test op)
            (assoc op :type :info :error :indeterminate-write :exception (exception-summary t)))
        (throw t)))))

(defn query-db-now-ms
  [conn]
  (try
    (some-> (c/query conn ["SELECT CAST(ROUND(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 1000) AS SIGNED) AS now_ms"]
                      {:row-fn :now_ms})
            first
            double
            long)
    (catch Throwable _
      nil)))

(defn snapshot-state
  [node conn schedule-meta]
  (let [snapshot-at-ms (System/currentTimeMillis)
        db-now-ms      (query-db-now-ms conn)
        base-row       (mv/query-base-row-projection conn)
        mv-row         (mv/query-mv-row-projection conn)
        row-diff       (mv/full-row-diff base-row mv-row)
        base-agg       (mv/query-base-agg conn)
        mv-agg         (mv/query-mv-agg conn)
        agg-diff       (mv/agg-diff base-agg mv-agg)
        log-table      (:log-table schedule-meta)
        log-row-count  (when log-table
                         (mv/count-table-rows conn log-table))
        schedule-ddl   (mv/read-schedule-metadata conn schedule-meta)
        runtime-meta   (mv/read-runtime-metadata conn schedule-meta)]
    {:snapshot-at-ms       snapshot-at-ms
     :snapshot-node        (str node)
     :db-now-ms            db-now-ms
     :db-client-offset-ms  (when db-now-ms
                             (- db-now-ms snapshot-at-ms))
     :row-equal?           (empty? row-diff)
     :agg-equal?           (empty? agg-diff)
     :row-diff             (when (seq row-diff) row-diff)
     :agg-diff             (when (seq agg-diff) agg-diff)
     :base-row-count       (count base-row)
     :mv-row-count         (count mv-row)
     :base-agg-count       (count base-agg)
     :mv-agg-count         (count mv-agg)
     :row-hash             (hash mv-row)
     :agg-hash             (hash mv-agg)
     :log-table            log-table
     :log-row-count        log-row-count
     :schedule-metadata    schedule-ddl
     :runtime-metadata     runtime-meta
     :row-refresh-stmt     (:row-refresh-stmt schedule-meta)
     :agg-refresh-stmt     (:agg-refresh-stmt schedule-meta)
     :purge-schedule-stmt  (:purge-schedule-stmt schedule-meta)
     :mlog-stmt            (:mlog-stmt schedule-meta)}))

(defn snapshot-with-reconnect!
  [conn-holder node test op schedule-meta]
  (try
    (with-reconnect! conn-holder node test retryable-write-error?
      #(assoc op :type :ok :result (snapshot-state node % schedule-meta)))
    (catch Throwable t
      (assoc op :type :fail :error :snapshot-error :exception (exception-summary t)))))

(defn ordered-snapshot-ops
  [snapshots]
  (->> snapshots
       (sort-by (juxt #(get (snapshot-result %) :snapshot-at-ms)
                      #(get (snapshot-result %) :snapshot-node)
                      :process
                      :time))
       vec))

(defn snapshots-by-node
  [snapshots]
  (->> (ordered-snapshot-ops snapshots)
       (group-by #(get (snapshot-result %) :snapshot-node))
       (into (sorted-map))))

(defn stable-pair?
  [[a b]]
  (let [av (snapshot-result a)
        bv (snapshot-result b)]
    (and av bv
         (:row-equal? av)
         (:agg-equal? av)
         (:row-equal? bv)
         (:agg-equal? bv)
         (= (:row-hash av) (:row-hash bv))
         (= (:agg-hash av) (:agg-hash bv)))))

(defn stable-pair-present?
  [snapshots]
  (some (fn [[_ node-snapshots]]
          (some stable-pair? (partition 2 1 node-snapshots)))
        (snapshots-by-node snapshots)))

(defn- purge-progress-state
  [snapshot-ops]
  (let [counts (->> snapshot-ops
                    (map snapshot-result)
                    (map :log-row-count)
                    (filter some?))]
    (cond
      (empty? counts) :unknown
      (some true? (map (fn [[a b]] (> a b)) (partition 2 1 counts))) :decreased
      (zero? (last counts)) :empty
      :else false)))

(defn purge-progress
  [snapshots]
  (let [states (->> (snapshots-by-node snapshots)
                    vals
                    (map purge-progress-state))]
    (cond
      (empty? states) :unknown
      (some #{:decreased} states) :decreased
      (some #{:empty} states) :empty
      (every? #{:unknown} states) :unknown
      :else false)))

(defn- runtime-row
  [snapshot-value component]
  (some (fn [row]
          (when (= component (:component row))
            row))
        (get-in snapshot-value [:runtime-metadata :rows])))

(defn- runtime-progress-keys
  [component]
  (case component
    :log-purge   [:next-time-ms :last-purged-tso]
    :row-refresh [:next-time-ms :last-success-read-tso]
    :agg-refresh [:next-time-ms :last-success-read-tso]
    [:next-time-ms]))

(defn- runtime-progress?
  [snapshot-values component]
  (let [rows      (keep #(runtime-row % component) snapshot-values)
        key-paths (runtime-progress-keys component)]
    (if (seq rows)
      (let [first-state (select-keys (first rows) key-paths)]
        (boolean
         (some #(not= first-state (select-keys % key-paths))
               (rest rows))))
      :unknown)))

(defn- history-entry
  [snapshot-value component]
  (some (fn [row]
          (when (= component (:component row))
            row))
        (get-in snapshot-value [:runtime-metadata :recent-history])))

(defn- history-progress-keys
  [component]
  (case component
    :log-purge   [:job-id :status :end-time-ms :row-count]
    :row-refresh [:job-id :status :end-time-ms :read-tso :row-count :failed-reason]
    :agg-refresh [:job-id :status :end-time-ms :read-tso :row-count :failed-reason]
    [:job-id :status]))

(defn- history-progress?
  [snapshot-values component]
  (let [rows      (keep #(history-entry % component) snapshot-values)
        key-paths (history-progress-keys component)]
    (if (seq rows)
      (let [first-state (select-keys (first rows) key-paths)]
        (boolean
         (some #(not= first-state (select-keys % key-paths))
               (rest rows))))
      :unknown)))

(def runtime-components
  [:row-refresh :agg-refresh :log-purge])

(defn- distinct-selected-state-count
  [rows key-paths]
  (let [states (->> rows
                    (map #(select-keys % key-paths))
                    (remove empty?)
                    distinct
                    seq)]
    (when states
      (count states))))

(defn- component-recovery-summary
  [snapshot-values component runtime-advanced? history-advanced?]
  (let [runtime-rows          (keep #(runtime-row % component) snapshot-values)
        history-rows          (keep #(history-entry % component) snapshot-values)
        runtime-key-paths     (runtime-progress-keys component)
        history-key-paths     (history-progress-keys component)
        last-snapshot-at-ms   (:snapshot-at-ms (last snapshot-values))
        last-runtime          (last runtime-rows)
        last-history          (last history-rows)
        next-time-ms          (:next-time-ms last-runtime)
        last-history-end-time (:end-time-ms last-history)]
    (cond-> {:runtime-advanced?        runtime-advanced?
             :history-advanced?        history-advanced?
             :runtime-state-count      (distinct-selected-state-count runtime-rows runtime-key-paths)
             :history-state-count      (distinct-selected-state-count history-rows history-key-paths)
             :stalled-after-quiet?     (and (= false runtime-advanced?)
                                           (= false history-advanced?))}
      last-runtime
      (assoc :last-runtime-state (select-keys last-runtime runtime-key-paths))

      (and last-snapshot-at-ms next-time-ms)
      (assoc :next-time-overdue-ms (- last-snapshot-at-ms next-time-ms))

      last-history
      (assoc :last-history-state (select-keys last-history history-key-paths))

      (and last-snapshot-at-ms last-history-end-time)
      (assoc :last-history-age-ms (- last-snapshot-at-ms last-history-end-time)))))

(defn- recovery-diagnosis
  [snapshot-values]
  (let [component-summaries      (into (sorted-map)
                                       (keep (fn [component]
                                               (let [runtime-advanced? (runtime-progress? snapshot-values component)
                                                     history-advanced? (history-progress? snapshot-values component)
                                                     summary           (component-recovery-summary
                                                                        snapshot-values
                                                                        component
                                                                        runtime-advanced?
                                                                        history-advanced?)]
                                                 (when (or (not= :unknown runtime-advanced?)
                                                           (not= :unknown history-advanced?)
                                                           (:last-runtime-state summary)
                                                           (:last-history-state summary))
                                                   [component summary]))))
                                       runtime-components)
        stalled-components       (->> component-summaries
                                      (keep (fn [[component summary]]
                                              (when (:stalled-after-quiet? summary)
                                                component)))
                                      vec)
        tracked-components       (keys component-summaries)
        refresh-components       [:row-refresh :agg-refresh]
        refresh-stall-suspected? (every? #(true? (get-in component-summaries
                                                         [% :stalled-after-quiet?]))
                                         refresh-components)]
    {:component-recovery-summary        component-summaries
     :stalled-components-after-quiet    stalled-components
     :all-components-stalled-after-quiet? (and (seq tracked-components)
                                               (= (count tracked-components)
                                                  (count stalled-components)))
     :refresh-stall-suspected?          refresh-stall-suspected?}))

(defrecord MVAutoschedClient [conn-holder node schema-created? schedule-meta]
  client/Client

  (open! [this test node]
    (close-conn-holder! conn-holder)
    (let [conn-holder' (atom nil)]
      (with-reconnect! conn-holder' node test setup-retryable-error?
        identity)
      (assoc this :conn-holder conn-holder' :node node)))

  (setup! [this test]
    (let [ids (map key-for-process
                   (range (long (max 1 (:concurrency test)))))
          setup (setup-schema-once!
                 test
                 #(with-reconnect! conn-holder node test setup-retryable-error?
                    (fn [conn]
                      (mv/setup-autosched-schema!
                       conn
                       ids
                       {:refresh-start-delay refresh-start-delay
                        :row-refresh-seconds row-refresh-seconds
                        :agg-refresh-seconds agg-refresh-seconds
                        :purge-start-delay   purge-start-delay
                        :purge-next-seconds  purge-next-seconds}))))]
      (reset! schema-created? true)
      (reset! schedule-meta setup))
    this)

  (invoke! [_ test op]
    (condp = (:f op)
      :insert       (apply-write-with-reconnect! conn-holder node test op)
      :update-value (apply-write-with-reconnect! conn-holder node test op)
      :move-group   (apply-write-with-reconnect! conn-holder node test op)
      :delete       (apply-write-with-reconnect! conn-holder node test op)

      :snapshot     (snapshot-with-reconnect! conn-holder node test op @schedule-meta)

      (assoc op :type :fail :error :unknown-op)))

  (teardown! [_ _])

  (close! [_ _]
    (close-conn-holder! conn-holder)))

(defn checker*
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [write-ops            (filter #(contains? #{:insert :update-value :move-group :delete} (:f %)) history)
            unresolved-writes    (filter #(and (= :info (:type %))
                                               (contains? #{:insert :update-value :move-group :delete} (:f %)))
                                         write-ops)
            snapshot-ops         (filter #(= :snapshot (:f %)) history)
            snapshot-failures    (filter op/fail? snapshot-ops)
            ok-snapshots         (ordered-snapshot-ops (filter op/ok? snapshot-ops))
            stable-pair          (stable-pair-present? ok-snapshots)
            snapshot-values      (mapv snapshot-result ok-snapshots)
            row-refresh-progress? (some :row-equal? snapshot-values)
            agg-refresh-progress? (some :agg-equal? snapshot-values)
            refresh-progress?    (and row-refresh-progress?
                                      agg-refresh-progress?)
            row-runtime-advanced? (runtime-progress? snapshot-values :row-refresh)
            agg-runtime-advanced? (runtime-progress? snapshot-values :agg-refresh)
            purge-runtime-advanced? (runtime-progress? snapshot-values :log-purge)
            row-history-advanced? (history-progress? snapshot-values :row-refresh)
            agg-history-advanced? (history-progress? snapshot-values :agg-refresh)
            purge-history-advanced? (history-progress? snapshot-values :log-purge)
            purge-progress-state (purge-progress ok-snapshots)
            purge-progress-ok?   (not= false purge-progress-state)
            unresolved-write-count (count unresolved-writes)
            snapshot-valid?      (empty? snapshot-failures)
            quiet-stable?        (boolean stable-pair)
            refresh-converged?   (boolean refresh-progress?)
            recovery-diagnosis   (recovery-diagnosis snapshot-values)
            write-resolution-valid? (zero? unresolved-write-count)
            autosched-converged? (and snapshot-valid?
                                      quiet-stable?
                                      refresh-converged?
                                      purge-progress-ok?)
            strict-valid?        (and autosched-converged?
                                      write-resolution-valid?)]
        (let [summary {:valid?                    autosched-converged?
                       :strict-valid?             strict-valid?
                       :autosched-converged?      autosched-converged?
                       :write-resolution-valid?   write-resolution-valid?
                       :recovered-write-ambiguity? (and (not write-resolution-valid?)
                                                        autosched-converged?)
                       :snapshot-valid?           snapshot-valid?
                       :write-count               (count write-ops)
                       :unresolved-write-count    unresolved-write-count
                       :snapshot-count            (count snapshot-ops)
                       :snapshot-fail-count       (count snapshot-failures)
                       :stable-after-quiet?       quiet-stable?
                       :row-refresh-converged?    (boolean row-refresh-progress?)
                       :agg-refresh-converged?    (boolean agg-refresh-progress?)
                       :post-fault-refresh?       refresh-converged?
                       :row-refresh-runtime-advanced? row-runtime-advanced?
                       :agg-refresh-runtime-advanced? agg-runtime-advanced?
                       :purge-runtime-advanced?   purge-runtime-advanced?
                       :row-refresh-history-advanced? row-history-advanced?
                       :agg-refresh-history-advanced? agg-history-advanced?
                       :purge-history-advanced?   purge-history-advanced?
                       :component-recovery-summary (:component-recovery-summary recovery-diagnosis)
                       :stalled-components-after-quiet (:stalled-components-after-quiet recovery-diagnosis)
                       :all-components-stalled-after-quiet? (:all-components-stalled-after-quiet? recovery-diagnosis)
                       :refresh-stall-suspected? (:refresh-stall-suspected? recovery-diagnosis)
                       :post-fault-purge          purge-progress-state
                       :snapshot-path             "mv-autosched/snapshots.edn"
                       :snapshot-json-path        "mv-autosched/snapshots.json"
                       :summary-path              "mv-autosched/summary.edn"
                       :summary-json-path         "mv-autosched/summary.json"
                       :first-snapshot-failure    (first snapshot-failures)
                       :last-snapshot             (some-> ok-snapshots last snapshot-result)
                       :first-unresolved-write    (first unresolved-writes)}]
          (when (and (:name test) (:start-time test))
            (artifact/write-edn+json! test ["mv-autosched" "snapshots.edn"] snapshot-values)
            (artifact/write-edn+json! test ["mv-autosched" "summary.edn"] summary))
          summary)))))

(defn workload
  [_]
  {:client          (MVAutoschedClient. (atom nil) nil (atom false) (atom nil))
   :generator       (gen/stagger 1/5 (generator))
   :checker         (checker/compose {:mv-autosched (checker*)
                                      :timeline     (timeline/html)})
   :final-generator (quiet-generator)})
