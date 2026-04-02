(ns tidb.mv-stateful
  (:refer-clojure :exclude [test])
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info]]
            [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]
             [store :as store]]
            [jepsen.checker.timeline :as timeline]
            [knossos.op :as op]
            [tidb.artifact :as artifact]
            [tidb.mview :as mv]
            [tidb.sql :as c]
            [tidb.util :as util]))

(def group-count 4)
(def write-fns [:insert :update-value :update-value :move-group :delete])
(def write-op-fns #{:insert :update-value :move-group :delete})
(def refresh-op-fns #{:refresh-row :refresh-agg})
(def ^:dynamic refresh-window-max-candidates 1024)
(def ^:dynamic refresh-window-max-overlapping-writes 8)

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

(defn stateful-write
  [process seq]
  (let [id      (key-for-process process)
        f       (rand-nth write-fns)
        token   (str "mv-stateful-" process "-" seq "-" (rand-int 1000000))
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

(defn completed-op?
  [op]
  (or (op/ok? op)
      (op/fail? op)))

(defn op-result
  [op]
  (or (:result op)
      (:value op)))

(defn op-phase
  [op]
  (or (:phase op) :active))

(declare refresh-row-on-conn! refresh-agg-on-conn!)

(defn generator
  []
  (let [seqs (atom {})]
    (reify gen/Generator
      (op [_ test process]
        (let [process (process-id process)
              seq     (get (swap! seqs update process (fnil inc 0)) process)
              p       (rand)]
          (if (zero? process)
            (cond
              (< p 0.56) (stateful-write process seq)
              (< p 0.78) {:type :invoke
                          :phase :active
                          :f :refresh-row
                          :value (inc (rand-int (long (max 1 (:concurrency test)))))}
              (< p 0.93) {:type :invoke, :phase :active, :f :refresh-agg}
              :else      {:type :invoke, :phase :active, :f :purge})
            (stateful-write process seq)))))))

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
        (= :connect-timed-out type)
        driver-batch-npe?
        (re-find #"timed(?: |-)?out|Connection is closed|closed connection|Connection reset|broken pipe|Socket" message))))

(defn retryable-write-error?
  [t]
  (or (ambiguous-write-error? t)
      (instance? NullPointerException t)))

(def reconnect-retry-count 4)
(def reconnect-retry-base-ms 250)
(def reconnect-retry-max-ms 1000)

(def op-timeout-ms
  (+ c/socket-timeout 5000))

(def setup-timeout-ms
  (max 120000
       (* 4 op-timeout-ms)))

(defn- reconnect-retry-delay-ms
  [attempt]
  (long
   (min reconnect-retry-max-ms
        (* reconnect-retry-base-ms
           (bit-shift-left 1 (max 0 (dec attempt)))))))

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
  ([conn-holder node f]
   (run-with-op-timeout! conn-holder node f op-timeout-ms))
  ([conn-holder node f timeout-ms]
   (let [worker (future
                  (try
                    {:ok (f)}
                    (catch Throwable t
                      {:error t})))
         result (deref worker timeout-ms ::timeout)]
     (if (= ::timeout result)
       (do
         (close-conn-holder! conn-holder)
         (future-cancel worker)
         (throw (ex-info (str "operation timed out after " timeout-ms " ms")
                         {:type :op-timed-out
                          :node node
                          :timeout-ms timeout-ms})))
       (if-let [t (:error result)]
         (throw t)
         (:ok result))))))

(defn ensure-conn!
  [conn-holder node test]
  (or @conn-holder
      (reset! conn-holder (c/open node test))))

(defn reopen-conn!
  [conn-holder node test]
  (close-conn-holder! conn-holder)
  (ensure-conn! conn-holder node test))

(defn with-reconnect!
  ([conn-holder node test retryable-error? f]
   (with-reconnect! conn-holder node test retryable-error? f op-timeout-ms))
  ([conn-holder node test retryable-error? f timeout-ms]
   (loop [attempt 1]
     (let [result (try
                    (let [conn (ensure-conn! conn-holder node test)]
                      {:ok (run-with-op-timeout! conn-holder node #(f conn) timeout-ms)})
                    (catch Throwable t
                      {:error t}))]
       (if-let [t (:error result)]
         (if (and (retryable-error? t)
                  (< attempt reconnect-retry-count))
           (let [delay-ms (reconnect-retry-delay-ms attempt)]
             (info {:reconnect/node       node
                    :reconnect/attempt    attempt
                    :reconnect/sleep-ms   delay-ms
                    :reconnect/error      (or (.getMessage t) (str t))})
             (close-conn-holder! conn-holder)
             (Thread/sleep delay-ms)
             (recur (inc attempt)))
           (throw t))
         (:ok result))))))

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
  [{:keys [deleted]}]
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

(declare compare-row! compare-all-rows! compare-agg!)

(defn exception-summary
  [^Throwable t]
  (or (.getMessage t)
      (str t)))

(defn apply-write-on-conn!
  [node test conn op]
  (let [{:keys [id g1 v1 version last-token deleted pad]} (:value op)
        params [(upsert-sql (:value op))
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

(defn refresh-row-with-reconnect!
  [conn-holder node test op]
  (try
    (with-reconnect! conn-holder node test ambiguous-write-error?
      (fn [conn]
        (j/with-db-transaction [tx conn {:isolation (util/isolation-level test)}]
          (refresh-row-on-conn! tx test op))))
    (catch Throwable t
      (assoc op :type :fail :error :refresh-row-error :exception (.getMessage t)))))

(defn refresh-agg-with-reconnect!
  [conn-holder node test op]
  (try
    (with-reconnect! conn-holder node test ambiguous-write-error?
      (fn [conn]
        (j/with-db-transaction [tx conn {:isolation (util/isolation-level test)}]
          (refresh-agg-on-conn! tx test op))))
    (catch Throwable t
      (assoc op :type :fail :error :refresh-agg-error :exception (.getMessage t)))))

(defn purge-with-reconnect!
  [conn-holder node test op]
  (try
    (with-reconnect! conn-holder node test ambiguous-write-error?
      (fn [conn]
        (assoc op :type :ok :result {:statement (mv/purge-log! conn)})))
    (catch Throwable t
      (assoc op :type :fail :error :purge-error :exception (.getMessage t)))))

(defn- compare-row-on-conn!
  [conn id]
  (let [expected (mv/query-base-row conn id)
        actual   (mv/query-mv-row conn id)
        diff     (mv/row-diff expected actual)]
    (if diff
      {:type :fail
       :value {:id id :diff diff}}
      {:type :ok
       :value {:id id :row actual}})))

(defn- compare-all-rows-on-conn!
  [conn]
  (let [expected (mv/query-base-row-projection conn)
        actual   (mv/query-mv-row-projection conn)
        diff     (mv/full-row-diff expected actual)]
    (if (seq diff)
      {:type :fail
       :value {:diff diff
               :expected expected
               :actual actual}}
      {:type :ok
       :value {:rows (count actual)}})))

(defn- compare-agg-on-conn!
  [conn]
  (let [expected (mv/query-base-agg conn)
        actual   (mv/query-mv-agg conn)
        diff     (mv/agg-diff expected actual)]
    (if (seq diff)
      {:type :fail
       :value {:diff diff
               :expected expected
               :actual actual}}
      {:type :ok
       :value {:groups (count actual)}})))

(defn compare-row!
  [conn test id]
  (j/with-db-transaction [tx conn {:isolation (util/isolation-level test)}]
    (compare-row-on-conn! tx id)))

(defn compare-all-rows!
  [conn test]
  (j/with-db-transaction [tx conn {:isolation (util/isolation-level test)}]
    (compare-all-rows-on-conn! tx)))

(defn compare-agg!
  [conn test]
  (j/with-db-transaction [tx conn {:isolation (util/isolation-level test)}]
    (compare-agg-on-conn! tx)))

(defn- refresh-row-on-conn!
  [conn test op]
  (mv/refresh-view! conn mv/row-view)
  (let [comparison (if (= :all (:value op))
                     (compare-all-rows-on-conn! conn)
                     (compare-row-on-conn! conn (or (:value op)
                                                    (key-for-process (:process op)))))
        {:keys [type value]} comparison]
    (assoc op :type type :result value)))

(defn- refresh-agg-on-conn!
  [conn _test op]
  (mv/refresh-view! conn mv/agg-view)
  (let [{:keys [type value]} (compare-agg-on-conn! conn)]
    (assoc op :type type :result value)))

(defn- pair-history
  [history]
  (second
   (reduce (fn [[pending pairs] op]
             (cond
               (= :invoke (:type op))
               [(assoc pending (:process op) op) pairs]

               (completed-op? op)
               (if-let [invoke (get pending (:process op))]
                 [(dissoc pending (:process op))
                  (conj pairs {:invoke invoke
                               :complete op})]
                 [pending pairs])

               :else
               [pending pairs]))
           [{} []]
           history)))

(defn- write-pair?
  [pair]
  (contains? write-op-fns (get-in pair [:complete :f])))

(defn- refresh-pair?
  [pair]
  (contains? refresh-op-fns (get-in pair [:complete :f])))

(defn- pair-index
  [pair key]
  (:index (key pair)))

(defn- write-value
  [pair]
  (get-in pair [:complete :value]))

(defn- write-process
  [pair]
  (get-in pair [:complete :process]))

(defn- live-row
  [{:keys [id g1 v1 version last-token deleted]}]
  (when (and (some? id) (not deleted))
    {:id         (long id)
     :g1         (long g1)
     :v1         (long v1)
     :version    (long version)
     :last-token last-token}))

(defn- row-model
  [state]
  (->> state
       (keep (fn [[id value]]
               (when-let [row (live-row value)]
                 [(long id) row])))
       (into (sorted-map))))

(defn- agg-model
  [state]
  (->> state
       vals
       (keep live-row)
       (reduce (fn [groups {:keys [g1 v1]}]
                 (update groups g1
                         (fn [row]
                           (if row
                             {:g1     g1
                              :cnt    (inc (:cnt row))
                              :sum-v1 (+ (:sum-v1 row) v1)
                              :min-v1 (min (:min-v1 row) v1)
                              :max-v1 (max (:max-v1 row) v1)}
                             {:g1     g1
                              :cnt    1
                              :sum-v1 v1
                              :min-v1 v1
                              :max-v1 v1}))))
               (sorted-map))))

(defn- refresh-window-state-choices
  [write-pairs refresh-pair]
  (let [invoke-index   (pair-index refresh-pair :invoke)
        complete-index (pair-index refresh-pair :complete)
        before-by-process
        (->> write-pairs
             (filter #(< (pair-index % :complete) invoke-index))
             (sort-by #(pair-index % :complete))
             (reduce (fn [state pair]
                       (assoc state
                              (write-process pair)
                              (write-value pair)))
                     {}))
        overlapping
        (->> write-pairs
             (remove #(< (pair-index % :complete) invoke-index))
             (remove #(> (pair-index % :invoke) complete-index))
             (sort-by #(pair-index % :invoke))
             (group-by write-process)
             (into {}
                   (map (fn [[process pairs]]
                          [process (mapv write-value pairs)]))))
        processes      (sort (distinct (concat (keys before-by-process)
                                               (keys overlapping))))]
    {:base-state              (->> before-by-process
                                   vals
                                   (reduce (fn [state value]
                                             (assoc state (:id value) value))
                                           {}))
     :overlapping            overlapping
     :processes              processes
     :overlapping-write-count (reduce + 0 (map count (vals overlapping)))
     :candidate-count        (reduce *' 1
                                     (map (fn [process]
                                            (inc (count (get overlapping process []))))
                                          processes))}))

(defn- refresh-window-states
  [{:keys [base-state overlapping processes]}]
  (letfn [(step [remaining current]
            (lazy-seq
             (if-let [process (first remaining)]
               (mapcat (fn [choice]
                         (step (rest remaining)
                               (if choice
                                 (assoc current (:id choice) choice)
                                 current)))
                       (cons nil (get overlapping process [])))
               [current])))]
    (step processes base-state)))

(defn- diff-score
  [diff]
  (+ (count (:missing-in-mv diff))
     (count (:unexpected-in-mv diff))
     (count (:mismatched diff))
     (count (:bad-live-cnt diff))
     (if (contains? diff :expected) 1 0)
     (if (contains? diff :live-cnt) 1 0)))

(defn- refresh-actual
  [complete]
  (let [result (:result complete)]
    (case (:f complete)
      :refresh-agg
      (if (contains? result :actual)
        (:actual result)
        ::absent)

      :refresh-row
      (if (= :all (:value complete))
        (if (contains? result :actual)
          (:actual result)
          ::absent)
        (cond
          (contains? result :row)
          (:row result)

          (contains? (:diff result) :actual)
          (let [diff   (:diff result)
                actual (:actual diff)]
            (cond
              (nil? actual) nil
              (map? actual)
              (cond-> actual
                (contains? diff :live-cnt) (assoc :live-cnt (:live-cnt diff))
                (not (contains? diff :live-cnt)) (assoc :live-cnt 1))
              :else
              actual))

          :else
          ::absent))

      ::absent)))

(defn- refresh-expected
  [complete state]
  (case (:f complete)
    :refresh-agg
    (agg-model state)

    :refresh-row
    (if (= :all (:value complete))
      (row-model state)
      (get (row-model state)
           (long (or (:value complete)
                     (key-for-process (:process complete))))))

    nil))

(defn- refresh-diff
  [complete expected actual]
  (case (:f complete)
    :refresh-agg
    (mv/agg-diff expected actual)

    :refresh-row
    (if (= :all (:value complete))
      (mv/full-row-diff expected actual)
      (mv/row-diff expected actual))

    nil))

(defn- refresh-result-base
  [complete actual]
  (case (:f complete)
    :refresh-agg
    {:groups (count actual)}

    :refresh-row
    (if (= :all (:value complete))
      {:rows (count actual)}
      {:id  (or (:value complete)
                (get-in complete [:result :id])
                (key-for-process (:process complete)))
       :row actual})

    {}))

(defn- refresh-window-check
  [refresh-pair write-pairs]
  (let [complete (:complete refresh-pair)
        actual   (refresh-actual complete)]
      (if (= ::absent actual)
      complete
      (let [{:keys [candidate-count
                    overlapping-write-count]
             :as state-choices} (refresh-window-state-choices write-pairs refresh-pair)
            window          {:invoke-index   (pair-index refresh-pair :invoke)
                             :complete-index (pair-index refresh-pair :complete)}
            skip-reason     (cond
                              (> overlapping-write-count
                                 refresh-window-max-overlapping-writes)
                              :overlapping-write-limit

                              (> candidate-count refresh-window-max-candidates)
                              :candidate-limit

                              :else
                              nil)]
        (if skip-reason
          (update complete :result merge
                  {:window-validation-skipped? true
                   :window-validation-skip-reason skip-reason
                   :candidate-count candidate-count
                   :overlapping-write-count overlapping-write-count
                   :window window})
          (let [evaluation
                (reduce (fn [best state]
                          (let [expected  (refresh-expected complete state)
                                diff      (refresh-diff complete expected actual)
                                candidate {:expected expected
                                           :diff diff}]
                            (if (empty? diff)
                              (reduced {:matched candidate})
                              (if-let [best-candidate (:best best)]
                                (if (< (diff-score diff)
                                       (diff-score (:diff best-candidate)))
                                  {:best candidate}
                                  best)
                                {:best candidate}))))
                        {:best nil}
                        (refresh-window-states state-choices))]
            (if-let [matched (:matched evaluation)]
              (assoc complete
                     :type :ok
                     :resolved? (when (op/fail? complete) true)
                     :result (merge (refresh-result-base complete actual)
                                    {:actual actual
                                     :expected (:expected matched)
                                     :validation :window-compatible
                                     :candidate-count candidate-count
                                     :overlapping-write-count overlapping-write-count
                                     :window window}))
              (let [{:keys [expected diff]} (:best evaluation)]
                (assoc complete
                       :type :fail
                       :error :refresh-window-mismatch
                       :result (merge (refresh-result-base complete actual)
                                      {:actual actual
                                       :expected expected
                                       :diff diff
                                       :candidate-count candidate-count
                                       :overlapping-write-count overlapping-write-count
                                       :window window}))))))))))

(defn- validate-refresh-op
  [write-pairs op-pair]
  (let [complete (:complete op-pair)]
    (if (and (contains? refresh-op-fns (:f complete))
             (op/fail? complete))
      (refresh-window-check op-pair write-pairs)
      complete)))

(defrecord MVStatefulClient [conn-holder node schema-created?]
  client/Client

  (open! [this test node]
    (close-conn-holder! conn-holder)
    (let [conn-holder' (atom nil)]
      (with-reconnect! conn-holder' node test setup-retryable-error?
        identity)
      (assoc this :conn-holder conn-holder' :node node)))

  (setup! [this test]
    (when (compare-and-set! schema-created? false true)
      (let [ids (map key-for-process
                     (range (long (max 1 (:concurrency test)))))]
        (with-reconnect! conn-holder node test setup-retryable-error?
          #(mv/setup-stateful-schema! % ids)
          setup-timeout-ms)))
    this)

  (invoke! [this test op]
    (condp = (:f op)
      :insert       (apply-write-with-reconnect! conn-holder node test op)
      :update-value (apply-write-with-reconnect! conn-holder node test op)
      :move-group   (apply-write-with-reconnect! conn-holder node test op)
      :delete       (apply-write-with-reconnect! conn-holder node test op)

      :refresh-row  (refresh-row-with-reconnect! conn-holder node test op)
      :refresh-agg  (refresh-agg-with-reconnect! conn-holder node test op)
      :purge        (purge-with-reconnect! conn-holder node test op)

      (assoc op :type :fail :error :unknown-op)))

  (teardown! [_ _])

  (close! [_ _]
    (close-conn-holder! conn-holder)))

(defn checker*
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [pairs           (pair-history history)
            write-pairs     (filter write-pair? pairs)
            validated-by-index
            (->> pairs
                 (filter refresh-pair?)
                 (map (fn [pair]
                        [(:index (:complete pair))
                         (validate-refresh-op write-pairs pair)]))
                 (into {}))
            validated-op    (fn [op]
                              (get validated-by-index (:index op) op))
            refresh-row-ops (->> history
                                 (filter #(and (= :refresh-row (:f %))
                                               (completed-op? %)))
                                 (map validated-op))
            refresh-agg-ops (->> history
                                 (filter #(and (= :refresh-agg (:f %))
                                               (completed-op? %)))
                                 (map validated-op))
            purge-ops       (filter #(and (= :purge (:f %))
                                          (completed-op? %))
                                    history)
            refresh-purge   (concat refresh-row-ops refresh-agg-ops purge-ops)
            recent-rp       (vec (take-last 20 refresh-purge))
            write-ops       (filter #(contains? #{:insert :update-value :move-group :delete} (:f %)) history)
            row-failures    (filter op/fail? refresh-row-ops)
            agg-failures    (filter op/fail? refresh-agg-ops)
            failures        (filter op/fail? refresh-purge)
            unresolved      (filter #(and (= :info (:type %))
                                          (contains? #{:insert :update-value :move-group :delete} (:f %)))
                                    write-ops)
            active-row-ops      (filter #(= :active (op-phase %)) refresh-row-ops)
            active-agg-ops      (filter #(= :active (op-phase %)) refresh-agg-ops)
            final-row-ops       (filter #(= :final (op-phase %)) refresh-row-ops)
            final-agg-ops       (filter #(= :final (op-phase %)) refresh-agg-ops)
            active-row-failures (filter op/fail? active-row-ops)
            active-agg-failures (filter op/fail? active-agg-ops)
            final-row-failures  (filter op/fail? final-row-ops)
            final-agg-failures  (filter op/fail? final-agg-ops)
            window-compatible-row-ops (filter #(= :window-compatible
                                                  (get-in % [:result :validation]))
                                              refresh-row-ops)
            window-compatible-agg-ops (filter #(= :window-compatible
                                                  (get-in % [:result :validation]))
                                              refresh-agg-ops)
            first-failure   (first failures)
            first-row-failure (first row-failures)
            first-agg-failure (first agg-failures)
            first-active-row-failure (first active-row-failures)
            first-active-agg-failure (first active-agg-failures)
            first-final-row-failure  (first final-row-failures)
            first-final-agg-failure  (first final-agg-failures)
            last-row-failure (last row-failures)
            last-agg-failure (last agg-failures)
            final-row-op    (last refresh-row-ops)
            final-agg-op    (last refresh-agg-ops)
            final-row-check (some-> final-row-op op-result)
            final-agg-check (some-> final-agg-op op-result)
            final-row-valid? (boolean (and final-row-op (op/ok? final-row-op)))
            final-agg-valid? (boolean (and final-agg-op (op/ok? final-agg-op)))
            ever-row-failed? (boolean first-row-failure)
            ever-agg-failed? (boolean first-agg-failure)
            write-resolution-valid? (empty? unresolved)
            stateful-valid? (and final-row-valid?
                                 final-agg-valid?)]
        (let [summary {:valid?                    stateful-valid?
                       :strict-valid?             (and stateful-valid?
                                                      write-resolution-valid?)
                       :write-resolution-valid?   write-resolution-valid?
                       :recovered-write-ambiguity? (and stateful-valid?
                                                        (not write-resolution-valid?))
                       :final-row-valid?          final-row-valid?
                       :final-agg-valid?          final-agg-valid?
                       :ever-row-failed?          ever-row-failed?
                       :ever-agg-failed?          ever-agg-failed?
                       :recovered-row-failure?    (and ever-row-failed?
                                                       final-row-valid?)
                       :recovered-agg-failure?    (and ever-agg-failed?
                                                       final-agg-valid?)
                       :ever-active-row-failed?   (boolean first-active-row-failure)
                       :ever-active-agg-failed?   (boolean first-active-agg-failure)
                       :ever-final-row-failed?    (boolean first-final-row-failure)
                       :ever-final-agg-failed?    (boolean first-final-agg-failure)
                       :refresh-row-ok-count      (count (filter op/ok? refresh-row-ops))
                       :refresh-row-fail-count    (count row-failures)
                       :refresh-agg-ok-count      (count (filter op/ok? refresh-agg-ops))
                       :refresh-agg-fail-count    (count agg-failures)
                       :active-refresh-row-ok-count   (count (filter op/ok? active-row-ops))
                       :active-refresh-row-fail-count (count active-row-failures)
                       :active-refresh-agg-ok-count   (count (filter op/ok? active-agg-ops))
                       :active-refresh-agg-fail-count (count active-agg-failures)
                       :final-refresh-row-ok-count    (count (filter op/ok? final-row-ops))
                       :final-refresh-row-fail-count  (count final-row-failures)
                       :final-refresh-agg-ok-count    (count (filter op/ok? final-agg-ops))
                       :final-refresh-agg-fail-count  (count final-agg-failures)
                       :window-compatible-row-count   (count window-compatible-row-ops)
                       :window-compatible-agg-count   (count window-compatible-agg-ops)
                       :purge-ok-count            (count (filter op/ok? purge-ops))
                       :purge-fail-count          (count (filter op/fail? purge-ops))
                       :unresolved-write-count    (count unresolved)
                       :first-failure             first-failure
                       :first-row-failure         first-row-failure
                       :first-agg-failure         first-agg-failure
                       :first-active-row-failure  first-active-row-failure
                       :first-active-agg-failure  first-active-agg-failure
                       :first-final-row-failure   first-final-row-failure
                       :first-final-agg-failure   first-final-agg-failure
                       :last-row-failure          last-row-failure
                       :last-agg-failure          last-agg-failure
                       :first-unresolved-write    (first unresolved)
                       :first-row-failure-path    "mv-stateful/first-row-failure.edn"
                       :first-row-failure-json-path "mv-stateful/first-row-failure.json"
                       :first-agg-failure-path    "mv-stateful/first-agg-failure.edn"
                       :first-agg-failure-json-path "mv-stateful/first-agg-failure.json"
                       :final-row-check-path      "mv-stateful/final-row-check.edn"
                       :final-row-check-json-path "mv-stateful/final-row-check.json"
                       :final-agg-check-path      "mv-stateful/final-agg-check.edn"
                       :final-agg-check-json-path "mv-stateful/final-agg-check.json"
                       :recent-refresh-purge-path "mv-stateful/recent-refresh-purge.edn"
                       :recent-refresh-purge-json-path "mv-stateful/recent-refresh-purge.json"
                       :first-failure-path        "mv-stateful/first-failure.edn"
                       :first-failure-json-path   "mv-stateful/first-failure.json"
                       :summary-path              "mv-stateful/summary.edn"
                       :summary-json-path         "mv-stateful/summary.json"}]
          (when (and (:name test) (:start-time test))
            (artifact/write-edn+json! test ["mv-stateful" "final-row-check.edn"] final-row-check)
            (artifact/write-edn+json! test ["mv-stateful" "final-agg-check.edn"] final-agg-check)
            (artifact/write-edn+json! test ["mv-stateful" "recent-refresh-purge.edn"] recent-rp)
            (artifact/write-edn+json! test ["mv-stateful" "first-failure.edn"] first-failure)
            (artifact/write-edn+json! test ["mv-stateful" "first-row-failure.edn"] first-row-failure)
            (artifact/write-edn+json! test ["mv-stateful" "first-agg-failure.edn"] first-agg-failure)
            (artifact/write-edn+json! test ["mv-stateful" "summary.edn"] summary))
          summary)))))

(defn workload
  [opts]
  {:client          (MVStatefulClient. (atom nil) nil (atom false))
   :generator       (gen/stagger 1/5 (generator))
   :checker         (checker/compose {:mv-stateful (checker*)
                                      :timeline    (timeline/html)})
   :final-generator (gen/on #{0}
                            (gen/seq [{:type :invoke, :phase :final, :f :refresh-row, :value :all}
                                      {:type :invoke, :phase :final, :f :refresh-agg}
                                      {:type :invoke, :phase :final, :f :purge}
                                      {:type :invoke, :phase :final, :f :refresh-row, :value :all}
                                      {:type :invoke, :phase :final, :f :refresh-agg}]))})
