(ns tidb.mv-stateful
  (:refer-clojure :exclude [test])
  (:require [clojure.java.jdbc :as j]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
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

(defn setup-retryable-error?
  [t]
  (let [message (str (.getMessage t))]
    (or (ambiguous-write-error? t)
        (instance? java.sql.BatchUpdateException t)
        (re-find #"Resolve lock timeout|Information schema is changed|Region is unavailable" message))))

(defn close-conn-holder!
  [conn-holder]
  (when-let [conn @conn-holder]
    (c/close! conn))
  (reset! conn-holder nil))

(defn ensure-conn!
  [conn-holder node test]
  (or @conn-holder
      (reset! conn-holder (c/open node test))))

(defn reopen-conn!
  [conn-holder node test]
  (close-conn-holder! conn-holder)
  (ensure-conn! conn-holder node test))

(defn with-reconnect!
  [conn-holder node test retryable-error? f]
  (loop [tries 3]
    (let [result (try
                   (let [conn (ensure-conn! conn-holder node test)]
                     {:ok (f conn)})
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
          #(mv/setup-stateful-schema! % ids))))
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
      (let [refresh-row-ops (filter #(and (= :refresh-row (:f %))
                                          (completed-op? %))
                                    history)
            refresh-agg-ops (filter #(and (= :refresh-agg (:f %))
                                          (completed-op? %))
                                    history)
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
            ever-agg-failed? (boolean first-agg-failure)]
        (let [summary {:valid?                    (and final-row-valid?
                                                      final-agg-valid?
                                                      (empty? unresolved))
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
