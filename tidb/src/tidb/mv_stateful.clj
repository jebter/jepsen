(ns tidb.mv-stateful
  (:refer-clojure :exclude [test])
  (:require [clojure.java.jdbc :as j]
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
                          :f :refresh-row
                          :value (inc (rand-int (long (max 1 (:concurrency test)))))}
              (< p 0.93) {:type :invoke, :f :refresh-agg}
              :else      {:type :invoke, :f :purge})
            (stateful-write process seq)))))))

(defn ambiguous-write-error?
  [t]
  (let [message (str (.getMessage t))]
    (or (instance? java.sql.SQLTimeoutException t)
        (instance? java.sql.SQLNonTransientConnectionException t)
        (re-find #"timed out|Connection is closed|closed connection|Connection reset|broken pipe|Socket" message))))

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

(defn verify-write!
  [node test op]
  (try
    (let [conn (c/open node test)]
      (try
        (let [row (mv/query-stored-row conn (get-in op [:value :id]))]
          (when (stored-row-matches? row (:value op))
            (assoc op :type :ok :resolved? true :result {:verify :read-back})))
        (finally
          (c/close! conn))))
    (catch Throwable _
      nil)))

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

(defn apply-write!
  [node test conn op]
  (let [{:keys [id g1 v1 version last-token deleted pad]} (:value op)
        params [(upsert-sql (:value op))
                id g1 v1 version last-token (if deleted 1 0) pad]]
    (try
      (c/execute! conn params {:transaction? false})
      (assoc op :type :ok)
      (catch Throwable t
        (if (ambiguous-write-error? t)
          (or (verify-write! node test op)
              (assoc op :type :info :error :indeterminate-write :exception (.getMessage t)))
          (throw t))))))

(defn compare-row!
  [conn test id]
  (j/with-db-transaction [tx conn {:isolation (util/isolation-level test)}]
    (let [expected (mv/query-base-row tx id)
          actual   (mv/query-mv-row tx id)
          diff     (mv/row-diff expected actual)]
      (if diff
        {:type :fail
         :value {:id id :diff diff}}
        {:type :ok
         :value {:id id :row actual}}))))

(defn compare-all-rows!
  [conn test]
  (j/with-db-transaction [tx conn {:isolation (util/isolation-level test)}]
    (let [expected (mv/query-base-row-projection tx)
          actual   (mv/query-mv-row-projection tx)
          diff     (mv/full-row-diff expected actual)]
      (if (seq diff)
        {:type :fail
         :value {:diff diff
                 :expected expected
                 :actual actual}}
        {:type :ok
         :value {:rows (count actual)}}))))

(defn compare-agg!
  [conn test]
  (j/with-db-transaction [tx conn {:isolation (util/isolation-level test)}]
    (let [expected (mv/query-base-agg tx)
          actual   (mv/query-mv-agg tx)
          diff     (mv/agg-diff expected actual)]
      (if (seq diff)
        {:type :fail
         :value {:diff diff
                 :expected expected
                 :actual actual}}
        {:type :ok
         :value {:groups (count actual)}}))))

(defrecord MVStatefulClient [conn node schema-created?]
  client/Client

  (open! [this test node]
    (assoc this :node node :conn (c/open node test)))

  (setup! [this test]
    (when (compare-and-set! schema-created? false true)
      (let [ids (map key-for-process
                     (range (long (max 1 (:concurrency test)))))]
        (c/with-conn-failure-retry conn
          (mv/setup-stateful-schema! conn ids))))
    this)

  (invoke! [this test op]
    (condp = (:f op)
      :insert       (apply-write! node test conn op)
      :update-value (apply-write! node test conn op)
      :move-group   (apply-write! node test conn op)
      :delete       (apply-write! node test conn op)

      :refresh-row
      (try
        (mv/refresh-view! conn mv/row-view)
        (let [comparison (if (= :all (:value op))
                           (compare-all-rows! conn test)
                           (compare-row! conn test (or (:value op)
                                                       (key-for-process (:process op)))))
              {:keys [type value]} comparison]
          (assoc op :type type :result value))
        (catch Throwable t
          (assoc op :type :fail :error :refresh-row-error :exception (.getMessage t))))

      :refresh-agg
      (try
        (mv/refresh-view! conn mv/agg-view)
        (let [{:keys [type value]} (compare-agg! conn test)]
          (assoc op :type type :result value))
        (catch Throwable t
          (assoc op :type :fail :error :refresh-agg-error :exception (.getMessage t))))

      :purge
      (try
        (assoc op :type :ok :result {:statement (mv/purge-log! conn)})
        (catch Throwable t
          (assoc op :type :fail :error :purge-error :exception (.getMessage t))))

      (assoc op :type :fail :error :unknown-op)))

  (teardown! [_ _])

  (close! [_ _]
    (c/close! conn)))

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
            failures        (filter op/fail? refresh-purge)
            unresolved      (filter #(and (= :info (:type %))
                                          (contains? #{:insert :update-value :move-group :delete} (:f %)))
                                    write-ops)
            first-failure   (first failures)
            last-row-failure (last (filter op/fail? refresh-row-ops))
            last-agg-failure (last (filter op/fail? refresh-agg-ops))
            final-row-op    (last refresh-row-ops)
            final-agg-op    (last refresh-agg-ops)
            final-row-check (some-> final-row-op op-result)
            final-agg-check (some-> final-agg-op op-result)
            final-row-valid? (boolean (and final-row-op (op/ok? final-row-op)))
            final-agg-valid? (boolean (and final-agg-op (op/ok? final-agg-op)))]
        (let [summary {:valid?                    (and final-row-valid?
                                                      final-agg-valid?
                                                      (empty? unresolved))
                       :final-row-valid?          final-row-valid?
                       :final-agg-valid?          final-agg-valid?
                       :refresh-row-ok-count      (count (filter op/ok? refresh-row-ops))
                       :refresh-row-fail-count    (count (filter op/fail? refresh-row-ops))
                       :refresh-agg-ok-count      (count (filter op/ok? refresh-agg-ops))
                       :refresh-agg-fail-count    (count (filter op/fail? refresh-agg-ops))
                       :purge-ok-count            (count (filter op/ok? purge-ops))
                       :purge-fail-count          (count (filter op/fail? purge-ops))
                       :unresolved-write-count    (count unresolved)
                       :first-failure             first-failure
                       :last-row-failure          last-row-failure
                       :last-agg-failure          last-agg-failure
                       :first-unresolved-write    (first unresolved)
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
            (artifact/write-edn+json! test ["mv-stateful" "summary.edn"] summary))
          summary)))))

(defn workload
  [opts]
  {:client          (MVStatefulClient. nil nil (atom false))
   :generator       (gen/stagger 1/5 (generator))
   :checker         (checker/compose {:mv-stateful (checker*)
                                      :timeline    (timeline/html)})
   :final-generator (gen/on #{0}
                            (gen/seq [{:type :invoke, :f :refresh-row, :value :all}
                                      {:type :invoke, :f :refresh-agg}
                                      {:type :invoke, :f :purge}
                                      {:type :invoke, :f :refresh-row, :value :all}
                                      {:type :invoke, :f :refresh-agg}]))})
