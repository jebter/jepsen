(ns tidb.mv-autosched
  (:refer-clojure :exclude [test])
  (:require [clojure.pprint :refer [pprint]]
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
  (gen/seq
   (vec
    (mapcat (fn [i]
              (cond-> []
                (pos? i) (conj (gen/sleep quiet-snapshot-interval-seconds))
                true     (conj {:type :invoke :f :snapshot})))
            (range quiet-snapshot-count)))))

(defn snapshot-result
  [op]
  (or (:result op)
      (:value op)))

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

(defn apply-write!
  [node test conn op]
  (let [{:keys [id g1 v1 version last-token deleted pad]} (:value op)
        params [(upsert-sql)
                id g1 v1 version last-token (if deleted 1 0) pad]]
    (try
      (c/execute! conn params {:transaction? false})
      (assoc op :type :ok)
      (catch Throwable t
        (if (ambiguous-write-error? t)
          (or (verify-write! node test op)
              (assoc op :type :info :error :indeterminate-write :exception (.getMessage t)))
          (throw t))))))

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
        schedule-ddl   (mv/read-schedule-metadata conn schedule-meta)]
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
     :row-refresh-stmt     (:row-refresh-stmt schedule-meta)
     :agg-refresh-stmt     (:agg-refresh-stmt schedule-meta)
     :purge-schedule-stmt  (:purge-schedule-stmt schedule-meta)
     :mlog-stmt            (:mlog-stmt schedule-meta)}))

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

(defrecord MVAutoschedClient [conn node schema-created? schedule-meta]
  client/Client

  (open! [this test node]
    (assoc this :node node :conn (c/open node test)))

  (setup! [this test]
    (when (compare-and-set! schema-created? false true)
      (let [ids (map key-for-process
                     (range (long (max 1 (:concurrency test)))))
            setup (c/with-conn-failure-retry conn
                    (mv/setup-autosched-schema!
                     conn
                     ids
                     {:refresh-start-delay refresh-start-delay
                      :row-refresh-seconds row-refresh-seconds
                      :agg-refresh-seconds agg-refresh-seconds
                      :purge-start-delay   purge-start-delay
                      :purge-next-seconds  purge-next-seconds}))]
        (reset! schedule-meta setup)))
    this)

  (invoke! [_ test op]
    (condp = (:f op)
      :insert       (apply-write! node test conn op)
      :update-value (apply-write! node test conn op)
      :move-group   (apply-write! node test conn op)
      :delete       (apply-write! node test conn op)

      :snapshot
      (try
        (assoc op :type :ok :result (snapshot-state node conn @schedule-meta))
        (catch Throwable t
          (assoc op :type :fail :error :snapshot-error :exception (.getMessage t))))

      (assoc op :type :fail :error :unknown-op)))

  (teardown! [_ _])

  (close! [_ _]
    (c/close! conn)))

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
            refresh-progress?    (some #(let [value (snapshot-result %)]
                                          (and (:row-equal? value) (:agg-equal? value)))
                                       ok-snapshots)
            purge-progress-state (purge-progress ok-snapshots)
            purge-progress-ok?   (not= false purge-progress-state)
            snapshot-values      (mapv snapshot-result ok-snapshots)]
        (let [summary {:valid?                    (and (empty? unresolved-writes)
                                                       (empty? snapshot-failures)
                                                       (boolean stable-pair)
                                                       refresh-progress?
                                                       purge-progress-ok?)
                       :write-count               (count write-ops)
                       :snapshot-count            (count snapshot-ops)
                       :snapshot-fail-count       (count snapshot-failures)
                       :stable-after-quiet?       (boolean stable-pair)
                       :post-fault-refresh?       (boolean refresh-progress?)
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
  {:client          (MVAutoschedClient. nil nil (atom false) (atom nil))
   :generator       (gen/stagger 1/5 (generator))
   :checker         (checker/compose {:mv-autosched (checker*)
                                      :timeline     (timeline/html)})
   :final-generator (quiet-generator)})
