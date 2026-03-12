(ns tidb.mview
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [tidb.sql :as c]))

(def base-table "mv_stateful_base")
(def row-view "mv_stateful_row")
(def agg-view "mv_stateful_agg")
(def refresh-lock-retry-count 5)
(def refresh-lock-retry-ms 250)

(declare try-statements!)

(defn execute-safely!
  ([conn stmt]
   (execute-safely! conn stmt nil))
  ([conn stmt err-msg]
   (try
     (c/execute! conn [stmt])
     true
     (catch java.sql.SQLException e
       (when err-msg
         (info err-msg (.getMessage e)))
       false))))

(defn drop-artifacts!
  [conn]
  (doseq [stmt [(str "DROP MATERIALIZED VIEW IF EXISTS " agg-view)
                (str "DROP MATERIALIZED VIEW IF EXISTS " row-view)
                (str "DROP MATERIALIZED VIEW LOG ON " base-table)
                (str "DROP TABLE IF EXISTS " base-table)]]
    (execute-safely! conn stmt "Ignoring MV cleanup error:")))

(defn create-base-table!
  [conn]
  (c/execute! conn
              [(str "CREATE TABLE IF NOT EXISTS " base-table "\n"
                    "(id         int          NOT NULL PRIMARY KEY,\n"
                    " g1         int          NOT NULL,\n"
                    " v1         bigint       NOT NULL,\n"
                    " version    bigint       NOT NULL,\n"
                    " last_token varchar(128) NOT NULL,\n"
                    " deleted    tinyint      NOT NULL DEFAULT 0,\n"
                    " pad        varchar(64)  NOT NULL,\n"
                    " KEY idx_mv_stateful_g1_deleted_v1 (g1, deleted, v1))")]))

(defn split-base-table!
  [conn ids]
  (let [splits (->> ids
                    (filter even?)
                    (map #(str "(" % ")"))
                    (str/join ","))]
    (when-not (str/blank? splits)
      (c/execute! conn [(str "split table " base-table " by " splits)]))))

(defn create-mlog!
  [conn]
  (try-statements!
   conn
   (str "Create MLOG failed for " base-table)
   [(str "CREATE MATERIALIZED VIEW LOG ON " base-table
         " (id, g1, v1, version, last_token, deleted)")
    (str "CREATE MATERIALIZED VIEW LOG ON " base-table
         " (id, g1, v1, version, last_token, deleted) PURGE IMMEDIATE")]))

(defn drop-row-view!
  [conn]
  (let [stmt (str "DROP MATERIALIZED VIEW IF EXISTS " row-view)]
    (c/execute! conn [stmt])
    stmt))

(defn drop-agg-view!
  [conn]
  (let [stmt (str "DROP MATERIALIZED VIEW IF EXISTS " agg-view)]
    (c/execute! conn [stmt])
    stmt))

(defn drop-mlog!
  [conn]
  (let [stmt (str "DROP MATERIALIZED VIEW LOG ON " base-table)]
    (c/execute! conn [stmt])
    stmt))

(defn create-mlog-with-purge-schedule!
  [conn start-delay next-seconds]
  (or (try
        (try-statements!
         conn
         (str "Create scheduled MLOG failed for " base-table)
         [(str "CREATE MATERIALIZED VIEW LOG ON " base-table
               " (id, g1, v1, version, last_token, deleted) "
               "PURGE START WITH (NOW() + INTERVAL " start-delay " SECOND) "
               "NEXT " next-seconds)
          (str "CREATE MATERIALIZED VIEW LOG ON " base-table
               " (id, g1, v1, version, last_token, deleted) "
               "START WITH (NOW() + INTERVAL " start-delay " SECOND) "
               "NEXT " next-seconds)])
        (catch java.sql.SQLException _
          nil))
      (create-mlog! conn)))

(defn create-row-view!
  [conn]
  (c/execute! conn
              [(str "CREATE MATERIALIZED VIEW " row-view " "
                    "(id, g1, v1, version, last_token, live_cnt) "
                    "COMMENT = 'jepsen:mv-stateful(row)' "
                    "REFRESH FAST AS "
                    "SELECT id, g1, v1, version, last_token, COUNT(*) AS live_cnt "
                    "FROM " base-table " "
                    "WHERE deleted = 0 "
                    "GROUP BY id, g1, v1, version, last_token")]))

(defn create-agg-view!
  [conn]
  (c/execute! conn
              [(str "CREATE MATERIALIZED VIEW " agg-view " "
                    "(g1, cnt, sum_v1, min_v1, max_v1) "
                    "COMMENT = 'jepsen:mv-stateful(agg)' "
                    "REFRESH FAST AS "
                    "SELECT g1, "
                    "COUNT(*) AS cnt, "
                    "SUM(v1)  AS sum_v1, "
                    "MIN(v1)  AS min_v1, "
                    "MAX(v1)  AS max_v1 "
                    "FROM " base-table " "
                    "WHERE deleted = 0 "
                    "GROUP BY g1")]))

(defn- refresh-lock-conflict?
  [^java.sql.SQLException e]
  (let [message (str (.getMessage e))]
    (boolean
     (re-find #"lock\(s\) could not be acquired immediately|NOWAIT is set"
              message))))

(defn- try-statements-once!
  [conn label statements]
  (loop [remaining statements
         last-error nil]
    (if-let [stmt (first remaining)]
      (let [result (try
                     {:stmt (do (c/execute! conn [stmt]) stmt)}
                     (catch java.sql.SQLException e
                       (info label (.getMessage e) "stmt=" stmt)
                       (if (refresh-lock-conflict? e)
                         (throw e)
                         {:error e})))]
        (if-let [ok-stmt (:stmt result)]
          ok-stmt
          (recur (rest remaining) (:error result))))
      (if last-error
        (throw last-error)
        nil))))

(defn- try-statements!
  [conn label statements]
  (loop [attempt 1]
    (let [result (try
                   {:stmt (try-statements-once! conn label statements)}
                   (catch java.sql.SQLException e
                     {:error e}))]
      (if-let [stmt (:stmt result)]
        stmt
        (let [e (:error result)]
          (if (and (refresh-lock-conflict? e)
                   (< attempt refresh-lock-retry-count))
            (do
              (info label "Retrying after transient lock conflict"
                    "attempt=" attempt
                    "error=" (.getMessage e))
              (Thread/sleep refresh-lock-retry-ms)
              (recur (inc attempt)))
            (throw e)))))))

(defn refresh-view!
  [conn view]
  (try-statements!
   conn
   (str "Refresh failed for " view)
   [(str "REFRESH MATERIALIZED VIEW " view " WITH SYNC MODE FAST")
    (str "REFRESH MATERIALIZED VIEW " view " FAST WITH SYNC MODE")
    (str "REFRESH MATERIALIZED VIEW " view " WITH SYNC MODE")
    (str "REFRESH MATERIALIZED VIEW " view)]))

(defn purge-log!
  [conn]
  (try-statements!
   conn
   (str "Purge failed for " base-table)
   [(str "PURGE MATERIALIZED VIEW LOG ON " base-table)
    (str "ALTER MATERIALIZED VIEW LOG ON " base-table " PURGE")
    (str "ALTER MATERIALIZED VIEW LOG ON " base-table " PURGE IMMEDIATE")]))

(defn schedule-refresh!
  [conn view start-delay next-seconds]
  (try-statements!
   conn
   (str "Schedule refresh failed for " view)
   [(str "ALTER MATERIALIZED VIEW " view
         " REFRESH START WITH (NOW() + INTERVAL " start-delay " SECOND) NEXT " next-seconds)
    (str "ALTER MATERIALIZED VIEW " view
         " START WITH (NOW() + INTERVAL " start-delay " SECOND) NEXT " next-seconds)]))

(defn schedule-purge!
  [conn start-delay next-seconds]
  (try-statements!
   conn
   (str "Schedule purge failed for " base-table)
   [(str "ALTER MATERIALIZED VIEW LOG ON " base-table
         " PURGE START WITH (NOW() + INTERVAL " start-delay " SECOND) NEXT " next-seconds)
    (str "ALTER MATERIALIZED VIEW LOG ON " base-table
         " START WITH (NOW() + INTERVAL " start-delay " SECOND) NEXT " next-seconds)]))

(defn list-table-names
  [conn]
  (->> (c/query conn ["SHOW TABLES"])
       (map (fn [row]
              (some-> row vals first str)))
       (remove nil?)
       set))

(defn artifact-state
  [conn]
  (let [tables    (list-table-names conn)
        log-table (first (sort (remove #{base-table row-view agg-view} tables)))]
    {:base-table-present? (contains? tables base-table)
     :row-view-present?   (contains? tables row-view)
     :agg-view-present?   (contains? tables agg-view)
     :mlog-present?       (some? log-table)
     :log-table           log-table
     :tables              (sort tables)}))

(defn count-table-rows
  [conn table-name]
  (try
    (some-> (c/query conn [(str "SELECT COUNT(*) AS cnt FROM " table-name)] {:row-fn :cnt})
            first
            long)
    (catch java.sql.SQLException _
      nil)))

(defn- ddl-text
  [row]
  (->> (vals row)
       (filter string?)
       (sort-by count >)
       first))

(defn- query-ddl
  [conn object-name candidates]
  (loop [remaining candidates
         last-error nil]
    (if-let [stmt (first remaining)]
      (let [result (try
                     (if-let [row (first (c/query conn [stmt]))]
                       {:row row}
                       {:missing? true})
                     (catch java.sql.SQLException e
                       {:error e}))]
        (cond
          (:row result)
          {:object     object-name
           :available? true
           :query      stmt
           :ddl        (ddl-text (:row result))}

          (:error result)
          (recur (rest remaining) (:error result))

          :else
          (recur (rest remaining) last-error)))
      {:object     object-name
       :available? false
       :error      (some-> last-error .getMessage)})))

(defn- parse-next-literal
  [ddl]
  (some->> ddl
           (re-find #"(?i)\bNEXT\s+([0-9]+)")
           second
           Long/parseLong))

(defn- ddl-schedule-summary
  [kind object-name expected-next-seconds observation]
  (let [ddl         (:ddl observation)
        ddl-upper   (some-> ddl str/upper-case)
        start-with? (boolean (and ddl-upper (str/includes? ddl-upper "START WITH")))
        next?       (boolean (and ddl-upper (re-find #"\bNEXT\s+[0-9]+" ddl-upper)))
        refresh?    (boolean (and ddl-upper (str/includes? ddl-upper "REFRESH")))
        purge?      (boolean (and ddl-upper (str/includes? ddl-upper "PURGE")))
        next-value  (parse-next-literal ddl)]
    (merge observation
           {:kind                   kind
            :object                 object-name
            :has-start-with?        start-with?
            :has-next?              next?
            :has-refresh?           refresh?
            :has-purge?             purge?
            :schedule-visible?      (and start-with? next?)
            :next-literal           next-value
            :expected-next-seconds  expected-next-seconds
            :next-matches-expected? (when (and next-value expected-next-seconds)
                                      (= next-value expected-next-seconds))})))

(defn- read-view-schedule-metadata
  [conn view expected-next-seconds]
  (->> [(str "SHOW CREATE MATERIALIZED VIEW " view)
        (str "SHOW CREATE TABLE " view)
        (str "SHOW CREATE VIEW " view)]
       (query-ddl conn view)
       (ddl-schedule-summary :refresh view expected-next-seconds)))

(defn- read-log-schedule-metadata
  [conn log-table expected-next-seconds]
  (if log-table
    (->> [(str "SHOW CREATE TABLE " log-table)
          (str "SHOW CREATE MATERIALIZED VIEW " log-table)]
         (query-ddl conn log-table)
         (ddl-schedule-summary :purge log-table expected-next-seconds))
    {:kind :purge
     :object nil
     :available? false
     :error "mlog backing table not discovered"}))

(defn read-schedule-metadata
  [conn {:keys [log-table row-refresh-seconds agg-refresh-seconds purge-next-seconds]}]
  {:row-refresh (read-view-schedule-metadata conn row-view row-refresh-seconds)
   :agg-refresh (read-view-schedule-metadata conn agg-view agg-refresh-seconds)
   :log-purge   (read-log-schedule-metadata conn log-table purge-next-seconds)})

(defn normalize-base-row
  [row]
  (when (and row (zero? (long (:deleted row))))
    {:id         (long (:id row))
     :g1         (long (:g1 row))
     :v1         (long (:v1 row))
     :version    (long (:version row))
     :last-token (:last_token row)}))

(defn normalize-mv-row
  [row]
  (when row
    {:id         (long (:id row))
     :g1         (long (:g1 row))
     :v1         (long (:v1 row))
     :version    (long (:version row))
     :last-token (:last_token row)
     :live-cnt   (long (:live_cnt row))}))

(defn query-base-row
  [conn id]
  (-> (c/query conn [(str "SELECT id, g1, v1, version, last_token, deleted "
                           "FROM " base-table " WHERE id = ?")
                     id])
      first
      normalize-base-row))

(defn query-stored-row
  [conn id]
  (first (c/query conn [(str "SELECT id, g1, v1, version, last_token, deleted, pad "
                              "FROM " base-table " WHERE id = ?")
                        id])))

(defn query-mv-row
  [conn id]
  (-> (c/query conn [(str "SELECT id, "
                           "MAX(g1) AS g1, "
                           "MAX(v1) AS v1, "
                           "MAX(version) AS version, "
                           "MAX(last_token) AS last_token, "
                           "SUM(live_cnt) AS live_cnt "
                           "FROM " row-view " WHERE id = ? "
                           "GROUP BY id")
                     id])
      first
      normalize-mv-row))

(defn query-base-row-projection
  [conn]
  (->> (c/query conn [(str "SELECT id, g1, v1, version, last_token "
                           "FROM " base-table " WHERE deleted = 0 ORDER BY id")])
       (map (fn [row]
              [(long (:id row))
               {:id         (long (:id row))
                :g1         (long (:g1 row))
                :v1         (long (:v1 row))
                :version    (long (:version row))
                :last-token (:last_token row)}]))
       (into (sorted-map))))

(defn query-mv-row-projection
  [conn]
  (->> (c/query conn [(str "SELECT id, "
                           "MAX(g1) AS g1, "
                           "MAX(v1) AS v1, "
                           "MAX(version) AS version, "
                           "MAX(last_token) AS last_token, "
                           "SUM(live_cnt) AS live_cnt "
                           "FROM " row-view " "
                           "GROUP BY id ORDER BY id")])
       (map (fn [row]
              [(long (:id row))
               {:id         (long (:id row))
                :g1         (long (:g1 row))
                :v1         (long (:v1 row))
                :version    (long (:version row))
                :last-token (:last_token row)
                :live-cnt   (long (:live_cnt row))}]))
       (into (sorted-map))))

(defn normalize-agg-row
  [row]
  {:g1     (long (:g1 row))
   :cnt    (long (:cnt row))
   :sum-v1 (long (:sum_v1 row))
   :min-v1 (long (:min_v1 row))
   :max-v1 (long (:max_v1 row))})

(defn query-base-agg
  [conn]
  (->> (c/query conn [(str "SELECT g1, COUNT(*) AS cnt, SUM(v1) AS sum_v1, "
                           "MIN(v1) AS min_v1, MAX(v1) AS max_v1 "
                           "FROM " base-table " WHERE deleted = 0 "
                           "GROUP BY g1 ORDER BY g1")])
       (map (fn [row]
              [(long (:g1 row)) (normalize-agg-row row)]))
       (into (sorted-map))))

(defn query-mv-agg
  [conn]
  (->> (c/query conn [(str "SELECT g1, cnt, sum_v1, min_v1, max_v1 "
                           "FROM " agg-view " ORDER BY g1")])
       (map (fn [row]
              [(long (:g1 row)) (normalize-agg-row row)]))
       (into (sorted-map))))

(defn row-diff
  [expected actual]
  (let [actual-row (some-> actual (dissoc :live-cnt))
        diff       (cond-> {}
                     (not= expected actual-row)
                     (assoc :expected expected
                            :actual actual-row)

                     (and actual (not= 1 (:live-cnt actual)))
                     (assoc :live-cnt (:live-cnt actual)))]
    (when (seq diff)
      diff)))

(defn full-row-diff
  [expected actual]
  (let [expected-keys (set (keys expected))
        actual-keys   (set (keys actual))
        missing       (sort (seq (set/difference expected-keys actual-keys)))
        unexpected    (sort (seq (set/difference actual-keys expected-keys)))
        mismatched    (->> (set/intersection expected-keys actual-keys)
                           sort
                           (keep (fn [k]
                                   (let [e (get expected k)
                                         a (get actual k)
                                         actual-row (some-> a (dissoc :live-cnt))]
                                     (when (not= e actual-row)
                                       [k {:expected e :actual actual-row}]))))
                           (into (sorted-map)))
        bad-live-cnt  (->> actual
                           (keep (fn [[k row]]
                                   (when (not= 1 (:live-cnt row))
                                     [k (:live-cnt row)])))
                           (into (sorted-map)))]
    (cond-> {}
      (seq missing)    (assoc :missing-in-mv missing)
      (seq unexpected) (assoc :unexpected-in-mv unexpected)
      (seq mismatched) (assoc :mismatched mismatched)
      (seq bad-live-cnt) (assoc :bad-live-cnt bad-live-cnt))))

(defn agg-diff
  [expected actual]
  (let [expected-keys (set (keys expected))
        actual-keys   (set (keys actual))
        missing       (sort (seq (set/difference expected-keys actual-keys)))
        unexpected    (sort (seq (set/difference actual-keys expected-keys)))
        mismatched    (->> (set/intersection expected-keys actual-keys)
                           sort
                           (keep (fn [k]
                                   (let [e (get expected k)
                                         a (get actual k)
                                         actual-row (some-> a (dissoc :live-cnt))]
                                     (when (not= e actual-row)
                                       [k {:expected e :actual actual-row}]))))
                           (into (sorted-map)))]
    (cond-> {}
      (seq missing)    (assoc :missing-in-mv missing)
      (seq unexpected) (assoc :unexpected-in-mv unexpected)
      (seq mismatched) (assoc :mismatched mismatched))))

(defn setup-stateful-schema!
  [conn ids]
  (drop-artifacts! conn)
  (create-base-table! conn)
  (split-base-table! conn ids)
  (create-mlog! conn)
  (create-row-view! conn)
  (create-agg-view! conn))

(defn setup-autosched-schema!
  [conn ids {:keys [refresh-start-delay row-refresh-seconds agg-refresh-seconds purge-start-delay purge-next-seconds]}]
  (drop-artifacts! conn)
  (create-base-table! conn)
  (split-base-table! conn ids)
  (let [tables-before (list-table-names conn)
        mlog-stmt     (create-mlog-with-purge-schedule! conn purge-start-delay purge-next-seconds)
        tables-after  (list-table-names conn)
        log-table     (first (sort (remove #{base-table row-view agg-view}
                                           (clojure.set/difference tables-after tables-before))))
        _             (create-row-view! conn)
        _             (create-agg-view! conn)
        row-stmt      (schedule-refresh! conn row-view refresh-start-delay row-refresh-seconds)
        agg-stmt      (schedule-refresh! conn agg-view refresh-start-delay agg-refresh-seconds)
        purge-stmt    (or (try (schedule-purge! conn purge-start-delay purge-next-seconds)
                               (catch java.sql.SQLException _ nil))
                          mlog-stmt)]
    {:log-table            log-table
     :mlog-stmt            mlog-stmt
     :row-refresh-stmt     row-stmt
     :agg-refresh-stmt     agg-stmt
     :purge-schedule-stmt  purge-stmt
     :row-refresh-seconds  row-refresh-seconds
     :agg-refresh-seconds  agg-refresh-seconds
     :purge-next-seconds   purge-next-seconds}))
