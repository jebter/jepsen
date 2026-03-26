(ns tidb.mview
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [tidb.sql :as c]))

(def base-table "mv_stateful_base")
(def row-view "mv_stateful_row")
(def agg-view "mv_stateful_agg")
(def timer-table "mysql.tidb_timers")
(def mview-refresh-info-table "mysql.tidb_mview_refresh_info")
(def mlog-purge-info-table "mysql.tidb_mlog_purge_info")
(def mview-refresh-hist-table "mysql.tidb_mview_refresh_hist")
(def mlog-purge-hist-table "mysql.tidb_mlog_purge_hist")
(def recent-runtime-history-limit 8)
(def refresh-lock-retry-count 8)
(def refresh-lock-retry-base-ms 250)
(def refresh-lock-retry-max-ms 1000)
(def setup-ddl-timeout-sec 60)

(declare try-statements!)

(defn- schedule-timestamp-exprs
  [seconds]
  [(str "NOW(0) + INTERVAL " seconds " SECOND")
   (str "(NOW() + INTERVAL " seconds " SECOND)")
   (str "DATE_ADD(NOW(0), INTERVAL " seconds " SECOND)")
   (str "(DATE_ADD(NOW(0), INTERVAL " seconds " SECOND))")
   (str "DATE_ADD(NOW(), INTERVAL " seconds " SECOND)")
   (str "(DATE_ADD(NOW(), INTERVAL " seconds " SECOND))")])

(defn- schedule-clause-pairs
  [start-delay next-seconds]
  (map vector
       (schedule-timestamp-exprs start-delay)
       (schedule-timestamp-exprs next-seconds)))

(defn execute-safely!
  ([conn stmt]
   (execute-safely! conn stmt nil {}))
  ([conn stmt err-msg]
   (execute-safely! conn stmt err-msg {}))
  ([conn stmt err-msg opts]
   (try
     (c/execute! conn [stmt] opts)
     true
     (catch java.sql.SQLException e
       (when err-msg
         (info err-msg (.getMessage e)))
       false))))

(defn drop-artifacts!
  [conn]
  (doseq [stmt [(str "DROP MATERIALIZED VIEW " agg-view)
                (str "DROP MATERIALIZED VIEW " row-view)
                (str "DROP MATERIALIZED VIEW LOG ON " base-table)
                (str "DROP TABLE IF EXISTS " base-table)]]
    (execute-safely! conn
                     stmt
                     "Ignoring MV cleanup error:"
                     {:timeout setup-ddl-timeout-sec})))

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
                    " KEY idx_mv_stateful_g1_deleted_v1 (g1, deleted, v1))")]
              {:timeout setup-ddl-timeout-sec}))

(defn split-base-table!
  [conn ids]
  (let [splits (->> ids
                    (filter even?)
                    (map #(str "(" % ")"))
                    (str/join ","))]
    (when-not (str/blank? splits)
      (execute-safely! conn
                       (str "split table " base-table " by " splits)
                       "Ignoring unsupported split-table syntax:"
                       {:timeout setup-ddl-timeout-sec}))))

(defn create-mlog!
  [conn]
  (try-statements!
   conn
   (str "Create MLOG failed for " base-table)
   [(str "CREATE MATERIALIZED VIEW LOG ON " base-table
         " (id, g1, v1, version, last_token, deleted)")
    (str "CREATE MATERIALIZED VIEW LOG ON " base-table
         " (id, g1, v1, version, last_token, deleted) PURGE IMMEDIATE")]
   {:timeout setup-ddl-timeout-sec}))

(defn drop-row-view!
  [conn]
  (let [stmt (str "DROP MATERIALIZED VIEW " row-view)]
    (c/execute! conn [stmt])
    stmt))

(defn drop-agg-view!
  [conn]
  (let [stmt (str "DROP MATERIALIZED VIEW " agg-view)]
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
         (vec
          (mapcat (fn [[start-expr next-expr]]
                    [(str "CREATE MATERIALIZED VIEW LOG ON " base-table
                          " (id, g1, v1, version, last_token, deleted) "
                          "PURGE START WITH " start-expr " "
                          "NEXT " next-expr)
                     (str "CREATE MATERIALIZED VIEW LOG ON " base-table
                          " (id, g1, v1, version, last_token, deleted) "
                          "START WITH " start-expr " "
                          "NEXT " next-expr)])
                  (schedule-clause-pairs start-delay next-seconds)))
         {:timeout setup-ddl-timeout-sec})
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
                    "GROUP BY id, g1, v1, version, last_token")]
              {:timeout setup-ddl-timeout-sec}))

(defn create-row-view-with-schedule!
  [conn start-delay next-seconds]
  (try-statements!
   conn
   (str "Create scheduled view failed for " row-view)
   (mapv (fn [[start-expr next-expr]]
           (str "CREATE MATERIALIZED VIEW " row-view " "
                "(id, g1, v1, version, last_token, live_cnt) "
                "COMMENT = 'jepsen:mv-stateful(row)' "
                "REFRESH FAST START WITH " start-expr " "
                "NEXT " next-expr " AS "
                "SELECT id, g1, v1, version, last_token, COUNT(*) AS live_cnt "
                "FROM " base-table " "
                "WHERE deleted = 0 "
                "GROUP BY id, g1, v1, version, last_token"))
         (schedule-clause-pairs start-delay next-seconds))
   {:timeout setup-ddl-timeout-sec}))

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
                    "GROUP BY g1")]
              {:timeout setup-ddl-timeout-sec}))

(defn create-agg-view-with-schedule!
  [conn start-delay next-seconds]
  (try-statements!
   conn
   (str "Create scheduled view failed for " agg-view)
   (mapv (fn [[start-expr next-expr]]
           (str "CREATE MATERIALIZED VIEW " agg-view " "
                "(g1, cnt, sum_v1, min_v1, max_v1) "
                "COMMENT = 'jepsen:mv-stateful(agg)' "
                "REFRESH FAST START WITH " start-expr " "
                "NEXT " next-expr " AS "
                "SELECT g1, "
                "COUNT(*) AS cnt, "
                "SUM(v1)  AS sum_v1, "
                "MIN(v1)  AS min_v1, "
                "MAX(v1)  AS max_v1 "
                "FROM " base-table " "
                "WHERE deleted = 0 "
                "GROUP BY g1"))
         (schedule-clause-pairs start-delay next-seconds))
   {:timeout setup-ddl-timeout-sec}))

(defn- refresh-lock-conflict?
  [^java.sql.SQLException e]
  (let [message (str (.getMessage e))]
    (boolean
     (re-find #"lock\(s\) could not be acquired immediately|NOWAIT is set"
              message))))

(defn- refresh-lock-retry-delay-ms
  [attempt]
  (long
   (min refresh-lock-retry-max-ms
        (* refresh-lock-retry-base-ms
           (bit-shift-left 1 (max 0 (dec attempt)))))))

(defn- missing-object-error?
  [^java.sql.SQLException e]
  (let [message (str (.getMessage e))]
    (boolean
     (re-find #"(?i)(\[(schema|planner):1146\].*doesn't exist|unknown table|unknown view|unknown materialized view|table '.*' doesn't exist)"
              message))))

(defn- try-statements-once!
  ([conn label statements]
   (try-statements-once! conn label statements {}))
  ([conn label statements opts]
  (loop [remaining statements
         last-error nil]
    (if-let [stmt (first remaining)]
      (let [result (try
                     {:stmt (do (c/execute! conn [stmt] opts) stmt)}
                     (catch java.sql.SQLException e
                       (info label (.getMessage e) "stmt=" stmt)
                       (if (or (refresh-lock-conflict? e)
                               (missing-object-error? e))
                         (throw e)
                         {:error e})))]
        (if-let [ok-stmt (:stmt result)]
          ok-stmt
          (recur (rest remaining) (:error result))))
      (if last-error
        (throw last-error)
        nil)))))

(defn- try-statements!
  ([conn label statements]
   (try-statements! conn label statements {}))
  ([conn label statements opts]
   (loop [attempt 1]
     (let [result (try
                    {:stmt (try-statements-once! conn label statements opts)}
                    (catch java.sql.SQLException e
                      {:error e}))]
       (if-let [stmt (:stmt result)]
         stmt
         (let [e (:error result)]
           (if (and (refresh-lock-conflict? e)
                    (< attempt refresh-lock-retry-count))
             (let [delay-ms (refresh-lock-retry-delay-ms attempt)]
               (info label "Retrying after transient lock conflict"
                     "attempt=" attempt
                     "sleep-ms=" delay-ms
                     "error=" (.getMessage e))
               (Thread/sleep delay-ms)
               (recur (inc attempt)))
             (throw e))))))))

(defn refresh-view-statements
  [view]
  ;; Current TiDB builds reject the older WITH SYNC MODE forms, so keep the
  ;; explicit refresh fallback list to the two supported non-sync variants.
  [(str "REFRESH MATERIALIZED VIEW " view " FAST")
   (str "REFRESH MATERIALIZED VIEW " view " COMPLETE")])

(defn refresh-view!
  [conn view]
  (try-statements!
   conn
   (str "Refresh failed for " view)
   (refresh-view-statements view)))

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
   (vec
    (mapcat (fn [[start-expr next-expr]]
              [(str "ALTER MATERIALIZED VIEW " view
                    " REFRESH START WITH " start-expr
                    " NEXT " next-expr)
               (str "ALTER MATERIALIZED VIEW " view
                    " START WITH " start-expr
                    " NEXT " next-expr)])
            (schedule-clause-pairs start-delay next-seconds)))
   {:timeout setup-ddl-timeout-sec}))

(defn schedule-purge!
  [conn start-delay next-seconds]
  (try-statements!
   conn
   (str "Schedule purge failed for " base-table)
   (vec
    (mapcat (fn [[start-expr next-expr]]
              [(str "ALTER MATERIALIZED VIEW LOG ON " base-table
                    " PURGE START WITH " start-expr
                    " NEXT " next-expr)
               (str "ALTER MATERIALIZED VIEW LOG ON " base-table
                    " START WITH " start-expr
                    " NEXT " next-expr)])
            (schedule-clause-pairs start-delay next-seconds)))
   {:timeout setup-ddl-timeout-sec}))

(defn list-table-names
  [conn]
  (->> (c/query conn ["SHOW TABLES"])
       (map (fn [row]
              (some-> row vals first str)))
       (remove nil?)
       set))

(defn- mlog-table-name?
  [table-name]
  (and table-name
       (str/starts-with? table-name "$mlog$")))

(defn artifact-state
  [conn]
  (let [tables    (list-table-names conn)
        log-table (first (sort (filter mlog-table-name?
                                       (remove #{base-table row-view agg-view} tables))))]
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
  (or (some->> ddl
               (re-find #"(?i)\bNEXT\s+([0-9]+)\b")
               second
               Long/parseLong)
      (some->> ddl
               (re-find #"(?i)\bNEXT\s+\(?\s*(?:NOW(?:\(\d*\))?\s*\+\s*)?INTERVAL\s+([0-9]+)\s+SECOND\b")
               second
               Long/parseLong)
      (some->> ddl
               (re-find #"(?i)\bNEXT\s+\(?\s*DATE_ADD\s*\(\s*NOW(?:\(\d*\))?\s*,\s*INTERVAL\s+([0-9]+)\s+SECOND\s*\)\s*\)?")
               second
               Long/parseLong)))

(defn- ddl-schedule-summary
  [kind object-name expected-next-seconds observation]
  (let [ddl         (:ddl observation)
        ddl-upper   (some-> ddl str/upper-case)
        start-with? (boolean (and ddl-upper (str/includes? ddl-upper "START WITH")))
        next?       (boolean (and ddl-upper (re-find #"\bNEXT\b" ddl-upper)))
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
            :next-matches-expected? (when (and next? expected-next-seconds)
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

(defn- ->long
  [x]
  (when (some? x)
    (long x)))

(defn- byte-array-value?
  [x]
  (= (class x) (Class/forName "[B")))

(defn- printable-value
  [x]
  (cond
    (nil? x) nil
    (byte-array-value? x) (String. ^bytes x java.nio.charset.StandardCharsets/UTF_8)
    :else (str x)))

(defn- timer-match-fields
  [row]
  (->> [(:timer_key row)
        (:hook_class row)
        (:timer_ext row)
        (:timer_data row)
        (:summary_data row)
        (:event_data row)]
       (map printable-value)
       (remove str/blank?)))

(defn- timer-match-tokens
  [{:keys [log-table]}]
  (->> [row-view agg-view log-table base-table]
       (remove str/blank?)
       distinct
       vec))

(defn- matched-timer-tokens
  [fields tokens]
  (->> tokens
       (filter (fn [token]
                 (some #(str/includes? % token) fields)))
       vec))

(defn- classify-timer-match
  [matched-tokens]
  (cond-> []
    (some #{row-view} matched-tokens) (conj :row-refresh)
    (some #{agg-view} matched-tokens) (conj :agg-refresh)
    (or (some #(or (= % base-table)
                   (str/starts-with? % "$mlog$"))
              matched-tokens)
        (some #(str/starts-with? % "$mlog$")
              matched-tokens))
    (conj :log-purge)))

(defn- normalize-timer-row
  [row tokens]
  (let [fields         (timer-match-fields row)
        matched-tokens (matched-timer-tokens fields tokens)
        event-status   (some-> (:event_status row) str/upper-case)
        timer-ext      (printable-value (:timer_ext row))
        timer-data     (printable-value (:timer_data row))
        summary-data   (printable-value (:summary_data row))
        event-data     (printable-value (:event_data row))]
    (cond-> {:id                 (->long (:id row))
             :namespace          (some-> (:namespace row) str)
             :timer-key          (some-> (:timer_key row) str)
             :time-zone          (some-> (:timezone row) str)
             :sched-policy-type  (some-> (:sched_policy_type row) str)
             :sched-policy-expr  (some-> (:sched_policy_expr row) str)
             :hook-class         (some-> (:hook_class row) str)
             :watermark          (printable-value (:watermark row))
             :enable?            (boolean (and (some? (:enable row))
                                               (not (zero? (long (:enable row))))))
             :event-status       event-status
             :event-id           (some-> (:event_id row) str)
             :event-start        (printable-value (:event_start row))
             :create-time        (printable-value (:create_time row))
             :update-time        (printable-value (:update_time row))
             :version            (->long (:version row))
             :matched-tokens     matched-tokens
             :matched-components (classify-timer-match matched-tokens)}
      (not (str/blank? timer-ext))
      (assoc :timer-ext timer-ext)

      (not (str/blank? timer-data))
      (assoc :timer-data timer-data)

      (not (str/blank? summary-data))
      (assoc :summary-data summary-data)

      (not (str/blank? event-data))
      (assoc :event-data event-data))))

(defn read-timer-metadata
  [conn schedule-meta]
  (let [tokens (timer-match-tokens schedule-meta)]
    (try
      (let [rows         (c/query conn [(str "SELECT id, namespace, timer_key, timezone, "
                                             "sched_policy_type, sched_policy_expr, hook_class, "
                                             "watermark, enable, event_status, event_id, "
                                             "event_start, create_time, update_time, version, "
                                             "timer_ext, timer_data, summary_data, event_data "
                                             "FROM " timer-table " ORDER BY id")])
            normalized   (mapv #(normalize-timer-row % tokens) rows)
            matched      (->> normalized
                              (filter #(seq (:matched-tokens %)))
                              vec)
            sample-rows  (->> normalized
                              (take 5)
                              vec)]
        {:available?     true
         :source         :timers
         :table          timer-table
         :object-tokens  tokens
         :total-count    (count normalized)
         :match-count    (count matched)
         :rows           matched
         :sample-rows    (when (zero? (count matched))
                           sample-rows)})
      (catch java.sql.SQLException e
        {:available?    false
         :source        :timers
         :table         timer-table
         :object-tokens tokens
         :error         (.getMessage e)}))))

(defn- query-system-runtime-rows
  [conn sql-prefix object-names]
  (let [placeholders (str/join ", " (repeat (count object-names) "?"))
        sql          (str sql-prefix
                          " ("
                          placeholders
                          ") "
                          "ORDER BY t.table_name")]
    (c/query conn (into [sql] object-names))))

(defn- query-refresh-runtime-rows
  [conn object-names]
  (when (seq object-names)
    (query-system-runtime-rows
     conn
     (str "SELECT t.table_name AS object_name, "
          "t.tidb_table_id AS object_id, "
          "CASE WHEN i.mview_id IS NULL THEN 0 ELSE 1 END AS info_present, "
          "CAST(ROUND(UNIX_TIMESTAMP(i.next_time) * 1000) AS SIGNED) AS next_time_ms, "
          "i.last_success_read_tso AS last_success_read_tso "
          "FROM information_schema.tables t "
          "LEFT JOIN " mview-refresh-info-table " i ON i.mview_id = t.tidb_table_id "
          "WHERE t.table_schema = DATABASE() AND t.table_name IN")
     object-names)))

(defn- query-purge-runtime-rows
  [conn object-names]
  (when (seq object-names)
    (query-system-runtime-rows
     conn
     (str "SELECT t.table_name AS object_name, "
          "t.tidb_table_id AS object_id, "
          "CASE WHEN i.mlog_id IS NULL THEN 0 ELSE 1 END AS info_present, "
          "CAST(ROUND(UNIX_TIMESTAMP(i.next_time) * 1000) AS SIGNED) AS next_time_ms, "
          "i.last_purged_tso AS last_purged_tso "
          "FROM information_schema.tables t "
          "LEFT JOIN " mlog-purge-info-table " i ON i.mlog_id = t.tidb_table_id "
          "WHERE t.table_schema = DATABASE() AND t.table_name IN")
     object-names)))

(defn- index-rows-by-object-name
  [rows]
  (into {}
        (map (fn [row]
               [(some-> (:object_name row) str) row]))
        rows))

(defn- info-present?
  [row]
  (boolean (and (some? (:info_present row))
                (not (zero? (long (:info_present row)))))))

(defn- runtime-row-base
  [component object-name system-table row]
  (let [info? (and row (info-present? row))]
    {:component          component
     :object             object-name
     :system-table       system-table
     :object-id          (some-> row :object_id ->long)
     :info-present?      (boolean info?)
     :next-time-ms       (some-> row :next_time_ms ->long)
     :next-time-present? (some? (some-> row :next_time_ms ->long))
     :matched-tokens     (if info? [object-name] [])
     :matched-components (if info? [component] [])
     :error              (when-not row
                           "object not found in information_schema.tables")}))

(defn- normalize-refresh-runtime-row
  [component object-name row]
  (assoc (runtime-row-base component object-name mview-refresh-info-table row)
         :last-success-read-tso (some-> row :last_success_read_tso ->long)))

(defn- normalize-purge-runtime-row
  [object-name row]
  (assoc (runtime-row-base :log-purge object-name mlog-purge-info-table row)
         :last-purged-tso (some-> row :last_purged_tso ->long)))

(defn- query-system-history-rows
  [conn sql-prefix object-names]
  (let [placeholders (str/join ", " (repeat (count object-names) "?"))
        sql          (str sql-prefix
                          " ("
                          placeholders
                          ") "
                          "ORDER BY t.table_name, job_id DESC")]
    (c/query conn (into [sql] object-names))))

(defn- query-refresh-history-rows
  [conn object-names]
  (when (seq object-names)
    (query-system-history-rows
     conn
     (str "SELECT t.table_name AS object_name, "
          "t.tidb_table_id AS object_id, "
          "h.refresh_job_id AS job_id, "
          "CAST(ROUND(UNIX_TIMESTAMP(h.refresh_time) * 1000) AS SIGNED) AS start_time_ms, "
          "CAST(ROUND(UNIX_TIMESTAMP(h.refresh_endtime) * 1000) AS SIGNED) AS end_time_ms, "
          "h.refresh_status AS status, "
          "h.refresh_rows AS row_count, "
          "h.refresh_read_tso AS read_tso, "
          "h.refresh_failed_reason AS failed_reason "
          "FROM information_schema.tables t "
          "JOIN " mview-refresh-hist-table " h ON h.mview_id = t.tidb_table_id "
          "WHERE t.table_schema = DATABASE() AND t.table_name IN")
     object-names)))

(defn- query-purge-history-rows
  [conn object-names]
  (when (seq object-names)
    (query-system-history-rows
     conn
     (str "SELECT t.table_name AS object_name, "
          "t.tidb_table_id AS object_id, "
          "h.purge_job_id AS job_id, "
          "CAST(ROUND(UNIX_TIMESTAMP(h.purge_time) * 1000) AS SIGNED) AS start_time_ms, "
          "CAST(ROUND(UNIX_TIMESTAMP(h.purge_endtime) * 1000) AS SIGNED) AS end_time_ms, "
          "h.purge_status AS status, "
          "h.purge_rows AS row_count, "
          "h.purge_failed_reason AS failed_reason "
          "FROM information_schema.tables t "
          "JOIN " mlog-purge-hist-table " h ON h.mlog_id = t.tidb_table_id "
          "WHERE t.table_schema = DATABASE() AND t.table_name IN")
     object-names)))

(defn- limit-history-rows
  [rows limit]
  (let [seen (volatile! {})]
    (->> rows
         (keep (fn [row]
                 (let [object-name (some-> (:object_name row) str)
                       object-count (get @seen object-name 0)]
                   (when (< object-count limit)
                     (vswap! seen assoc object-name (inc object-count))
                     row))))
         vec)))

(defn- history-row-base
  [component kind object-name system-table row]
  (let [status        (some-> row :status str)
        failed-reason (some-> row :failed_reason str)
        start-time-ms (some-> row :start_time_ms ->long)
        end-time-ms   (some-> row :end_time_ms ->long)]
    (cond-> {:component      component
             :kind           kind
             :object         object-name
             :system-table   system-table
             :object-id      (some-> row :object_id ->long)
             :job-id         (some-> row :job_id ->long)
             :status         status
             :start-time-ms  start-time-ms
             :end-time-ms    end-time-ms
             :duration-ms    (when (and start-time-ms end-time-ms)
                               (- end-time-ms start-time-ms))
             :row-count      (some-> row :row_count ->long)
             :success?       (when status
                               (= "SUCCESS" (str/upper-case status)))}
      (not (str/blank? failed-reason))
      (assoc :failed-reason failed-reason))))

(defn- normalize-refresh-history-row
  [component object-name row]
  (assoc (history-row-base component
                           :refresh
                           object-name
                           mview-refresh-hist-table
                           row)
         :read-tso (some-> row :read_tso ->long)))

(defn- normalize-purge-history-row
  [object-name row]
  (history-row-base :log-purge
                    :purge
                    object-name
                    mlog-purge-hist-table
                    row))

(defn- read-recent-runtime-history
  [conn {:keys [log-table]}]
  (try
    (let [refresh-history (limit-history-rows
                           (query-refresh-history-rows conn [row-view agg-view])
                           recent-runtime-history-limit)
          purge-history   (limit-history-rows
                           (when log-table
                             (query-purge-history-rows conn [log-table]))
                           recent-runtime-history-limit)
          rows            (vec
                           (concat
                            (map #(normalize-refresh-history-row :row-refresh row-view %) (filter (comp #{row-view} :object_name) refresh-history))
                            (map #(normalize-refresh-history-row :agg-refresh agg-view %) (filter (comp #{agg-view} :object_name) refresh-history))
                            (map #(normalize-purge-history-row log-table %) purge-history)))]
      {:available? true
       :tables     (cond-> [mview-refresh-hist-table]
                     log-table (conj mlog-purge-hist-table))
       :rows       rows
       :limit      recent-runtime-history-limit})
    (catch java.sql.SQLException e
      {:available? false
       :tables     (cond-> [mview-refresh-hist-table]
                     log-table (conj mlog-purge-hist-table))
       :error      (.getMessage e)})))

(defn- read-system-runtime-metadata
  [conn schedule-meta]
  (try
    (let [log-table         (:log-table schedule-meta)
          refresh-rows      (query-refresh-runtime-rows conn [row-view agg-view])
          refresh-by-object (index-rows-by-object-name refresh-rows)
          purge-by-object   (when log-table
                              (-> (query-purge-runtime-rows conn [log-table])
                                  index-rows-by-object-name))
          rows              (cond-> [(normalize-refresh-runtime-row :row-refresh
                                                                    row-view
                                                                    (get refresh-by-object row-view))
                                     (normalize-refresh-runtime-row :agg-refresh
                                                                    agg-view
                                                                    (get refresh-by-object agg-view))]
                              log-table
                              (conj (normalize-purge-runtime-row log-table
                                                                 (get purge-by-object log-table))))
          history-meta      (read-recent-runtime-history conn {:log-table log-table})
          timer-meta        (read-timer-metadata conn schedule-meta)
          missing-components (->> rows
                                  (remove :info-present?)
                                  (map :component)
                                  vec)]
      (cond-> {:available?          true
               :source              :system-tables
               :tables              (cond-> [mview-refresh-info-table]
                                      log-table (conj mlog-purge-info-table))
               :expected-components (mapv :component rows)
               :match-count         (count (filter :info-present? rows))
               :rows                rows
               :missing-components  missing-components
               :history-tables      (:tables history-meta)
               :recent-history-limit recent-runtime-history-limit}
        (:available? history-meta)
        (assoc :recent-history       (:rows history-meta)
               :recent-history-count (count (:rows history-meta)))

        (not (:available? history-meta))
        (assoc :history-error (:error history-meta))

        (:available? timer-meta)
        (assoc :timer-metadata timer-meta)

        (not (:available? timer-meta))
        (assoc :timer-error (:error timer-meta))))
    (catch java.sql.SQLException e
      {:available? false
       :source    :system-tables
       :tables    (cond-> [mview-refresh-info-table]
                    (:log-table schedule-meta) (conj mlog-purge-info-table))
       :error     (.getMessage e)})))

(defn read-runtime-metadata
  [conn schedule-meta]
  (let [system-metadata (read-system-runtime-metadata conn schedule-meta)]
    (if (:available? system-metadata)
      system-metadata
      (let [timer-metadata (read-timer-metadata conn schedule-meta)]
        (if (:available? timer-metadata)
          (assoc timer-metadata
                 :fallback-source :system-tables
                 :fallback-error  (:error system-metadata))
          {:available?    false
           :source        :unavailable
           :tables        (vec (concat (:tables system-metadata)
                                       [timer-table]))
           :error         (str/join " | "
                                    (remove str/blank?
                                            [(:error system-metadata)
                                             (:error timer-metadata)]))
           :probe-errors  {:system-tables (:error system-metadata)
                           :timers        (:error timer-metadata)}})))))

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
        row-stmt      (or (try
                            (create-row-view-with-schedule! conn refresh-start-delay row-refresh-seconds)
                            (catch java.sql.SQLException _
                              (execute-safely! conn (str "DROP MATERIALIZED VIEW " row-view))
                              nil))
                          (do
                            (create-row-view! conn)
                            (schedule-refresh! conn row-view refresh-start-delay row-refresh-seconds)))
        agg-stmt      (or (try
                            (create-agg-view-with-schedule! conn refresh-start-delay agg-refresh-seconds)
                            (catch java.sql.SQLException _
                              (execute-safely! conn (str "DROP MATERIALIZED VIEW " agg-view))
                              nil))
                          (do
                            (create-agg-view! conn)
                            (schedule-refresh! conn agg-view refresh-start-delay agg-refresh-seconds)))
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
