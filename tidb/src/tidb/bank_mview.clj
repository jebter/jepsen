(ns tidb.bank-mview
  (:refer-clojure :exclude [test])
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]]
            [jepsen.tests.bank :as bank]
            [knossos.op :as op]
            [tidb.sql :as c :refer :all]
            [tidb.util :as util]
            [clojure.tools.logging :refer [info]]))

(def refresh-start-delay-seconds 5)
(def refresh-next-seconds 1)

(def check-timeout-ms 60000)
(def check-sleep-ms 200)

(defn transfer-value [from to b1 b2 amount]
  {:from   [from (+ b1 amount) b1]
   :to     [to (- b2 amount) b2]
   :amount amount})

(defn execute-safely!
  ([conn stmt]
   (execute-safely! conn stmt nil))
  ([conn stmt err-msg]
   (try
     (c/execute! conn [stmt])
     (catch java.sql.SQLException e
       (when err-msg
         (info err-msg (.getMessage e)))))))

(defn create-mlog!
  [conn]
  ; Default env has no TiFlash; don't enable purge schedule by default.
  ; If the implementation requires a PURGE clause, fall back to PURGE IMMEDIATE.
  (try
    (c/execute! conn ["CREATE MATERIALIZED VIEW LOG ON accounts (id, balance)"])
    (catch java.sql.SQLException e
      (info "CREATE MLOG without PURGE failed; retry with PURGE IMMEDIATE:" (.getMessage e))
      (c/execute! conn ["CREATE MATERIALIZED VIEW LOG ON accounts (id, balance) PURGE IMMEDIATE"]))))

(defn create-mview!
  [conn]
  (c/execute! conn [(str
                     "CREATE MATERIALIZED VIEW mv_accounts (id, balance, cnt) "
                     "COMMENT = 'jepsen:bank-mview(minimal)' "
                     "REFRESH FAST "
                     "AS "
                     "SELECT "
                     "  id, "
                     "  MAX(balance) AS balance, "
                     "  COUNT(*)     AS cnt "
                     "FROM accounts "
                     "GROUP BY id")]))

(defn schedule-refresh!
  [conn]
  (c/execute! conn [(str "ALTER MATERIALIZED VIEW mv_accounts "
                         "REFRESH START WITH (NOW() + INTERVAL "
                         refresh-start-delay-seconds
                         " SECOND) NEXT "
                         refresh-next-seconds)]))

(defn query-base-accounts
  [conn]
  (->> (c/query conn ["SELECT id, balance FROM accounts ORDER BY id"])
       (map (juxt :id (comp long :balance)))
       (into (sorted-map))))

(defn query-mv-accounts
  [conn]
  (->> (c/query conn ["SELECT id, balance, cnt FROM mv_accounts ORDER BY id"])
       (map (fn [row]
              [(:id row)
               {:balance (long (:balance row))
                :cnt     (long (:cnt row))}]))
       (into (sorted-map))))

(defn diff-accounts
  [base mv]
  (let [base-ks (set (keys base))
        mv-ks   (set (keys mv))
        missing-in-mv   (sort (seq (set/difference base-ks mv-ks)))
        missing-in-base (sort (seq (set/difference mv-ks base-ks)))
        mismatched (->> (set/intersection base-ks mv-ks)
                        (sort)
                        (keep (fn [k]
                                (let [b (get base k)
                                      m (get-in mv [k :balance])]
                                  (when (not= b m)
                                    [k {:base b :mv m}]))))
                        (into {}))
        bad-cnt (->> mv
                     (keep (fn [[k v]]
                             (when (not= 1 (:cnt v))
                               [k (:cnt v)])))
                     (into {}))]
    (cond-> {}
      (seq missing-in-mv)   (assoc :missing-in-mv missing-in-mv)
      (seq missing-in-base) (assoc :missing-in-base missing-in-base)
      (seq mismatched)      (assoc :mismatched mismatched)
      (seq bad-cnt)         (assoc :bad-cnt bad-cnt))))

(defn await-mv-equals-base!
  [conn]
  (let [deadline (+ (System/currentTimeMillis) check-timeout-ms)]
    (loop [attempt 0
           last-diff nil]
      (let [base (query-base-accounts conn)
            mv   (query-mv-accounts conn)
            diff (diff-accounts base mv)]
        (if (empty? diff)
          {:ok? true
           :attempt attempt}
          (if (< (System/currentTimeMillis) deadline)
            (do (Thread/sleep check-sleep-ms)
                (recur (inc attempt) diff))
            {:ok? false
             :attempt attempt
             :diff (or diff last-diff)
             :base base
             :mv (into (sorted-map) (map (fn [[k v]] [k (:balance v)]) mv))
             :mv-cnt (into (sorted-map) (map (fn [[k v]] [k (:cnt v)]) mv))}))))))

(defn mview-checker
  []
  (reify checker/Checker
    (check [_ _ history _]
      (let [checks (->> history
                        (filter #(= :check (:f %)))
                        (filter #(#{:ok :fail} (:type %))))
            oks   (filter op/ok? checks)
            fails (filter #(= :fail (:type %)) checks)]
        {:valid? (and (seq checks) (empty? fails))
         :check-count (count checks)
         :ok-count (count oks)
         :fail-count (count fails)
         :first-failure (first fails)}))))

(defrecord BankMViewClient [conn tbl-created?]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (when (compare-and-set! tbl-created? false true)
      (c/with-conn-failure-retry conn
        ; Cleanup: drop MV -> drop MLog -> drop base table.
        (c/execute! conn ["DROP MATERIALIZED VIEW IF EXISTS mv_records"])
        (execute-safely! conn "DROP MATERIALIZED VIEW LOG ON records")
        (execute-safely! conn "DROP TABLE IF EXISTS records")
        (c/execute! conn ["DROP MATERIALIZED VIEW IF EXISTS mv_accounts"])
        (execute-safely! conn "DROP MATERIALIZED VIEW LOG ON accounts" "Ignore drop mlog error:")
        (c/execute! conn ["DROP TABLE IF EXISTS accounts"])

        ; Base table.
        (c/execute! conn ["CREATE TABLE IF NOT EXISTS accounts
                           (id     int not null primary key,
                            balance bigint not null)"])
        (c/execute! conn [(str "split table accounts by "
                               (->> (:accounts test)
                                    (filter even?)
                                    (map #(str "(" % ")"))
                                    (str/join ",")))])
        (when (:table-cache test)
          (c/execute! conn ["ALTER TABLE accounts CACHE"]))

        ; Initial data.
        (doseq [a (:accounts test)]
          (try
            (with-txn-retries conn
              (c/insert! conn :accounts {:id      a
                                         :balance (if (= a (first (:accounts test)))
                                                    (:total-amount test)
                                                    0)}))
            (catch java.sql.SQLIntegrityConstraintViolationException _ nil)))

        ; MV chain.
        (create-mlog! conn)
        (c/execute! conn ["DROP MATERIALIZED VIEW IF EXISTS mv_accounts"])
        (create-mview! conn)
        (schedule-refresh! conn))))

  (invoke! [this test op]
    (cond
      (= :check (:f op))
      (try
        (let [res (await-mv-equals-base! conn)]
          (if (:ok? res)
            (assoc op :type :ok :value (dissoc res :ok?))
            (assoc op :type :fail :value (dissoc res :ok?))))
        (catch Throwable t
          (assoc op :type :fail :error (.getMessage t))))

      (and (= :transfer (:f op)) (:single-stmt-write test))
      (with-error-handling op
        (let [{:keys [from to amount]} (:value op)]
          (c/execute! conn
                      ["update accounts set balance = balance + if(id=?,-?,?) where id=? or (id=? and 1/if(balance>=?,1,0))"
                       from amount amount to from amount]
                      {:transaction? false})
          (attach-txn-info conn (assoc op :type :ok))))

      true
      (with-txn op [c conn {:isolation (util/isolation-level test)
                            :before-hook (partial c/rand-init-txn! test conn)}]
        (case (:f op)
          :read
          (let [accounts (->> (c/query c ["SELECT id, balance FROM mv_accounts"])
                              (map (fn [row] [(:id row) (long (:balance row))]))
                              (into (sorted-map)))]
            (assoc op :type :ok, :value accounts))

          :transfer
          (let [{:keys [from to amount]} (:value op)
                b1 (-> c
                       (c/query [(str "select balance from accounts where id = ? "
                                      (:read-lock test)) from]
                                {:row-fn :balance})
                       first
                       (- amount))
                b2 (-> c
                       (c/query [(str "select balance from accounts where id = ? "
                                      (:read-lock test))
                                 to]
                                {:row-fn :balance})
                       first
                       (+ amount))]
            (cond (neg? b1)
                  (assoc op :type :fail, :value [:negative from b1])
                  (neg? b2)
                  (assoc op :type :fail, :value [:negative to b2])
                  true
                  (if (:update-in-place test)
                    (do (c/execute! c ["update accounts set balance = balance - ? where id = ?" amount from])
                        (c/execute! c ["update accounts set balance = balance + ? where id = ?" amount to])
                        (assoc op :type :ok :value (transfer-value from to b1 b2 amount)))
                    (do (c/update! c :accounts {:balance b1} ["id = ?" from])
                        (c/update! c :accounts {:balance b2} ["id = ?" to])
                        (assoc op :type :ok :value (transfer-value from to b1 b2 amount))))))))))

  (teardown! [_ _])

  (close! [_ _]
    (c/close! conn)))

(defn workload
  [opts]
  (let [t (bank/test)]
    (assoc t
           :client (BankMViewClient. nil (atom false))
           :checker (checker/compose {:bank (:checker t)
                                     :mview (mview-checker)})
           :final-generator (gen/once {:type :invoke :f :check}))))
