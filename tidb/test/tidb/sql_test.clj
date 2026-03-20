(ns tidb.sql-test
  (:require [clojure.java.jdbc :as j]
            [clojure.test :refer :all]
            [tidb.sql :as sql]))

(deftest init-sql-skips-tidb-txn-mode-for-v3-beta1
  (let [stmts (sql/init-sql {:version "v3.0.0-beta.1"
                             :txn-mode "optimistic"})]
    (is (not-any? #(re-find #"tidb_txn_mode" %) stmts))
    (is (some #{"set @@tidb_general_log = 1"} stmts))))

(deftest init-sql-keeps-tidb-txn-mode-for-newer-versions
  (let [stmts (sql/init-sql {:version "v3.0.0"
                             :txn-mode "optimistic"})]
    (is (some #{"set @@tidb_txn_mode = 'optimistic'"} stmts))
    (is (some #{"set @@tidb_general_log = 1"} stmts))))

(deftest open-preserves-version-sensitive-init-options-for-reopen
  (let [test-opts {:auto-retry :default
                   :auto-retry-limit :default
                   :version "v3.0.0-beta.1"
                   :txn-mode "optimistic"
                   :follower-read true
                   :init-sql ["set names utf8mb4"]}
        conn      (with-redefs [j/get-connection (fn [_] :fake-conn)
                                j/add-connection (fn [spec conn]
                                                   (assoc spec :connection conn))
                                sql/init-conn!   (fn [conn _] conn)]
                    (sql/open :node-1 test-opts 1000))]
    (is (= :node-1 (::sql/node conn)))
    (is (= test-opts (::sql/test conn)))))

(deftest await-node-passes-test-options-through-open
  (let [test-opts {:version "v3.0.0-beta.1"
                   :txn-mode "optimistic"}
        open-args (atom nil)]
    (with-redefs [sql/open   (fn [node test timeout]
                               (reset! open-args [node test timeout])
                               {:connection :fake})
                  j/execute! (fn [& _] nil)
                  sql/close! (fn [_] nil)]
      (sql/await-node test-opts :node-1)
      (is (= [:node-1 test-opts 30000] @open-args)))))

(deftest with-conn-failure-retry-retries-try-again-later
  (let [attempts (atom 0)
        conn     :fake-conn]
    (with-redefs [sql/reopen! identity
                  rand-int    (constantly 0)]
      (is (= :ok
             (sql/with-conn-failure-retry conn
               (if (= 1 (swap! attempts inc))
                 (throw (java.sql.SQLException.
                         "(conn=1) Write conflict, reason=Optimistic [try again later]"))
                 :ok))))
      (is (= 2 @attempts)))))
