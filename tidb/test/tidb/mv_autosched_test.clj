(ns tidb.mv-autosched-test
  (:require [clojure.test :refer :all]
            [tidb.mv-autosched :as autosched]))

(def safe-point-timeout-message
  "PD server timeout: start timestamp may fall behind safe point")

(deftest retryable-snapshot-error-detects-safe-point-timeout
  (is (true? (autosched/retryable-snapshot-error?
              (java.sql.SQLException. safe-point-timeout-message))))
  (is (false? (autosched/retryable-snapshot-error?
               (RuntimeException. "non-retryable snapshot failure")))))

(deftest quiet-window-covers-refresh-retry-backoff
  (is (>= autosched/quiet-window-seconds
          (* 2 autosched/refresh-retry-backoff-ceiling-seconds))))

(deftest snapshot-with-reconnect-retries-safe-point-timeout
  (let [attempts      (atom 0)
        conn-holder   (atom nil)
        op            {:type :invoke :f :snapshot}
        schedule-meta {:log-table "mv_log"}
        safe-point-ex (java.sql.SQLException. safe-point-timeout-message)]
    (with-redefs [autosched/ensure-conn! (fn [holder _ _]
                                           (or @holder
                                               (reset! holder
                                                       (keyword (str "conn-" (inc @attempts))))))
                  autosched/close-conn-holder! (fn [holder]
                                                 (reset! holder nil))
                  autosched/run-with-op-timeout! (fn [_ _ f]
                                                   (let [attempt (swap! attempts inc)]
                                                     (if (= 1 attempt)
                                                       (throw safe-point-ex)
                                                       (f))))
                  autosched/snapshot-state (fn [node conn schedule]
                                             {:snapshot-node node
                                              :connection    conn
                                              :schedule      schedule})]
      (is (= {:type   :ok
              :f      :snapshot
              :result {:snapshot-node "node-3"
                       :connection    :conn-2
                       :schedule      schedule-meta}}
             (autosched/snapshot-with-reconnect!
              conn-holder "node-3" {} op schedule-meta)))
      (is (= 2 @attempts)))))
