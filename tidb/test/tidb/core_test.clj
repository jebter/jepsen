(ns tidb.core-test
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [tidb.core :as core]
            [tidb.db :as db]
            [tidb.nemesis :as nemesis]))

(deftest maybe-gnuplot-checker-skips-when-unavailable
  (let [inner (reify checker/Checker
                (check [_ _ _ _]
                  (throw (ex-info "should not execute" {}))))]
    (with-redefs [core/gnuplot-available? (delay false)]
      (is (= {:valid? true
              :skipped? true
              :reason :gnuplot-unavailable}
             (checker/check (core/maybe-gnuplot-checker :perf inner)
                            nil
                            []
                            {}))))))

(deftest maybe-gnuplot-checker-delegates-when-available
  (let [called? (atom false)
        inner   (reify checker/Checker
                  (check [_ _ _ _]
                    (reset! called? true)
                    {:valid? true
                     :delegated? true}))]
    (with-redefs [core/gnuplot-available? (delay true)]
      (is (= {:valid? true
              :delegated? true}
             (checker/check (core/maybe-gnuplot-checker :clock-skew inner)
                            nil
                            []
                            {})))
      (is @called?))))

(deftest test-name-includes-run-tag-when-present
  (with-redefs [core/workloads {:mv-stateful (fn [_]
                                               {:generator nil
                                                :client :fake-client
                                                :checker nil})}
                nemesis/nemesis (fn [_]
                                  {:nemesis :fake-nemesis
                                   :generator nil})
                core/test-net (fn [_] :fake-net)
                core/requires-real-network? (fn [_] false)
                db/db (fn [] :fake-db)]
    (is (= "TiDB nightly mv-stateful auto-retry auto-retry-limit :default txn-mode optimistic isolation :repeatable-read run-tag branch-validation-123 nemesis kill-db"
           (:name (core/test {:version "nightly"
                              :workload :mv-stateful
                              :nemesis {:interval 10 :kill-db true}
                              :time-limit 300
                              :auto-retry :default
                              :auto-retry-limit :default
                              :txn-mode "optimistic"
                              :isolation :repeatable-read
                              :run-tag "branch-validation-123"}))))))
