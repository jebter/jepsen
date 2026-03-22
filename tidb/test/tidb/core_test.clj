(ns tidb.core-test
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [tidb.core :as core]))

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
