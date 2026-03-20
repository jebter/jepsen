(ns tidb.mview-test
  (:require [clojure.test :refer :all]
            [tidb.mview :as mv]
            [tidb.sql :as sql]))

(deftest split-base-table-ignores-unsupported-syntax
  (let [calls (atom [])]
    (with-redefs [sql/execute! (fn [_ [stmt]]
                                 (swap! calls conj stmt)
                                 (throw (java.sql.SQLSyntaxErrorException.
                                         "split table unsupported")))]
      (is (false? (mv/split-base-table! :conn [1 2 4])))
      (is (= ["split table mv_stateful_base by (2),(4)"] @calls)))))

(deftest split-base-table-skips-blank-split-list
  (let [calls (atom 0)]
    (with-redefs [sql/execute! (fn [& _]
                                 (swap! calls inc)
                                 nil)]
      (is (nil? (mv/split-base-table! :conn [1 3 5])))
      (is (zero? @calls)))))
