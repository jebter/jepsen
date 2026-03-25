(ns tidb.mv-lifecycle-checker-test
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [tidb.mv-lifecycle :as lifecycle]))

(deftest lifecycle-checker-counts-the-full-management-sequence
  (let [history (mapv #(assoc % :type :ok :value {:phase (:lifecycle-phase %)})
                      lifecycle/management-ops)
        summary (checker/check (lifecycle/checker*) {} history nil)]
    (is (true? (:valid? summary)))
    (is (true? (:strict-valid? summary)))
    (is (true? (:write-resolution-valid? summary)))
    (is (false? (:recovered-write-ambiguity? summary)))
    (is (true? (:lifecycle-complete? summary)))
    (is (= (count lifecycle/management-ops) (:expected-lifecycle-op-count summary)))
    (is (= (count lifecycle/management-ops) (:lifecycle-op-count summary)))
    (is (= (count lifecycle/management-ops) (:lifecycle-ok-count summary)))))

(deftest lifecycle-checker-separates-write-ambiguity-from-lifecycle-convergence
  (let [unresolved-write {:type :info
                          :f :move-group
                          :process 2
                          :time 42
                          :value {:id 3
                                  :g1 1
                                  :v1 300042
                                  :version 42
                                  :last-token "mv-lifecycle-test-42"
                                  :deleted false
                                  :pad "mv-lifecycle-test-42........."}
                          :error :indeterminate-write
                          :exception "Query timed out"}
        history          (into [unresolved-write]
                               (mapv #(assoc % :type :ok :value {:phase (:lifecycle-phase %)})
                                     lifecycle/management-ops))
        summary          (checker/check (lifecycle/checker*) {} history nil)]
    (is (true? (:valid? summary)))
    (is (false? (:strict-valid? summary)))
    (is (false? (:write-resolution-valid? summary)))
    (is (true? (:recovered-write-ambiguity? summary)))
    (is (true? (:lifecycle-complete? summary)))
    (is (= 1 (:unresolved-write-count summary)))
    (is (= unresolved-write (:first-unresolved-write summary)))
    (is (nil? (:first-failure summary)))))
