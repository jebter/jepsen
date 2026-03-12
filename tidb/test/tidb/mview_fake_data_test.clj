(ns tidb.mview-fake-data-test
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [tidb.mv-autosched :as autosched]
            [tidb.mv-autosched-time :as autosched-time]
            [tidb.mv-lifecycle :as lifecycle]
            [tidb.mview :as mv]
            [tidb.mv-stateful :as stateful]))

(defn snapshot-op
  [snapshot-at-ms snapshot-node row-hash agg-hash log-row-count]
  {:type :ok
   :f :snapshot
   :process 0
   :time snapshot-at-ms
   :value {:snapshot-at-ms snapshot-at-ms
           :snapshot-node snapshot-node
           :row-equal? true
           :agg-equal? true
           :row-hash row-hash
           :agg-hash agg-hash
           :log-row-count log-row-count}})

(defn schedule-snapshot
  [snapshot-at-ms snapshot-node component entry]
  {:snapshot-at-ms snapshot-at-ms
   :snapshot-node snapshot-node
   :schedule-metadata {component entry}})

(deftest stored-row-matches-checks-full-row-shape
  (let [expected {:id 1 :g1 2 :v1 3 :version 4 :last-token "tok" :deleted false :pad "pad"}
        row      {:id 1 :g1 2 :v1 3 :version 4 :last_token "tok" :deleted 0 :pad "pad"}]
    (is (true? (stateful/stored-row-matches? row expected)))
    (is (true? (autosched/stored-row-matches? row expected)))
    (is (false? (stateful/stored-row-matches? (assoc row :g1 9) expected)))
    (is (false? (autosched/stored-row-matches? (assoc row :v1 9) expected)))
    (is (false? (stateful/stored-row-matches? (assoc row :pad "other") expected)))))

(deftest stable-pair-present-is-node-local
  (testing "cross-node adjacent equal snapshots do not count as stable"
    (is (false?
         (boolean
          (autosched/stable-pair-present?
           [(snapshot-op 0 "n1" 1 1 5)
            (snapshot-op 1 "n2" 1 1 4)])))))
  (testing "same-node equal snapshots still count as stable"
    (is (true?
         (boolean
          (autosched/stable-pair-present?
           [(snapshot-op 0 "n1" 1 1 5)
            (snapshot-op 1 "n2" 9 9 4)
            (snapshot-op 2 "n1" 1 1 5)]))))))

(deftest purge-progress-is-node-local
  (testing "cross-node one-off counts do not look like local purge progress"
    (is (= false
           (autosched/purge-progress
            [(snapshot-op 0 "n1" 1 1 5)
             (snapshot-op 1 "n2" 1 1 1)]))))
  (testing "same-node row-count decrease is still detected"
    (is (= :decreased
           (autosched/purge-progress
            [(snapshot-op 0 "n1" 1 1 5)
             (snapshot-op 1 "n2" 1 1 8)
             (snapshot-op 2 "n1" 1 1 3)])))))

(deftest schedule-metadata-disappearance-is-node-local
  (testing "cross-node availability mismatch is not treated as disappearance"
    (let [summary (autosched-time/schedule-metadata-summary
                   [(schedule-snapshot 0 "n1" :row-refresh
                                       {:available? true
                                        :schedule-visible? true
                                        :next-literal 5
                                        :next-matches-expected? true
                                        :query "SHOW CREATE MATERIALIZED VIEW mv_stateful_row"
                                        :object "mv_stateful_row"})
                    (schedule-snapshot 1 "n2" :row-refresh
                                       {:available? false
                                        :error "unavailable"
                                        :query "SHOW CREATE MATERIALIZED VIEW mv_stateful_row"
                                        :object "mv_stateful_row"})])]
      (is (nil? (get-in summary [:row-refresh :first-disappearance])))
      (is (false? (boolean (get-in summary [:row-refresh :fully-unavailable?]))))))
  (testing "same-node availability loss is still recorded as disappearance"
    (let [summary (autosched-time/schedule-metadata-summary
                   [(schedule-snapshot 0 "n1" :row-refresh
                                       {:available? true
                                        :schedule-visible? true
                                        :next-literal 5
                                        :next-matches-expected? true
                                        :query "SHOW CREATE MATERIALIZED VIEW mv_stateful_row"
                                        :object "mv_stateful_row"})
                    (schedule-snapshot 1 "n1" :row-refresh
                                       {:available? false
                                        :error "unavailable"
                                        :query "SHOW CREATE MATERIALIZED VIEW mv_stateful_row"
                                        :object "mv_stateful_row"})])]
      (is (= "n1" (get-in summary [:row-refresh :first-disappearance :snapshot-node]))))))

(deftest lifecycle-artifact-state-diff-only-checks-expected-keys
  (is (= {:row-view-present? {:expected false :actual true}}
         (lifecycle/artifact-state-diff
          {:base-table-present? true
           :row-view-present? true
           :agg-view-present? false
           :mlog-present? true}
          {:base-table-present? true
           :row-view-present? false})))
  (is (= {}
         (lifecycle/artifact-state-diff
          {:base-table-present? true
           :row-view-present? false
           :agg-view-present? false
           :mlog-present? true}
          {:base-table-present? true
           :row-view-present? false}))))

(deftest lifecycle-checker-counts-the-full-management-sequence
  (let [history (mapv #(assoc % :type :ok :value {:phase (:lifecycle-phase %)})
                      lifecycle/management-ops)
        summary (checker/check (lifecycle/checker*) {} history nil)]
    (is (true? (:valid? summary)))
    (is (true? (:lifecycle-complete? summary)))
    (is (= (count lifecycle/management-ops) (:expected-lifecycle-op-count summary)))
    (is (= (count lifecycle/management-ops) (:lifecycle-op-count summary)))
    (is (= (count lifecycle/management-ops) (:lifecycle-ok-count summary)))))

(deftest lifecycle-transition-recovers-when-artifact-state-already-matches
  (let [op     {:type :invoke
                :f :drop-row-view
                :expected-state {:base-table-present? true
                                 :row-view-present? false}}
        actual {:base-table-present? true
                :row-view-present? false
                :agg-view-present? true
                :mlog-present? true}]
    (with-redefs [mv/artifact-state (fn [_] actual)]
      (let [result (#'tidb.mv-lifecycle/lifecycle-transition!
                    ::conn
                    op
                    #(throw (ex-info "connection reset" {})))]
        (is (= :ok (:type result)))
        (is (true? (:resolved? result)))
        (is (= "connection reset" (:exception result)))
        (is (= (:expected-state op) (get-in result [:value :expected-state])))
        (is (= actual (get-in result [:value :artifact-state])))))))

(deftest lifecycle-transition-still-fails-when-state-does-not-match
  (let [op     {:type :invoke
                :f :drop-row-view
                :expected-state {:base-table-present? true
                                 :row-view-present? false}}
        actual {:base-table-present? true
                :row-view-present? true
                :agg-view-present? true
                :mlog-present? true}]
    (with-redefs [mv/artifact-state (fn [_] actual)]
      (let [result (#'tidb.mv-lifecycle/lifecycle-transition!
                    ::conn
                    op
                    #(throw (ex-info "connection reset" {})))]
        (is (= :fail (:type result)))
        (is (= :drop-row-view-error (:error result)))
        (is (= {:row-view-present? {:expected false :actual true}}
               (get-in result [:value :diff])))))))
