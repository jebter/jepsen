(ns tidb.mview-fake-data-test
  (:require [clojure.test :refer :all]
            [jepsen.client :as client]
            [jepsen.checker :as checker]
            [jepsen.control :as control]
            [jepsen.generator :as gen]
            [tidb.mv-autosched :as autosched]
            [tidb.mv-autosched-time :as autosched-time]
            [tidb.artifact :as artifact]
            [tidb.db :as db]
            [tidb.mv-lifecycle :as lifecycle]
            [tidb.nemesis :as nemesis]
            [tidb.mview :as mv]
            [tidb.mv-stateful :as stateful]
            [tidb.sql :as c]))

(defn snapshot-op
  ([snapshot-at-ms snapshot-node row-hash agg-hash log-row-count]
   (snapshot-op snapshot-at-ms snapshot-node true true row-hash agg-hash log-row-count))
  ([snapshot-at-ms snapshot-node row-equal? agg-equal? row-hash agg-hash log-row-count]
   {:type :ok
    :f :snapshot
    :process 0
    :time snapshot-at-ms
    :value {:snapshot-at-ms snapshot-at-ms
            :snapshot-node snapshot-node
            :row-equal? row-equal?
            :agg-equal? agg-equal?
            :row-hash row-hash
            :agg-hash agg-hash
            :log-row-count log-row-count}}))

(defn time-snapshot-op
  [snapshot-at-ms snapshot-node row-equal? agg-equal? row-hash agg-hash log-row-count]
  {:type :ok
   :f :snapshot
   :process 0
   :time snapshot-at-ms
   :result {:snapshot-at-ms snapshot-at-ms
            :snapshot-node snapshot-node
            :row-equal? row-equal?
            :agg-equal? agg-equal?
            :row-hash row-hash
            :agg-hash agg-hash
            :log-row-count log-row-count}})

(defn schedule-snapshot
  [snapshot-at-ms snapshot-node component entry]
  {:snapshot-at-ms snapshot-at-ms
   :snapshot-node snapshot-node
   :schedule-metadata {component entry}})

(defn timer-snapshot
  [snapshot-at-ms snapshot-node entry]
  {:snapshot-at-ms snapshot-at-ms
   :snapshot-node snapshot-node
   :timer-metadata entry})

(defn runtime-snapshot
  [snapshot-at-ms snapshot-node entry]
  {:snapshot-at-ms snapshot-at-ms
   :snapshot-node snapshot-node
   :runtime-metadata entry})

(defn lifecycle-write-events
  [invoke-index complete-index process f value]
  [{:type :invoke
    :f f
    :process process
    :index invoke-index
    :value value}
   {:type :ok
    :f f
    :process process
    :index complete-index
    :value value}])

(defn lifecycle-refresh-events
  [invoke-index complete-index f phase actual]
  (let [count-key (if (= :refresh-row f) :rows :groups)]
    [{:type :invoke
      :f f
      :process 0
      :index invoke-index
      :lifecycle-phase phase}
     {:type :ok
      :f f
      :process 0
      :index complete-index
      :lifecycle-phase phase
      :result {count-key (count actual)
               :actual actual}}]))

(defn validate-lifecycle-refresh-history
  [history]
  (let [pairs        (#'tidb.mv-lifecycle/pair-history history)
        write-pairs  (filter #'tidb.mv-lifecycle/write-pair? pairs)
        refresh-pair (first (filter #'tidb.mv-lifecycle/lifecycle-refresh-pair? pairs))]
    (#'tidb.mv-lifecycle/validate-refresh-op write-pairs refresh-pair)))

(defn stateful-refresh-events
  [invoke-index complete-index f value complete-type result]
  [{:type :invoke
    :f f
    :process 0
    :phase :active
    :index invoke-index
    :value value}
   {:type complete-type
    :f f
    :process 0
    :phase :active
    :index complete-index
    :value value
    :result result}])

(defn validate-stateful-refresh-history
  [history]
  (let [pairs        (#'tidb.mv-stateful/pair-history history)
        write-pairs  (filter #'tidb.mv-stateful/write-pair? pairs)
        refresh-pair (first (filter #'tidb.mv-stateful/refresh-pair? pairs))]
    (#'tidb.mv-stateful/validate-refresh-op write-pairs refresh-pair)))

(deftest stored-row-matches-checks-full-row-shape
  (let [expected {:id 1 :g1 2 :v1 3 :version 4 :last-token "tok" :deleted false :pad "pad"}
        row      {:id 1 :g1 2 :v1 3 :version 4 :last_token "tok" :deleted 0 :pad "pad"}]
    (is (true? (stateful/stored-row-matches? row expected)))
    (is (true? (autosched/stored-row-matches? row expected)))
    (is (false? (stateful/stored-row-matches? (assoc row :g1 9) expected)))
    (is (false? (autosched/stored-row-matches? (assoc row :v1 9) expected)))
    (is (false? (stateful/stored-row-matches? (assoc row :pad "other") expected)))))

(deftest stateful-write-retries-on-a-reopened-connection
  (let [old-conn      {:name :old}
        new-conn      {:name :new}
        conn-holder   (atom old-conn)
        execute-calls (atom [])
        closed-calls  (atom [])
        token         "mv-stateful-test-token"
        op            {:type :invoke
                       :f :move-group
                       :value {:id 1
                               :g1 2
                               :v1 3
                               :version 4
                               :last-token token
                               :deleted false
                               :pad (stateful/pad-for token)}}]
    (with-redefs [c/execute! (fn [conn _ _]
                               (swap! execute-calls conj (:name conn))
                               (when (= conn old-conn)
                                 (throw (java.sql.SQLNonTransientConnectionException.
                                         "execute() is called on closed connection"))))
                  c/open     (fn [_ _] new-conn)
                  c/close!   (fn [conn]
                               (swap! closed-calls conj (:name conn))
                               nil)]
      (let [result (stateful/apply-write-with-reconnect! conn-holder :node {} op)]
        (is (= :ok (:type result)))
        (is (= [:old :new] @execute-calls))
        (is (= [:old] @closed-calls))
        (is (= new-conn @conn-holder))))))

(deftest stateful-write-retries-when-open-times-out
  (let [new-conn    {:name :new}
        conn-holder (atom nil)
        open-calls  (atom 0)
        token       "mv-stateful-open-timeout-token"
        op          {:type :invoke
                     :f :insert
                     :value {:id 1
                             :g1 2
                             :v1 3
                             :version 4
                             :last-token token
                             :deleted false
                             :pad (stateful/pad-for token)}}]
    (with-redefs [c/open     (fn [_ _]
                               (if (= 1 (swap! open-calls inc))
                                 (throw (ex-info "throw+: {:type :connect-timed-out, :node :n1}"
                                                 {:type :connect-timed-out
                                                  :node :n1}))
                                 new-conn))
                  c/execute! (fn [conn _ _]
                               (is (= new-conn conn))
                               nil)
                  c/close!   (fn [_] nil)]
      (let [result (stateful/apply-write-with-reconnect! conn-holder :node {} op)]
        (is (= :ok (:type result)))
        (is (= 2 @open-calls))
        (is (= new-conn @conn-holder))))))

(deftest stateful-client-open-retries-when-open-times-out
  (let [old-conn     {:name :old}
        new-conn     {:name :new}
        conn-holder  (atom old-conn)
        open-calls   (atom 0)
        closed-calls (atom [])
        mv-client    (stateful/->MVStatefulClient conn-holder nil (atom false))]
    (with-redefs [c/open   (fn [_ _]
                             (if (= 1 (swap! open-calls inc))
                               (throw (ex-info "throw+: {:type :connect-timed-out, :node :n1}"
                                               {:type :connect-timed-out
                                                :node :n1}))
                               new-conn))
                  c/close! (fn [conn]
                             (swap! closed-calls conj (:name conn))
                             nil)]
      (let [opened (client/open! mv-client {} :n1)]
        (is (= :n1 (:node opened)))
        (is (= 2 @open-calls))
        (is (= [:old] @closed-calls))
        (is (not (identical? conn-holder (:conn-holder opened))))
        (is (nil? @conn-holder))
        (is (= new-conn @(-> opened :conn-holder)))))))

(deftest stateful-write-retries-on-driver-batch-npe
  (let [old-conn      {:name :old}
        new-conn      {:name :new}
        conn-holder   (atom old-conn)
        execute-calls (atom [])
        closed-calls  (atom [])
        token         "mv-stateful-driver-batch-npe-token"
        op            {:type :invoke
                       :f :update-value
                       :value {:id 1
                               :g1 2
                               :v1 3
                               :version 4
                               :last-token token
                               :deleted false
                               :pad (stateful/pad-for token)}}]
    (with-redefs [c/execute! (fn [conn _ _]
                               (swap! execute-calls conj (:name conn))
                               (when (= conn old-conn)
                                 (throw
                                  (doto (NullPointerException.)
                                    (.setStackTrace
                                     (into-array
                                      StackTraceElement
                                      [(StackTraceElement.
                                        "org.mariadb.jdbc.ClientSidePreparedStatement"
                                        "executeBatch"
                                        "ClientSidePreparedStatement.java"
                                        298)]))))))
                  c/open     (fn [_ _] new-conn)
                  c/close!   (fn [conn]
                               (swap! closed-calls conj (:name conn))
                               nil)]
      (let [result (stateful/apply-write-with-reconnect! conn-holder :node {} op)]
        (is (= :ok (:type result)))
        (is (= [:old :new] @execute-calls))
        (is (= [:old] @closed-calls))
        (is (= new-conn @conn-holder))))))

(deftest stateful-write-retries-on-generic-npe
  (let [old-conn      {:name :old}
        new-conn      {:name :new}
        conn-holder   (atom old-conn)
        execute-calls (atom [])
        closed-calls  (atom [])
        token         "mv-stateful-generic-npe-token"
        op            {:type :invoke
                       :f :update-value
                       :value {:id 1
                               :g1 2
                               :v1 3
                               :version 4
                               :last-token token
                               :deleted false
                               :pad (stateful/pad-for token)}}]
    (with-redefs [c/execute! (fn [conn _ _]
                               (swap! execute-calls conj (:name conn))
                               (when (= conn old-conn)
                                 (throw (NullPointerException.))))
                  c/open     (fn [_ _] new-conn)
                  c/close!   (fn [conn]
                               (swap! closed-calls conj (:name conn))
                               nil)]
      (let [result (stateful/apply-write-with-reconnect! conn-holder :node {} op)]
        (is (= :ok (:type result)))
        (is (= [:old :new] @execute-calls))
        (is (= [:old] @closed-calls))
        (is (= new-conn @conn-holder))))))

(deftest stateful-write-retries-when-op-times-out
  (let [old-conn      {:name :old}
        new-conn      {:name :new}
        conn-holder   (atom old-conn)
        execute-calls (atom [])
        aborted-calls (atom [])
        token         "mv-stateful-op-timeout-token"
        op            {:type :invoke
                       :f :update-value
                       :value {:id 1
                               :g1 2
                               :v1 3
                               :version 4
                               :last-token token
                               :deleted false
                               :pad (stateful/pad-for token)}}]
    (with-redefs [stateful/op-timeout-ms 20
                  c/execute!            (fn [conn _ _]
                                          (swap! execute-calls conj (:name conn))
                                          (when (= conn old-conn)
                                            (Thread/sleep 1000)))
                  c/open                (fn [_ _] new-conn)
                  c/abort!              (fn [conn]
                                          (swap! aborted-calls conj (:name conn))
                                          nil)
                  c/close!              (fn [_]
                                          (throw (ex-info "close! should not be called" {})))]
      (let [result (stateful/apply-write-with-reconnect! conn-holder :node {} op)]
        (is (= :ok (:type result)))
        (is (= [:old :new] @execute-calls))
        (is (= [:old] @aborted-calls))
        (is (= new-conn @conn-holder))))))

(deftest stateful-verify-write-falls-back-to-other-nodes
  (let [token       "mv-stateful-verify-fallback-token"
        op          {:type :invoke
                     :f :update-value
                     :value {:id 1
                             :g1 2
                             :v1 3
                             :version 4
                             :last-token token
                             :deleted false
                             :pad (stateful/pad-for token)}}
        open-calls  (atom [])
        close-calls (atom [])]
    (with-redefs [c/open             (fn [node _]
                                       (swap! open-calls conj node)
                                       (case node
                                         :n1 (throw (ex-info "throw+: {:type :connect-timed-out, :node :n1}"
                                                             {:type :connect-timed-out
                                                              :node :n1}))
                                         {:node node}))
                  mv/query-stored-row (fn [conn id]
                                        (is (= 1 id))
                                        (when (= :n2 (:node conn))
                                          {:id 1
                                           :g1 2
                                           :v1 3
                                           :version 4
                                           :last_token token
                                           :deleted 0
                                           :pad (stateful/pad-for token)}))
                  c/close!           (fn [conn]
                                       (swap! close-calls conj (:node conn))
                                       nil)]
      (let [result (stateful/verify-write! :n1 {:nodes [:n1 :n2 :n3]} op)]
        (is (= :ok (:type result)))
        (is (true? (:resolved? result)))
        (is (= {:resolution :read-back :node :n2} (:result result)))
        (is (= [:n1 :n2] @open-calls))
        (is (= [:n2] @close-calls))))))

(deftest stateful-ambiguous-write-resolves-via-fallback-node
  (let [token       "mv-stateful-ambiguous-fallback-token"
        writer-conn {:name :writer}
        open-calls  (atom [])
        close-calls (atom [])
        op          {:type :invoke
                     :f :move-group
                     :value {:id 1
                             :g1 2
                             :v1 3
                             :version 4
                             :last-token token
                             :deleted false
                             :pad (stateful/pad-for token)}}]
    (with-redefs [c/execute!         (fn [_ _ _]
                                       (throw (ex-info "throw+: {:type :connect-timed-out, :node :n1}"
                                                       {:type :connect-timed-out
                                                        :node :n1})))
                  c/open             (fn [node _]
                                       (swap! open-calls conj node)
                                       (case node
                                         :n1 (throw (ex-info "throw+: {:type :connect-timed-out, :node :n1}"
                                                             {:type :connect-timed-out
                                                              :node :n1}))
                                         {:node node}))
                  mv/query-stored-row (fn [conn id]
                                        (is (= 1 id))
                                        (when (= :n2 (:node conn))
                                          {:id 1
                                           :g1 2
                                           :v1 3
                                           :version 4
                                           :last_token token
                                           :deleted 0
                                           :pad (stateful/pad-for token)}))
                  c/close!           (fn [conn]
                                       (swap! close-calls conj (:node conn))
                                       nil)]
      (let [result (stateful/apply-write! :n1 {:nodes [:n1 :n2 :n3]} writer-conn op)]
        (is (= :ok (:type result)))
        (is (true? (:resolved? result)))
        (is (= {:resolution :read-back :node :n2} (:result result)))
        (is (= [:n1 :n2] @open-calls))
        (is (= [:n2] @close-calls))))))

(deftest stateful-ambiguous-write-replays-via-fallback-node
  (let [token         "mv-stateful-replay-fallback-token"
        writer-conn   {:name :writer}
        open-calls    (atom [])
        execute-calls (atom [])
        close-calls   (atom [])
        op            {:type :invoke
                       :f :move-group
                       :value {:id 1
                               :g1 2
                               :v1 3
                               :version 4
                               :last-token token
                               :deleted false
                               :pad (stateful/pad-for token)}}]
    (with-redefs [c/execute!         (fn [conn _ _]
                                       (swap! execute-calls conj (or (:name conn) (:node conn)))
                                       (when (= writer-conn conn)
                                         (throw (ex-info "throw+: {:type :connect-timed-out, :node :n1}"
                                                         {:type :connect-timed-out
                                                          :node :n1}))))
                  c/open             (fn [node _]
                                       (swap! open-calls conj node)
                                       (case node
                                         :n1 (throw (ex-info "throw+: {:type :connect-timed-out, :node :n1}"
                                                             {:type :connect-timed-out
                                                              :node :n1}))
                                         {:node node}))
                  mv/query-stored-row (fn [_ _] nil)
                  c/close!           (fn [conn]
                                       (swap! close-calls conj (or (:name conn) (:node conn)))
                                       nil)]
      (let [result (stateful/apply-write! :n1 {:nodes [:n1 :n2]} writer-conn op)]
        (is (= :ok (:type result)))
        (is (true? (:resolved? result)))
        (is (= {:resolution :replay :node :n2} (:result result)))
        (is (= [:writer :n2] @execute-calls))
        (is (= [:n1 :n2] (take 2 @open-calls)))
        (is (= :n2 (last @open-calls)))
        (is (every? #{:n2} @close-calls))
        (is (<= 2 (count @close-calls)))))))

(deftest autosched-write-retries-on-a-reopened-connection
  (let [old-conn      {:name :old}
        new-conn      {:name :new}
        conn-holder   (atom old-conn)
        execute-calls (atom [])
        closed-calls  (atom [])
        token         "mv-autosched-test-token"
        op            {:type :invoke
                       :f :move-group
                       :value {:id 1
                               :g1 2
                               :v1 3
                               :version 4
                               :last-token token
                               :deleted false
                               :pad (autosched/pad-for token)}}]
    (with-redefs [c/execute! (fn [conn _ _]
                               (swap! execute-calls conj (:name conn))
                               (when (= conn old-conn)
                                 (throw (java.sql.SQLNonTransientConnectionException.
                                         "execute() is called on closed connection"))))
                  c/open     (fn [_ _] new-conn)
                  c/close!   (fn [conn]
                               (swap! closed-calls conj (:name conn))
                               nil)]
      (let [result (autosched/apply-write-with-reconnect! conn-holder :node {} op)]
        (is (= :ok (:type result)))
        (is (= [:old :new] @execute-calls))
        (is (= [:old] @closed-calls))
        (is (= new-conn @conn-holder))))))

(deftest autosched-write-retries-when-op-times-out
  (let [old-conn      {:name :old}
        new-conn      {:name :new}
        conn-holder   (atom old-conn)
        execute-calls (atom [])
        aborted-calls (atom [])
        token         "mv-autosched-op-timeout-token"
        op            {:type :invoke
                       :f :move-group
                       :value {:id 1
                               :g1 2
                               :v1 3
                               :version 4
                               :last-token token
                               :deleted false
                               :pad (autosched/pad-for token)}}]
    (with-redefs [autosched/op-timeout-ms 20
                  c/execute!             (fn [conn _ _]
                                           (swap! execute-calls conj (:name conn))
                                           (when (= conn old-conn)
                                             (Thread/sleep 1000)))
                  c/open                 (fn [_ _] new-conn)
                  c/abort!               (fn [conn]
                                           (swap! aborted-calls conj (:name conn))
                                           nil)
                  c/close!               (fn [_]
                                           (throw (ex-info "close! should not be called" {})))]
      (let [result (autosched/apply-write-with-reconnect! conn-holder :node {} op)]
        (is (= :ok (:type result)))
        (is (= [:old :new] @execute-calls))
        (is (= [:old] @aborted-calls))
        (is (= new-conn @conn-holder))))))

(deftest autosched-client-open-retries-when-open-times-out
  (let [old-conn     {:name :old}
        new-conn     {:name :new}
        conn-holder  (atom old-conn)
        open-calls   (atom 0)
        closed-calls (atom [])
        mv-client    (autosched/->MVAutoschedClient conn-holder nil (atom false) (atom nil))]
    (with-redefs [c/open   (fn [_ _]
                             (if (= 1 (swap! open-calls inc))
                               (throw (ex-info "throw+: {:type :connect-timed-out, :node :n1}"
                                               {:type :connect-timed-out
                                                :node :n1}))
                               new-conn))
                  c/close! (fn [conn]
                             (swap! closed-calls conj (:name conn))
                             nil)]
      (let [opened (client/open! mv-client {} :n1)]
        (is (= :n1 (:node opened)))
        (is (= 2 @open-calls))
        (is (= [:old] @closed-calls))
        (is (not (identical? conn-holder (:conn-holder opened))))
        (is (nil? @conn-holder))
        (is (= new-conn @(-> opened :conn-holder)))))))

(deftest autosched-client-setup-runs-once-per-test-across-distinct-clients
  (let [setup-state  (atom {})
        setup-calls  (atom [])
        setup-result {:row-refresh-stmt "row"
                      :agg-refresh-stmt "agg"
                      :purge-schedule-stmt "purge"}
        test         {:name "mv-autosched-setup-once"
                      :start-time "20260316T000000.000Z"
                      :concurrency 5}
        client-a     (autosched/->MVAutoschedClient (atom nil) :n1 (atom false) (atom nil))
        client-b     (autosched/->MVAutoschedClient (atom nil) :n2 (atom false) (atom nil))]
    (with-redefs [autosched/autosched-schema-setup-by-test setup-state
                  autosched/with-reconnect!
                  (fn [_ node _ _ f]
                    (f {:node node}))
                  mv/setup-autosched-schema!
                  (fn [conn ids opts]
                    (swap! setup-calls conj {:conn conn :ids ids :opts opts})
                    setup-result)]
      (let [configured-a (client/setup! client-a test)
            configured-b (client/setup! client-b test)]
        (is (= 1 (count @setup-calls)))
        (is (= {:conn {:node :n1}
                :ids [1 2 3 4 5]
                :opts {:refresh-start-delay autosched/refresh-start-delay
                       :row-refresh-seconds autosched/row-refresh-seconds
                       :agg-refresh-seconds autosched/agg-refresh-seconds
                       :purge-start-delay autosched/purge-start-delay
                       :purge-next-seconds autosched/purge-next-seconds}}
               (first @setup-calls)))
        (is (true? @(-> configured-a :schema-created?)))
        (is (true? @(-> configured-b :schema-created?)))
        (is (= setup-result @(-> configured-a :schedule-meta)))
        (is (= setup-result @(-> configured-b :schedule-meta)))))))

(deftest autosched-verify-write-falls-back-to-other-nodes
  (let [token       "mv-autosched-verify-fallback-token"
        op          {:type :invoke
                     :f :update-value
                     :value {:id 1
                             :g1 2
                             :v1 3
                             :version 4
                             :last-token token
                             :deleted false
                             :pad (autosched/pad-for token)}}
        open-calls  (atom [])
        close-calls (atom [])]
    (with-redefs [c/open             (fn [node _]
                                       (swap! open-calls conj node)
                                       (case node
                                         :n1 (throw (ex-info "throw+: {:type :connect-timed-out, :node :n1}"
                                                             {:type :connect-timed-out
                                                              :node :n1}))
                                         {:node node}))
                  mv/query-stored-row (fn [conn id]
                                        (is (= 1 id))
                                        (when (= :n2 (:node conn))
                                          {:id 1
                                           :g1 2
                                           :v1 3
                                           :version 4
                                           :last_token token
                                           :deleted 0
                                           :pad (autosched/pad-for token)}))
                  c/close!           (fn [conn]
                                       (swap! close-calls conj (:node conn))
                                       nil)]
      (let [result (autosched/verify-write! :n1 {:nodes [:n1 :n2 :n3]} op)]
        (is (= :ok (:type result)))
        (is (true? (:resolved? result)))
        (is (= {:resolution :read-back :node :n2} (:result result)))
        (is (= [:n1 :n2] @open-calls))
        (is (= [:n2] @close-calls))))))

(deftest autosched-ambiguous-write-replays-via-fallback-node
  (let [token         "mv-autosched-replay-fallback-token"
        writer-conn   {:name :writer}
        open-calls    (atom [])
        execute-calls (atom [])
        close-calls   (atom [])
        op            {:type :invoke
                       :f :move-group
                       :value {:id 1
                               :g1 2
                               :v1 3
                               :version 4
                               :last-token token
                               :deleted false
                               :pad (autosched/pad-for token)}}]
    (with-redefs [c/execute!         (fn [conn _ _]
                                       (swap! execute-calls conj (or (:name conn) (:node conn)))
                                       (when (= writer-conn conn)
                                         (throw (ex-info "throw+: {:type :connect-timed-out, :node :n1}"
                                                         {:type :connect-timed-out
                                                          :node :n1}))))
                  c/open             (fn [node _]
                                       (swap! open-calls conj node)
                                       (case node
                                         :n1 (throw (ex-info "throw+: {:type :connect-timed-out, :node :n1}"
                                                             {:type :connect-timed-out
                                                              :node :n1}))
                                         {:node node}))
                  mv/query-stored-row (fn [_ _] nil)
                  c/close!           (fn [conn]
                                       (swap! close-calls conj (or (:name conn) (:node conn)))
                                       nil)]
      (let [result (autosched/apply-write! :n1 {:nodes [:n1 :n2]} writer-conn op)]
        (is (= :ok (:type result)))
        (is (true? (:resolved? result)))
        (is (= {:resolution :replay :node :n2} (:result result)))
        (is (= [:writer :n2] @execute-calls))
        (is (= [:n1 :n2] (take 2 @open-calls)))
        (is (= :n2 (last @open-calls)))
        (is (every? #{:n2} @close-calls))
        (is (<= 2 (count @close-calls)))))))

(deftest autosched-snapshot-retries-on-a-reopened-connection
  (let [old-conn      {:name :old}
        new-conn      {:name :new}
        conn-holder   (atom old-conn)
        snapshot-calls (atom [])
        closed-calls  (atom [])
        op            {:type :invoke :f :snapshot}]
    (with-redefs [autosched/snapshot-state
                  (fn [node conn schedule-meta]
                    (is (= {:log-table nil} schedule-meta))
                    (swap! snapshot-calls conj (:name conn))
                    (when (= conn old-conn)
                      (throw (java.sql.SQLNonTransientConnectionException.
                              "snapshot() is called on closed connection")))
                    {:snapshot-node (str node)
                     :row-equal? true
                     :agg-equal? true
                     :row-hash 1
                     :agg-hash 1
                     :log-row-count 0})
                  c/open   (fn [_ _] new-conn)
                  c/close! (fn [conn]
                             (swap! closed-calls conj (:name conn))
                             nil)]
      (let [result (autosched/snapshot-with-reconnect! conn-holder :n1 {} op {:log-table nil})]
        (is (= :ok (:type result)))
        (is (= [:old :new] @snapshot-calls))
        (is (= [:old] @closed-calls))
        (is (= new-conn @conn-holder))
        (is (= (str :n1) (get-in result [:result :snapshot-node])))))))

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

(deftest autosched-checker-separates-write-ambiguity-from-convergence
  (let [unresolved-write {:type :info
                          :f :update-value
                          :process 2
                          :time 42
                          :value {:id 3
                                  :g1 3
                                  :v1 300042
                                  :version 42
                                  :last-token "mv-autosched-test-42"
                                  :deleted false
                                  :pad (autosched/pad-for "mv-autosched-test-42")}
                          :error :indeterminate-write
                          :exception "Query timed out"}
        summary          (checker/check (autosched/checker*)
                                        {}
                                        [unresolved-write
                                         (snapshot-op 0 "n1" 10 20 5)
                                         (snapshot-op 1 "n1" 10 20 0)]
                                        nil)]
    (is (true? (:valid? summary)))
    (is (true? (:autosched-converged? summary)))
    (is (false? (:strict-valid? summary)))
    (is (false? (:write-resolution-valid? summary)))
    (is (true? (:recovered-write-ambiguity? summary)))
    (is (true? (:snapshot-valid? summary)))
    (is (= 1 (:unresolved-write-count summary)))
    (is (= unresolved-write (:first-unresolved-write summary)))
    (is (true? (:stable-after-quiet? summary)))
    (is (true? (:post-fault-refresh? summary)))
    (is (= :decreased (:post-fault-purge summary)))))

(deftest autosched-checker-records-runtime-and-history-progress
  (let [first-snapshot  (update (snapshot-op 0 "n1" true true 10 20 5)
                                :value
                                assoc
                                :runtime-metadata
                                {:rows [{:component :row-refresh
                                         :next-time-ms 1000
                                         :last-success-read-tso 10}
                                        {:component :agg-refresh
                                         :next-time-ms 2000
                                         :last-success-read-tso 20}
                                        {:component :log-purge
                                         :next-time-ms 3000
                                         :last-purged-tso 30}]
                                 :recent-history [{:component :row-refresh
                                                   :job-id 1
                                                   :status "RUNNING"
                                                   :end-time-ms 1010
                                                   :read-tso 10
                                                   :row-count 1}
                                                  {:component :agg-refresh
                                                   :job-id 2
                                                   :status "RUNNING"
                                                   :end-time-ms 2020
                                                   :read-tso 20
                                                   :row-count 2}
                                                  {:component :log-purge
                                                   :job-id 3
                                                   :status "RUNNING"
                                                   :end-time-ms 3030
                                                   :row-count 3}]})
        second-snapshot (update (snapshot-op 1 "n1" true true 10 20 0)
                                :value
                                assoc
                                :runtime-metadata
                                {:rows [{:component :row-refresh
                                         :next-time-ms 1100
                                         :last-success-read-tso 11}
                                        {:component :agg-refresh
                                         :next-time-ms 2100
                                         :last-success-read-tso 21}
                                        {:component :log-purge
                                         :next-time-ms 3100
                                         :last-purged-tso 31}]
                                 :recent-history [{:component :row-refresh
                                                   :job-id 4
                                                   :status "SUCCESS"
                                                   :end-time-ms 1110
                                                   :read-tso 11
                                                   :row-count 4}
                                                  {:component :agg-refresh
                                                   :job-id 5
                                                   :status "SUCCESS"
                                                   :end-time-ms 2120
                                                   :read-tso 21
                                                   :row-count 5}
                                                  {:component :log-purge
                                                   :job-id 6
                                                   :status "SUCCESS"
                                                   :end-time-ms 3130
                                                   :row-count 6}]})
        summary         (checker/check (autosched/checker*)
                                       {}
                                       [first-snapshot second-snapshot]
                                       nil)]
    (is (true? (:valid? summary)))
    (is (true? (:row-refresh-converged? summary)))
    (is (true? (:agg-refresh-converged? summary)))
    (is (true? (:row-refresh-runtime-advanced? summary)))
    (is (true? (:agg-refresh-runtime-advanced? summary)))
    (is (true? (:purge-runtime-advanced? summary)))
    (is (true? (:row-refresh-history-advanced? summary)))
    (is (true? (:agg-refresh-history-advanced? summary)))
    (is (true? (:purge-history-advanced? summary)))))

(deftest autosched-checker-still-fails-when-quiet-phase-does-not-converge
  (let [summary (checker/check (autosched/checker*)
                               {}
                               [(snapshot-op 0 "n1" false false 10 20 5)
                                (snapshot-op 1 "n1" false false 11 21 5)]
                               nil)]
    (is (false? (:valid? summary)))
    (is (false? (:autosched-converged? summary)))
    (is (false? (:strict-valid? summary)))
    (is (true? (:write-resolution-valid? summary)))
    (is (false? (:recovered-write-ambiguity? summary)))
    (is (true? (:snapshot-valid? summary)))
    (is (= 0 (:unresolved-write-count summary)))
    (is (false? (:stable-after-quiet? summary)))
    (is (false? (:post-fault-refresh? summary)))
    (is (= false (:post-fault-purge summary)))))

(deftest schedule-ddl-uses-datetime-next-expression
  (let [calls (atom [])
        mlog  (str "CREATE MATERIALIZED VIEW LOG ON mv_stateful_base "
                   "(id, g1, v1, version, last_token, deleted) "
                   "PURGE START WITH NOW(0) + INTERVAL 2 SECOND "
                   "NEXT NOW(0) + INTERVAL 11 SECOND")
        row   (str "ALTER MATERIALIZED VIEW mv_stateful_row "
                   "REFRESH START WITH NOW(0) + INTERVAL 2 SECOND "
                   "NEXT NOW(0) + INTERVAL 5 SECOND")
        purge (str "ALTER MATERIALIZED VIEW LOG ON mv_stateful_base "
                   "PURGE START WITH NOW(0) + INTERVAL 2 SECOND "
                   "NEXT NOW(0) + INTERVAL 11 SECOND")]
    (with-redefs [c/execute! (fn [_ [stmt] & _]
                               (swap! calls conj stmt)
                               nil)]
      (is (= mlog (mv/create-mlog-with-purge-schedule! ::conn 2 11)))
      (is (= row (mv/schedule-refresh! ::conn mv/row-view 2 5)))
      (is (= purge (mv/schedule-purge! ::conn 2 11)))
      (is (= [mlog row purge] @calls)))))

(deftest scheduled-view-create-ddl-uses-datetime-next-expression
  (let [calls (atom [])
        row   (str "CREATE MATERIALIZED VIEW mv_stateful_row "
                   "(id, g1, v1, version, last_token, live_cnt) "
                   "COMMENT = 'jepsen:mv-stateful(row)' "
                   "REFRESH FAST START WITH NOW(0) + INTERVAL 2 SECOND "
                   "NEXT NOW(0) + INTERVAL 5 SECOND AS "
                   "SELECT id, g1, v1, version, last_token, COUNT(*) AS live_cnt "
                   "FROM mv_stateful_base "
                   "WHERE deleted = 0 "
                   "GROUP BY id, g1, v1, version, last_token")]
    (with-redefs [c/execute! (fn [_ [stmt] & _]
                               (swap! calls conj stmt)
                               nil)]
      (is (= row (mv/create-row-view-with-schedule! ::conn 2 5)))
      (is (= [row] @calls)))))

(deftest autosched-setup-prefers-create-time-schedule-before-alter-fallback
  (let [table-reads (atom 0)
        alter-calls (atom [])
        result      (with-redefs [mv/drop-artifacts! (fn [_] nil)
                                  mv/create-base-table! (fn [_] nil)
                                  mv/split-base-table! (fn [_ _] nil)
                                  mv/list-table-names (fn [_]
                                                        (swap! table-reads inc)
                                                        (if (= 1 @table-reads)
                                                          #{mv/base-table}
                                                          #{mv/base-table "$mlog$mv_stateful_base"}))
                                  mv/create-mlog-with-purge-schedule! (fn [_ _ _] "mlog-scheduled")
                                  mv/create-row-view-with-schedule! (fn [_ _ _] "row-create-scheduled")
                                  mv/create-agg-view-with-schedule! (fn [_ _ _] "agg-create-scheduled")
                                  mv/create-row-view! (fn [_] "row-create-plain")
                                  mv/create-agg-view! (fn [_] "agg-create-plain")
                                  mv/schedule-refresh! (fn [& args]
                                                         (swap! alter-calls conj args)
                                                         "unexpected-alter")
                                  mv/schedule-purge! (fn [_ _ _] "purge-scheduled")]
                      (mv/setup-autosched-schema!
                       ::conn
                       [1 2 3 4 5]
                       {:refresh-start-delay 2
                        :row-refresh-seconds 5
                        :agg-refresh-seconds 7
                        :purge-start-delay 2
                        :purge-next-seconds 11}))]
    (is (= "row-create-scheduled" (:row-refresh-stmt result)))
    (is (= "agg-create-scheduled" (:agg-refresh-stmt result)))
    (is (= "purge-scheduled" (:purge-schedule-stmt result)))
    (is (= "$mlog$mv_stateful_base" (:log-table result)))
    (is (empty? @alter-calls))))

(deftest schedule-ddl-falls-back-to-legacy-expression-variant
  (let [calls  (atom [])
        first  (str "ALTER MATERIALIZED VIEW mv_stateful_row "
                    "REFRESH START WITH NOW(0) + INTERVAL 2 SECOND "
                    "NEXT NOW(0) + INTERVAL 5 SECOND")
        second (str "ALTER MATERIALIZED VIEW mv_stateful_row "
                    "START WITH NOW(0) + INTERVAL 2 SECOND "
                    "NEXT NOW(0) + INTERVAL 5 SECOND")
        legacy (str "ALTER MATERIALIZED VIEW mv_stateful_row "
                    "REFRESH START WITH (NOW() + INTERVAL 2 SECOND) "
                    "NEXT (NOW() + INTERVAL 5 SECOND)")]
    (with-redefs [c/execute! (fn [_ [stmt] & _]
                               (swap! calls conj stmt)
                               (when (#{first second} stmt)
                                 (throw (java.sql.SQLSyntaxErrorException.
                                         "synthetic schedule syntax failure")))
                               nil)]
      (is (= legacy (mv/schedule-refresh! ::conn mv/row-view 2 5)))
      (is (= [first second legacy] @calls)))))

(deftest schedule-ddl-falls-back-to-date-add-expression-variant
  (let [calls            (atom [])
        first            (str "ALTER MATERIALIZED VIEW mv_stateful_row "
                              "REFRESH START WITH NOW(0) + INTERVAL 2 SECOND "
                              "NEXT NOW(0) + INTERVAL 5 SECOND")
        second           (str "ALTER MATERIALIZED VIEW mv_stateful_row "
                              "START WITH NOW(0) + INTERVAL 2 SECOND "
                              "NEXT NOW(0) + INTERVAL 5 SECOND")
        legacy           (str "ALTER MATERIALIZED VIEW mv_stateful_row "
                              "REFRESH START WITH (NOW() + INTERVAL 2 SECOND) "
                              "NEXT (NOW() + INTERVAL 5 SECOND)")
        legacy-no-refresh (str "ALTER MATERIALIZED VIEW mv_stateful_row "
                               "START WITH (NOW() + INTERVAL 2 SECOND) "
                               "NEXT (NOW() + INTERVAL 5 SECOND)")
        date-add         (str "ALTER MATERIALIZED VIEW mv_stateful_row "
                              "REFRESH START WITH DATE_ADD(NOW(0), INTERVAL 2 SECOND) "
                              "NEXT DATE_ADD(NOW(0), INTERVAL 5 SECOND)")]
    (with-redefs [c/execute! (fn [_ [stmt] & _]
                               (swap! calls conj stmt)
                               (when (#{first second legacy legacy-no-refresh} stmt)
                                 (throw (java.sql.SQLSyntaxErrorException.
                                         "synthetic schedule syntax failure")))
                               nil)]
      (is (= date-add (mv/schedule-refresh! ::conn mv/row-view 2 5)))
      (is (= [first second legacy legacy-no-refresh date-add] @calls)))))

(deftest parse-next-literal-supports-datetime-next-expression
  (is (= 5
         (#'tidb.mview/parse-next-literal
          "REFRESH FAST START WITH NOW(0) + INTERVAL 10 SECOND NEXT NOW(0) + INTERVAL 5 SECOND AS SELECT 1")))
  (is (= 5
         (#'tidb.mview/parse-next-literal
          "REFRESH FAST START WITH (DATE_ADD(NOW(), INTERVAL 2 SECOND)) NEXT (DATE_ADD(NOW(), INTERVAL 5 SECOND)) AS SELECT 1")))
  (is (= 7
         (#'tidb.mview/parse-next-literal
          "REFRESH FAST START WITH (DATE_ADD(NOW(0), INTERVAL 2 SECOND)) NEXT (DATE_ADD(NOW(0), INTERVAL 7 SECOND)) AS SELECT 1")))
  (let [summary (#'tidb.mview/ddl-schedule-summary
                 :refresh
                 "mv_stateful_row"
                 5
                 {:available? true
                  :query "SHOW CREATE MATERIALIZED VIEW mv_stateful_row"
                  :ddl "CREATE MATERIALIZED VIEW mv_stateful_row REFRESH START WITH (NOW() + INTERVAL 2 SECOND) NEXT (NOW() + INTERVAL 5 SECOND) AS SELECT 1"})]
    (is (true? (:has-start-with? summary)))
    (is (true? (:has-next? summary)))
    (is (true? (:schedule-visible? summary)))
    (is (= 5 (:next-literal summary)))
    (is (true? (:next-matches-expected? summary)))))

(deftest read-timer-metadata-filters-to-mv-related-rows
  (with-redefs [c/query (fn [_ [_]]
                          [{:id 11
                            :namespace "default"
                            :timer_key "mview/mv_stateful_row/refresh"
                            :timezone "UTC"
                            :sched_policy_type "INTERVAL"
                            :sched_policy_expr "5s"
                            :hook_class "mview.refresh"
                            :watermark "2026-03-15 10:00:00"
                            :enable 1
                            :event_status "IDLE"
                            :event_id ""
                            :event_start nil
                            :create_time "2026-03-15 09:59:00"
                            :update_time "2026-03-15 10:00:00"
                            :version 7
                            :timer_ext "{}"
                            :timer_data nil
                            :summary_data nil
                            :event_data nil}
                           {:id 12
                            :namespace "default"
                            :timer_key "ttl/job/1"
                            :timezone "UTC"
                            :sched_policy_type "INTERVAL"
                            :sched_policy_expr "1h"
                            :hook_class "ttl.job"
                            :watermark "2026-03-15 10:00:00"
                            :enable 1
                            :event_status "TRIGGER"
                            :event_id "evt-1"
                            :event_start "2026-03-15 10:00:01"
                            :create_time "2026-03-15 09:00:00"
                            :update_time "2026-03-15 10:00:01"
                            :version 8
                            :timer_ext "{}"
                            :timer_data nil
                            :summary_data nil
                            :event_data nil}])]
    (let [summary (mv/read-timer-metadata ::conn {:log-table "$mlog$mv_stateful_base"})]
      (is (true? (:available? summary)))
      (is (= 2 (:total-count summary)))
      (is (= 1 (:match-count summary)))
      (is (nil? (:sample-rows summary)))
      (is (= [mv/row-view] (get-in summary [:rows 0 :matched-tokens])))
      (is (= [:row-refresh] (get-in summary [:rows 0 :matched-components])))
      (is (= "mview/mv_stateful_row/refresh"
             (get-in summary [:rows 0 :timer-key]))))))

(deftest read-timer-metadata-keeps-a-sample-when-no-rows-match
  (with-redefs [c/query (fn [_ [_]]
                          [{:id 21
                            :namespace "default"
                            :timer_key "ttl/job/1"
                            :timezone "UTC"
                            :sched_policy_type "INTERVAL"
                            :sched_policy_expr "1h"
                            :hook_class "ttl.job"
                            :watermark "2026-03-15 10:00:00"
                            :enable 1
                            :event_status "IDLE"
                            :event_id ""
                            :event_start nil
                            :create_time "2026-03-15 09:00:00"
                            :update_time "2026-03-15 10:00:00"
                            :version 1
                            :timer_ext "{}"
                            :timer_data nil
                            :summary_data nil
                            :event_data nil}])]
    (let [summary (mv/read-timer-metadata ::conn {:log-table "$mlog$mv_stateful_base"})]
      (is (true? (:available? summary)))
      (is (= 0 (:match-count summary)))
      (is (= [] (:rows summary)))
      (is (= "ttl/job/1" (get-in summary [:sample-rows 0 :timer-key]))))))

(deftest read-runtime-metadata-prefers-system-tables
  (with-redefs [c/query (fn [_ [sql & _]]
                          (cond
                            (re-find #"LEFT JOIN mysql\.tidb_mview_refresh_info" sql)
                            [{:object_name mv/row-view
                              :object_id 101
                              :info_present 1
                              :next_time_ms 1700000001000
                              :last_success_read_tso 12345}
                             {:object_name mv/agg-view
                              :object_id 102
                              :info_present 1
                              :next_time_ms 1700000002000
                              :last_success_read_tso 12346}]

                            (re-find #"LEFT JOIN mysql\.tidb_mlog_purge_info" sql)
                            [{:object_name "$mlog$mv_stateful_base"
                              :object_id 103
                              :info_present 1
                              :next_time_ms 1700000003000
                              :last_purged_tso 54321}]

                            (re-find #"JOIN mysql\.tidb_mview_refresh_hist" sql)
                            [{:object_name mv/row-view
                              :object_id 101
                              :job_id 9001
                              :start_time_ms 1700000000000
                              :end_time_ms 1700000001000
                              :status "SUCCESS"
                              :row_count 8
                              :read_tso 23456
                              :failed_reason nil}
                             {:object_name mv/agg-view
                              :object_id 102
                              :job_id 9002
                              :start_time_ms 1700000002000
                              :end_time_ms 1700000002500
                              :status "RUNNING"
                              :row_count 3
                              :read_tso 23457
                              :failed_reason "retrying"}]

                            (re-find #"JOIN mysql\.tidb_mlog_purge_hist" sql)
                            [{:object_name "$mlog$mv_stateful_base"
                              :object_id 103
                              :job_id 9003
                              :start_time_ms 1700000003000
                              :end_time_ms 1700000003200
                              :status "SUCCESS"
                              :row_count 5
                              :failed_reason nil}]

                            (re-find #"FROM mysql\.tidb_timers" sql)
                            []

                            :else
                            (throw (ex-info "unexpected query" {:sql sql}))))]
    (let [summary (mv/read-runtime-metadata ::conn {:log-table "$mlog$mv_stateful_base"})]
      (is (true? (:available? summary)))
      (is (= :system-tables (:source summary)))
      (is (= [mv/mview-refresh-info-table mv/mlog-purge-info-table]
             (:tables summary)))
      (is (= [mv/mview-refresh-hist-table mv/mlog-purge-hist-table]
             (:history-tables summary)))
      (is (= 3 (:match-count summary)))
      (is (= [] (:missing-components summary)))
      (is (= mv/recent-runtime-history-limit
             (:recent-history-limit summary)))
      (is (= 3 (:recent-history-count summary)))
      (is (= :row-refresh (get-in summary [:rows 0 :component])))
      (is (= 1700000001000 (get-in summary [:rows 0 :next-time-ms])))
      (is (= 12346 (get-in summary [:rows 1 :last-success-read-tso])))
      (is (= :log-purge (get-in summary [:rows 2 :component])))
      (is (= 54321 (get-in summary [:rows 2 :last-purged-tso])))
      (is (= :row-refresh (get-in summary [:recent-history 0 :component])))
      (is (= 23457 (get-in summary [:recent-history 1 :read-tso])))
      (is (= :log-purge (get-in summary [:recent-history 2 :component]))))))

(deftest read-runtime-metadata-falls-back-to-timers
  (with-redefs [c/query (fn [_ [sql & _]]
                          (cond
                            (re-find #"LEFT JOIN mysql\.tidb_mview_refresh_info" sql)
                            (throw (java.sql.SQLSyntaxErrorException.
                                    "Table 'mysql.tidb_mview_refresh_info' doesn't exist"))

                            (re-find #"FROM mysql\.tidb_timers" sql)
                            [{:id 31
                              :namespace "default"
                              :timer_key "mview/mv_stateful_row/refresh"
                              :timezone "UTC"
                              :sched_policy_type "INTERVAL"
                              :sched_policy_expr "5s"
                              :hook_class "mview.refresh"
                              :watermark "2026-03-15 10:00:00"
                              :enable 1
                              :event_status "IDLE"
                              :event_id ""
                              :event_start nil
                              :create_time "2026-03-15 09:59:00"
                              :update_time "2026-03-15 10:00:00"
                              :version 7
                              :timer_ext "{}"
                              :timer_data nil
                              :summary_data nil
                              :event_data nil}]

                            :else
                            []))]
    (let [summary (mv/read-runtime-metadata ::conn {:log-table "$mlog$mv_stateful_base"})]
      (is (true? (:available? summary)))
      (is (= :timers (:source summary)))
      (is (= :system-tables (:fallback-source summary)))
      (is (re-find #"tidb_mview_refresh_info" (:fallback-error summary)))
      (is (= 1 (:match-count summary)))
      (is (= "mview/mv_stateful_row/refresh"
             (get-in summary [:rows 0 :timer-key]))))))

(deftest quiet-generator-runs-the-full-sequence-per-client-thread
  (let [sleep-calls (atom [])]
    (with-redefs [gen/sleep (fn [seconds]
                              (let [seen? (atom false)]
                                (reify gen/Generator
                                  (op [_ _ process]
                                    (when-not @seen?
                                      (reset! seen? true)
                                      (swap! sleep-calls conj [process seconds]))
                                    nil))))]
      (let [generator (autosched/quiet-generator)
            test      {:concurrency 2}]
        (is (= {:type :invoke :f :snapshot}
               (gen/op generator test 0)))
        (is (= {:type :invoke :f :snapshot}
               (gen/op generator test 1)))
        (is (= [] @sleep-calls))
        (is (= {:type :invoke :f :snapshot}
               (gen/op generator test 0)))
        (is (= [[0 autosched/quiet-snapshot-interval-seconds]]
               @sleep-calls))
        (is (= {:type :invoke :f :snapshot}
               (gen/op generator test 1)))
        (is (= [[0 autosched/quiet-snapshot-interval-seconds]
                [1 autosched/quiet-snapshot-interval-seconds]]
               @sleep-calls))))))

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

(deftest timer-metadata-summary-is-node-local
  (let [summary (autosched-time/timer-metadata-summary
                 [(timer-snapshot 0 "n1"
                                  {:available? true
                                   :table "mysql.tidb_timers"
                                   :total-count 3
                                   :match-count 1
                                   :rows [{:id 1
                                           :timer-key "mview/mv_stateful_row/refresh"
                                           :event-status "IDLE"
                                           :matched-tokens [mv/row-view]
                                           :matched-components [:row-refresh]}]})
                  (timer-snapshot 1 "n2"
                                  {:available? false
                                   :table "mysql.tidb_timers"
                                   :error "permission denied"})
                  (timer-snapshot 2 "n1"
                                  {:available? true
                                   :table "mysql.tidb_timers"
                                   :total-count 3
                                   :match-count 1
                                   :rows [{:id 1
                                           :timer-key "mview/mv_stateful_row/refresh"
                                           :event-status "TRIGGER"
                                           :matched-tokens [mv/row-view]
                                           :matched-components [:row-refresh]}]})])]
    (is (= 3 (:observation-count summary)))
    (is (= 2 (:available-count summary)))
    (is (true? (:ever-matched? summary)))
    (is (= "n1" (get-in summary [:first-match :snapshot-node])))
    (is (= {"IDLE" 1 "TRIGGER" 1}
           (:event-status-counts summary)))
    (is (= false (get-in summary [:by-node "n1" :fully-unavailable?])))
    (is (= true (get-in summary [:by-node "n2" :fully-unavailable?])))))

(deftest runtime-metadata-summary-tracks-system-table-gaps-per-node
  (let [summary (autosched-time/runtime-metadata-summary
                 [(runtime-snapshot 0 "n1"
                                    {:available? true
                                     :source :system-tables
                                     :tables [mv/mview-refresh-info-table mv/mlog-purge-info-table]
                                     :match-count 2
                                     :missing-components [:agg-refresh]
                                     :rows [{:component :row-refresh
                                             :object mv/row-view
                                             :system-table mv/mview-refresh-info-table
                                             :info-present? true
                                             :next-time-ms 1700000001000}
                                            {:component :agg-refresh
                                             :object mv/agg-view
                                             :system-table mv/mview-refresh-info-table
                                             :info-present? false}
                                            {:component :log-purge
                                             :object "$mlog$mv_stateful_base"
                                             :system-table mv/mlog-purge-info-table
                                             :info-present? true
                                             :next-time-ms 1700000003000}]})
                  (runtime-snapshot 1 "n2"
                                    {:available? false
                                     :source :unavailable
                                     :error "permission denied"})
                  (runtime-snapshot 2 "n1"
                                    {:available? true
                                     :source :system-tables
                                     :tables [mv/mview-refresh-info-table mv/mlog-purge-info-table]
                                     :match-count 3
                                     :missing-components []
                                     :rows [{:component :row-refresh
                                             :object mv/row-view
                                             :system-table mv/mview-refresh-info-table
                                             :info-present? true
                                             :next-time-ms 1700000004000}
                                            {:component :agg-refresh
                                             :object mv/agg-view
                                             :system-table mv/mview-refresh-info-table
                                             :info-present? true
                                             :next-time-ms 1700000005000}
                                            {:component :log-purge
                                             :object "$mlog$mv_stateful_base"
                                             :system-table mv/mlog-purge-info-table
                                             :info-present? true
                                             :next-time-ms 1700000006000}]})])]
    (is (= 3 (:observation-count summary)))
    (is (= 2 (:available-count summary)))
    (is (true? (:ever-matched? summary)))
    (is (= {:row-refresh 2 :agg-refresh 1 :log-purge 2}
           (:present-component-counts summary)))
    (is (= {:agg-refresh 1}
           (:missing-component-counts summary)))
    (is (= #{} (clojure.set/difference #{:system-tables :unavailable}
                                       (set (:sources summary)))))
    (is (= false (get-in summary [:by-node "n1" :fully-unavailable?])))
    (is (= true (get-in summary [:by-node "n2" :fully-unavailable?])))))

(deftest autosched-time-check-only-uses-post-reset-snapshots
  (let [history [{:type :info
                  :f :bump-clock
                  :clock-offsets {"n1" 3.0}}
                 (time-snapshot-op 0 "n1" true true 1 1 5)
                 (time-snapshot-op 1000 "n1" true true 1 1 5)
                 {:type :info
                  :f :reset-clock
                  :clock-offsets {"n1" 0.0}}
                 (time-snapshot-op 40000 "n1" false false 2 2 5)
                 (time-snapshot-op 80000 "n1" false false 3 3 5)]
        summary (checker/check (autosched-time/checker*)
                               {:nemesis-spec {:clock-skew true}}
                               history
                               nil)
        anomaly-kinds (set (map :kind (:anomalies summary)))]
    (is (= 2 (:snapshot-count summary)))
    (is (= 4 (:raw-snapshot-count summary)))
    (is (= 2 (:ignored-pre-reset-snapshot-count summary)))
    (is (contains? anomaly-kinds :no-post-reset-convergence))
    (is (contains? anomaly-kinds :no-post-reset-stability))))

(deftest autosched-time-check-classifies-clock-skew-capability-gaps
  (let [history [{:type :info
                  :f :bump-clock
                  :value {"n1" 3.0}
                  :error "indeterminate: Command exited with non-zero status 2 on node n1:\nsettimeofday: Operation not permitted"}
                 (time-snapshot-op 0 "n1" true true 1 1 1)]
        summary (checker/check (autosched-time/checker*)
                               {:nemesis-spec {:clock-skew true}}
                               history
                               nil)
        anomaly-kinds (set (map :kind (:anomalies summary)))]
    (is (false? (:clock-skew-supported? summary)))
    (is (contains? anomaly-kinds :clock-skew-unsupported))
    (is (= :bump-clock (get-in summary [:first-clock-failure :f])))
    (is (re-find #"Operation not permitted"
                 (:clock-skew-support-error summary)))))

(deftest final-generator-reset-clock-targets-all-test-nodes
  (let [generator (nemesis/final-generator {:clock-skew true})
        test      {:nodes ["n1" "n2" "n3"]
                   :concurrency 1}
        op        (gen/op generator test :nemesis)]
    (is (= {:type :info
            :f :reset-clock
            :value ["n1" "n2" "n3"]}
           op))))

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

(deftest lifecycle-refresh-window-validation-allows-overlapping-agg-write-to-be-excluded
  (let [history (vec
                 (concat
                  (lifecycle-write-events
                   1 2 3 :insert
                   {:id 4 :g1 0 :v1 400013 :version 13 :last-token "tok-4" :deleted false :pad "pad-4"})
                  (lifecycle-write-events
                   3 4 2 :insert
                   {:id 3 :g1 3 :v1 300012 :version 12 :last-token "tok-3" :deleted false :pad "pad-3"})
                  (lifecycle-refresh-events
                   5 8 :refresh-agg :agg-refresh-after-recreate
                   (sorted-map
                    0 {:g1 0 :cnt 1 :sum-v1 400013 :min-v1 400013 :max-v1 400013}
                    3 {:g1 3 :cnt 1 :sum-v1 300012 :min-v1 300012 :max-v1 300012}))
                  (lifecycle-write-events
                   6 7 1 :update-value
                   {:id 2 :g1 2 :v1 200013 :version 13 :last-token "tok-2" :deleted false :pad "pad-2"})))
        validated (validate-lifecycle-refresh-history history)]
    (is (= :ok (:type validated)))
    (is (= :window-compatible (get-in validated [:result :validation])))
    (is (= 2 (get-in validated [:result :candidate-count])))
    (is (= 1 (get-in validated [:result :overlapping-write-count])))))

(deftest lifecycle-refresh-window-validation-allows-prefix-of-overlapping-row-writes
  (let [history (vec
                 (concat
                  (lifecycle-refresh-events
                   1 6 :refresh-row :row-refresh-after-recreate
                   (sorted-map
                    2 {:id 2 :g1 2 :v1 200005 :version 5 :last-token "tok-v5" :live-cnt 1}))
                  (lifecycle-write-events
                   2 3 1 :update-value
                   {:id 2 :g1 2 :v1 200005 :version 5 :last-token "tok-v5" :deleted false :pad "pad-v5"})
                  (lifecycle-write-events
                   4 5 1 :update-value
                   {:id 2 :g1 2 :v1 200006 :version 6 :last-token "tok-v6" :deleted false :pad "pad-v6"})))
        validated (validate-lifecycle-refresh-history history)]
    (is (= :ok (:type validated)))
    (is (= :window-compatible (get-in validated [:result :validation])))
    (is (= 3 (get-in validated [:result :candidate-count])))
    (is (= 2 (get-in validated [:result :overlapping-write-count])))))

(deftest lifecycle-refresh-window-validation-fails-when-definite-write-is-missing
  (let [history (vec
                 (concat
                  (lifecycle-write-events
                   1 2 1 :insert
                   {:id 2 :g1 2 :v1 200013 :version 13 :last-token "tok-2" :deleted false :pad "pad-2"})
                  (lifecycle-refresh-events
                   3 4 :refresh-agg :agg-refresh-after-recreate
                   (sorted-map))))
        validated (validate-lifecycle-refresh-history history)]
    (is (= :fail (:type validated)))
    (is (= :refresh-window-mismatch (:error validated)))
    (is (= '(2) (get-in validated [:result :diff :missing-in-mv])))
    (is (= {:invoke-index 3 :complete-index 4}
           (get-in validated [:result :window])))))

(deftest stateful-refresh-window-validation-allows-overlapping-row-write-to-be-excluded
  (let [history (vec
                 (concat
                  (lifecycle-write-events
                   1 2 1 :update-value
                   {:id 2 :g1 2 :v1 200005 :version 5 :last-token "tok-v5" :deleted false :pad "pad-v5"})
                  (stateful-refresh-events
                   3 6 :refresh-row 2 :fail
                   {:id 2
                    :diff {:expected {:id 2 :g1 2 :v1 200006 :version 6 :last-token "tok-v6"}
                           :actual   {:id 2 :g1 2 :v1 200005 :version 5 :last-token "tok-v5"}}})
                  (lifecycle-write-events
                   4 5 1 :update-value
                   {:id 2 :g1 2 :v1 200006 :version 6 :last-token "tok-v6" :deleted false :pad "pad-v6"})))
        validated (validate-stateful-refresh-history history)]
    (is (= :ok (:type validated)))
    (is (true? (:resolved? validated)))
    (is (= :window-compatible (get-in validated [:result :validation])))
    (is (= 2 (get-in validated [:result :candidate-count])))
    (is (= 1 (get-in validated [:result :overlapping-write-count])))))

(deftest stateful-refresh-window-validation-allows-overlapping-agg-write-to-be-excluded
  (let [history (vec
                 (concat
                  (lifecycle-write-events
                   1 2 3 :insert
                   {:id 4 :g1 0 :v1 400013 :version 13 :last-token "tok-4" :deleted false :pad "pad-4"})
                  (lifecycle-write-events
                   3 4 2 :insert
                   {:id 3 :g1 3 :v1 300012 :version 12 :last-token "tok-3" :deleted false :pad "pad-3"})
                  (stateful-refresh-events
                   5 8 :refresh-agg nil :fail
                   {:diff {:mismatched
                           {2 {:expected {:g1 2 :cnt 1 :sum-v1 200013 :min-v1 200013 :max-v1 200013}
                               :actual   nil}}}
                    :expected (sorted-map
                               0 {:g1 0 :cnt 1 :sum-v1 400013 :min-v1 400013 :max-v1 400013}
                               2 {:g1 2 :cnt 1 :sum-v1 200013 :min-v1 200013 :max-v1 200013}
                               3 {:g1 3 :cnt 1 :sum-v1 300012 :min-v1 300012 :max-v1 300012})
                    :actual (sorted-map
                             0 {:g1 0 :cnt 1 :sum-v1 400013 :min-v1 400013 :max-v1 400013}
                             3 {:g1 3 :cnt 1 :sum-v1 300012 :min-v1 300012 :max-v1 300012})})
                  (lifecycle-write-events
                   6 7 1 :insert
                   {:id 2 :g1 2 :v1 200013 :version 13 :last-token "tok-2" :deleted false :pad "pad-2"})))
        validated (validate-stateful-refresh-history history)]
    (is (= :ok (:type validated)))
    (is (true? (:resolved? validated)))
    (is (= :window-compatible (get-in validated [:result :validation])))
    (is (= 2 (get-in validated [:result :candidate-count])))
    (is (= 1 (get-in validated [:result :overlapping-write-count])))))

(deftest stateful-refresh-row-on-conn-keeps-refresh-and-compare-on-one-connection
  (let [conn       {:name :tx}
        op         {:type :invoke :f :refresh-row :value :all}
        base-rows  {1 {:id 1 :g1 1 :v1 11 :version 3 :last-token "tok-1"}}
        mv-rows    {1 {:id 1 :g1 1 :v1 11 :version 3 :last-token "tok-1" :live-cnt 1}}
        calls      (atom [])]
    (with-redefs [mv/refresh-view!           (fn [passed-conn view]
                                               (swap! calls conj [:refresh passed-conn view])
                                               nil)
                  mv/query-base-row-projection (fn [passed-conn]
                                                 (swap! calls conj [:base passed-conn])
                                                 base-rows)
                  mv/query-mv-row-projection   (fn [passed-conn]
                                                 (swap! calls conj [:mv passed-conn])
                                                 mv-rows)
                  mv/full-row-diff             (fn [expected actual]
                                                 (swap! calls conj [:diff expected actual])
                                                 nil)]
      (let [result (#'tidb.mv-stateful/refresh-row-on-conn! conn {} op)]
        (is (= :ok (:type result)))
        (is (= {:rows 1} (:result result)))
        (is (= [[:refresh conn mv/row-view]
                [:base conn]
                [:mv conn]
                [:diff base-rows mv-rows]]
               @calls))))))

(deftest mview-refresh-view-stops-fallback-when-object-is-missing
  (let [stmt  (str "REFRESH MATERIALIZED VIEW " mv/row-view " FAST")
        calls (atom [])]
    (with-redefs [c/execute! (fn [_ [passed-stmt] & _]
                               (swap! calls conj passed-stmt)
                               (throw (java.sql.SQLException.
                                       "[schema:1146] Table 'test.mv_stateful_row' doesn't exist")))]
      (is (thrown-with-msg? java.sql.SQLException
                            #"mv_stateful_row' doesn't exist"
                            (mv/refresh-view! ::conn mv/row-view)))
      (is (= [stmt] @calls)))))

(deftest mview-refresh-view-falls-back-to-complete
  (let [stmt1 (str "REFRESH MATERIALIZED VIEW " mv/row-view " FAST")
        stmt2 (str "REFRESH MATERIALIZED VIEW " mv/row-view " COMPLETE")
        calls (atom [])]
    (with-redefs [c/execute! (fn [_ [passed-stmt] & _]
                               (swap! calls conj passed-stmt)
                               (when (not= stmt2 passed-stmt)
                                 (throw (java.sql.SQLException.
                                         "You have an error in your SQL syntax"))))]
      (is (= stmt2 (mv/refresh-view! ::conn mv/row-view)))
      (is (= [stmt1 stmt2] @calls)))))

(deftest mview-refresh-view-prefers-fast-statement-when-supported
  (let [stmt  (str "REFRESH MATERIALIZED VIEW " mv/row-view " FAST")
        calls (atom [])]
    (with-redefs [c/execute! (fn [_ [passed-stmt] & _]
                               (swap! calls conj passed-stmt)
                               :ok)]
      (is (= stmt (mv/refresh-view! ::conn mv/row-view)))
      (is (= [stmt] @calls)))))

(deftest ensure-bin-layout-finishes-interrupted-single-file-normalization
  (let [root-bin    (str db/tidb-dir ".root-bin")
        db-bin-path (str db/tidb-dir "/" db/db-bin)
        present     (atom #{db/tidb-dir root-bin})
        dirs        (atom #{db/tidb-dir})
        calls       (atom [])]
    (with-redefs [jepsen.control.util/exists?
                  (fn [path]
                    (contains? @present path))
                  db/directory?
                  (fn [path]
                    (contains? @dirs path))
                  control/exec
                  (fn [& args]
                    (swap! calls conj (vec args))
                    (case (first args)
                      :mv
                      (let [[_ _ src dst] args
                            src-dir? (contains? @dirs src)]
                        (swap! present disj src)
                        (swap! dirs disj src)
                        (swap! present conj dst)
                        (when src-dir?
                          (swap! dirs conj dst)))

                      :mkdir
                      (let [path (last args)]
                        (swap! present conj path)
                        (swap! dirs conj path))

                      :rm
                      (let [path (last args)]
                        (swap! present disj path)
                        (swap! dirs disj path))

                      :ln
                      (let [path (last args)]
                        (swap! present conj path))
                      nil))]
      (db/ensure-bin-layout!)
      (is (false? (contains? @present root-bin)))
      (is (contains? @present db-bin-path))
      (is (contains? @present db/tidb-bin-dir))
      (is (some #(= [:mv :-f root-bin db-bin-path] %) @calls))
      (is (not-any? #(= [:mv :-f db/tidb-dir root-bin] %) @calls))
      (is (some #(= [:ln :-sf db-bin-path (str db/tidb-bin-dir "/" db/db-bin)] %) @calls)))))

(deftest install-required-reason-detects-broken-tidb-bin-symlink
  (let [present #{db/tidb-dir}]
    (with-redefs [jepsen.control.util/exists?
                  (fn [path]
                    (contains? present path))
                  db/path-live?
                  (fn [path]
                    (contains? #{(str db/tidb-bin-dir "/" db/pd-bin)
                                 (str db/tidb-bin-dir "/" db/kv-bin)}
                               path))]
      (is (= [:missing-binaries [db/db-bin]]
             (#'tidb.db/install-required-reason {}))))))

(deftest stateful-checker-trusts-final-refresh-over-intermediate-noise
  (let [history [{:type :ok :f :insert}
                 {:type :fail
                  :phase :active
                  :f :refresh-row
                  :process 0
                  :result {:diff {:expected {:id 3 :version 12}
                                  :actual   {:id 3 :version 11}}}}
                 {:type :fail
                  :phase :active
                  :f :refresh-agg
                  :process 0
                  :result {:diff {:unexpected-in-mv [0]}}}
                 {:type :ok :phase :active :f :purge :process 0 :result {:statement "PURGE"}}
                 {:type :ok :phase :final :f :refresh-row :process 0 :result {:rows 2}}
                 {:type :ok :phase :final :f :refresh-agg :process 0 :result {:groups 2}}]
        summary (checker/check (stateful/checker*) {} history nil)]
    (is (true? (:valid? summary)))
    (is (true? (:final-row-valid? summary)))
    (is (true? (:final-agg-valid? summary)))
    (is (true? (:ever-row-failed? summary)))
    (is (true? (:ever-agg-failed? summary)))
    (is (true? (:ever-active-row-failed? summary)))
    (is (true? (:ever-active-agg-failed? summary)))
    (is (false? (:ever-final-row-failed? summary)))
    (is (false? (:ever-final-agg-failed? summary)))
    (is (true? (:recovered-row-failure? summary)))
    (is (true? (:recovered-agg-failure? summary)))
    (is (= 1 (:refresh-row-fail-count summary)))
    (is (= 1 (:refresh-agg-fail-count summary)))
    (is (= 1 (:active-refresh-row-fail-count summary)))
    (is (= 1 (:active-refresh-agg-fail-count summary)))
    (is (= 1 (:final-refresh-row-ok-count summary)))
    (is (= 1 (:final-refresh-agg-ok-count summary)))
    (is (= 0 (:final-refresh-row-fail-count summary)))
    (is (= 0 (:final-refresh-agg-fail-count summary)))
    (is (= :refresh-row (:f (:first-row-failure summary))))
    (is (= :refresh-agg (:f (:first-agg-failure summary))))
    (is (= :refresh-row (:f (:first-active-row-failure summary))))
    (is (= :refresh-agg (:f (:first-active-agg-failure summary))))
    (is (nil? (:first-final-row-failure summary)))
    (is (nil? (:first-final-agg-failure summary)))
    (is (= "mv-stateful/first-row-failure.edn" (:first-row-failure-path summary)))
    (is (= "mv-stateful/first-agg-failure.edn" (:first-agg-failure-path summary)))))

(deftest stateful-checker-downgrades-window-compatible-active-failures
  (let [history (vec
                 (concat
                  (lifecycle-write-events
                   1 2 2 :insert
                   {:id 3 :g1 3 :v1 300008 :version 8 :last-token "mv-stateful-2-8" :deleted false :pad "pad-v8"})
                  (stateful-refresh-events
                   3 6 :refresh-row 3 :fail
                   {:id 3
                    :diff {:expected {:id 3 :g1 1 :v1 300009 :version 9 :last-token "mv-stateful-2-9"}
                           :actual   {:id 3 :g1 3 :v1 300008 :version 8 :last-token "mv-stateful-2-8"}}})
                  (lifecycle-write-events
                   4 5 2 :move-group
                   {:id 3 :g1 1 :v1 300009 :version 9 :last-token "mv-stateful-2-9" :deleted false :pad "pad-v9"})
                  [{:type :ok :phase :final :f :refresh-row :process 0 :result {:rows 1}}
                   {:type :ok :phase :final :f :refresh-agg :process 0 :result {:groups 1}}]))
        summary (checker/check (stateful/checker*) {} history nil)]
    (is (true? (:valid? summary)))
    (is (= 0 (:refresh-row-fail-count summary)))
    (is (= 0 (:active-refresh-row-fail-count summary)))
    (is (= 1 (:window-compatible-row-count summary)))
    (is (nil? (:first-row-failure summary)))
    (is (nil? (:first-failure summary)))))

(deftest stateful-checker-still-fails-when-final-refresh-fails
  (let [history [{:type :ok :phase :final :f :refresh-row :process 0 :result {:rows 1}}
                 {:type :fail
                  :phase :final
                  :f :refresh-agg
                  :process 0
                  :result {:diff {:mismatched {1 {:expected {:cnt 2}
                                                 :actual   {:cnt 1}}}}}}]
        summary (checker/check (stateful/checker*) {} history nil)]
    (is (false? (:valid? summary)))
    (is (true? (:final-row-valid? summary)))
    (is (false? (:final-agg-valid? summary)))
    (is (false? (:ever-row-failed? summary)))
    (is (true? (:ever-agg-failed? summary)))
    (is (false? (:ever-active-row-failed? summary)))
    (is (false? (:ever-active-agg-failed? summary)))
    (is (false? (:ever-final-row-failed? summary)))
    (is (true? (:ever-final-agg-failed? summary)))
    (is (false? (:recovered-row-failure? summary)))
    (is (false? (:recovered-agg-failure? summary)))
    (is (= 0 (:active-refresh-row-fail-count summary)))
    (is (= 0 (:active-refresh-agg-fail-count summary)))
    (is (= 0 (:final-refresh-row-fail-count summary)))
    (is (= 1 (:final-refresh-agg-fail-count summary)))
    (is (nil? (:first-row-failure summary)))
    (is (= :refresh-agg (:f (:first-agg-failure summary))))
    (is (nil? (:first-active-row-failure summary)))
    (is (nil? (:first-active-agg-failure summary)))
    (is (nil? (:first-final-row-failure summary)))
    (is (= :refresh-agg (:f (:first-final-agg-failure summary))))))

(deftest stateful-checker-writes-dedicated-failure-artifacts
  (let [history [{:type :fail
                  :f :refresh-row
                  :process 0
                  :result {:diff {:expected {:id 3 :version 12}
                                  :actual   {:id 3 :version 11}}}}
                 {:type :fail
                  :f :refresh-agg
                  :process 0
                  :result {:diff {:unexpected-in-mv [0]}}}
                 {:type :ok :f :refresh-row :process 0 :result {:rows 2}}
                 {:type :ok :f :refresh-agg :process 0 :result {:groups 2}}]
        writes  (atom [])]
    (with-redefs [artifact/write-edn+json!
                  (fn [_ path value]
                    (swap! writes conj {:path path :value value})
                    {:path path :value value})]
      (checker/check (stateful/checker*)
                     {:name "artifact-check" :start-time "20260313T000000.000Z"}
                     history
                     nil))
    (is (some #(= ["mv-stateful" "first-row-failure.edn"] (:path %)) @writes))
    (is (some #(= ["mv-stateful" "first-agg-failure.edn"] (:path %)) @writes))))

(deftest lifecycle-client-open-retries-when-open-times-out
  (let [old-conn     {:name :old}
        new-conn     {:name :new}
        conn-holder  (atom old-conn)
        open-calls   (atom 0)
        closed-calls (atom [])
        mv-client    (lifecycle/->MVLifecycleClient conn-holder nil (atom false))]
    (with-redefs [c/open   (fn [_ _]
                             (if (= 1 (swap! open-calls inc))
                               (throw (ex-info "throw+: {:type :connect-timed-out, :node :n1}"
                                               {:type :connect-timed-out
                                                :node :n1}))
                               new-conn))
                  c/close! (fn [conn]
                             (swap! closed-calls conj (:name conn))
                             nil)]
      (let [opened (client/open! mv-client {} :n1)]
        (is (= :n1 (:node opened)))
        (is (= 2 @open-calls))
        (is (= [:old] @closed-calls))
        (is (not (identical? conn-holder (:conn-holder opened))))
        (is (nil? @conn-holder))
        (is (= new-conn @(-> opened :conn-holder)))))))

(deftest lifecycle-client-writes-use-reconnect-aware-path
  (let [conn-holder (atom {:name :existing})
        mv-client   (lifecycle/->MVLifecycleClient conn-holder :n1 (atom false))
        op          {:type :invoke
                     :f :insert
                     :value {:id 1
                             :g1 1
                             :v1 1
                             :version 1
                             :last-token "mv-lifecycle-insert-token"
                             :deleted false
                             :pad "mv-lifecycle-insert-token......"}}
        calls       (atom [])]
    (with-redefs [stateful/apply-write-with-reconnect!
                  (fn [passed-holder passed-node passed-test passed-op]
                    (swap! calls conj {:holder passed-holder
                                       :node passed-node
                                       :test passed-test
                                       :op passed-op})
                    (assoc passed-op :type :ok :result {:resolution :reconnect}))]
      (let [result (client/invoke! mv-client {:name "mv-lifecycle"} op)]
        (is (= :ok (:type result)))
        (is (= 1 (count @calls)))
        (is (identical? conn-holder (:holder (first @calls))))
        (is (= :n1 (:node (first @calls))))
        (is (= op (:op (first @calls))))))))

(deftest lifecycle-transition-retries-artifact-check-on-reopened-connection
  (let [old-conn     {:name :old}
        new-conn     {:name :new}
        conn-holder  (atom old-conn)
        open-calls   (atom 0)
        close-calls  (atom [])
        action-calls (atom [])
        op           {:type :invoke
                      :f :drop-row-view
                      :expected-state {:base-table-present? true
                                       :row-view-present? false}}]
    (with-redefs [c/open            (fn [_ _]
                                      (swap! open-calls inc)
                                      new-conn)
                  c/close!          (fn [conn]
                                      (swap! close-calls conj (:name conn))
                                      nil)
                  mv/drop-row-view! (fn [conn]
                                      (swap! action-calls conj (:name conn))
                                      "DROP MATERIALIZED VIEW mv_stateful_row")
                  mv/artifact-state (fn [conn]
                                      (if (= :old (:name conn))
                                        (throw (java.sql.SQLNonTransientConnectionException.
                                                "Connection reset"))
                                        {:base-table-present? true
                                         :row-view-present? false
                                         :agg-view-present? true
                                         :mlog-present? true}))]
      (let [result (#'tidb.mv-lifecycle/lifecycle-transition-with-reconnect!
                    conn-holder
                    :n1
                    {}
                    op
                    mv/drop-row-view!)]
        (is (= :ok (:type result)))
        (is (= [:old :new] @action-calls))
        (is (= 1 @open-calls))
        (is (= [:old] @close-calls))
        (is (= new-conn @conn-holder))
        (is (= {:base-table-present? true
                :row-view-present? false}
               (get-in result [:result :expected-state])))
        (is (= {:base-table-present? true
                :row-view-present? false
                :agg-view-present? true
                :mlog-present? true}
               (get-in result [:result :artifact-state])))))))

(deftest lifecycle-refresh-failure-captures-artifact-state
  (let [conn-holder (atom {:name :existing})
        op          {:type :invoke
                     :f :refresh-row
                     :lifecycle-phase :baseline-row-refresh}
        actual      {:base-table-present? true
                     :row-view-present? false
                     :agg-view-present? true
                     :mlog-present? true}]
    (with-redefs [stateful/with-reconnect!
                  (fn [_ _ _ _ f]
                    (if (identical? f mv/artifact-state)
                      (f ::probe-conn)
                      (f ::refresh-conn)))
                  mv/refresh-view!
                  (fn [_ _]
                    (throw (java.sql.SQLException.
                            "[schema:1146] Table 'test.mv_stateful_row' doesn't exist")))
                  mv/artifact-state
                  (fn [conn]
                    (is (= ::probe-conn conn))
                    actual)]
      (let [result (#'tidb.mv-lifecycle/lifecycle-refresh-row-with-reconnect!
                    conn-holder
                    :n1
                    {}
                    op)]
        (is (= :fail (:type result)))
        (is (= :refresh-row-error (:error result)))
        (is (= actual (get-in result [:result :artifact-state])))))))

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
        (is (= (:expected-state op) (get-in result [:result :expected-state])))
        (is (= actual (get-in result [:result :artifact-state])))))))

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
               (get-in result [:result :diff])))))))

(deftest mview-drop-ddl-does-not-use-if-exists
  (let [calls (atom [])]
    (with-redefs [c/execute! (fn [_ [stmt] & _]
                               (swap! calls conj stmt)
                               nil)]
      (is (= "DROP MATERIALIZED VIEW mv_stateful_row"
             (mv/drop-row-view! ::conn)))
      (is (= "DROP MATERIALIZED VIEW mv_stateful_agg"
             (mv/drop-agg-view! ::conn)))
      (mv/drop-artifacts! ::conn)
      (is (= ["DROP MATERIALIZED VIEW mv_stateful_row"
              "DROP MATERIALIZED VIEW mv_stateful_agg"
              "DROP MATERIALIZED VIEW mv_stateful_agg"
              "DROP MATERIALIZED VIEW mv_stateful_row"
              "DROP MATERIALIZED VIEW LOG ON mv_stateful_base"
              "DROP TABLE IF EXISTS mv_stateful_base"]
             @calls)))))

(deftest mview-artifact-state-ignores-non-mlog-tables
  (with-redefs [c/query (fn [_ [stmt] & _]
                          (is (= "SHOW TABLES" stmt))
                          [{:table "jepsen_await"}
                           {:table "mv_stateful_base"}
                           {:table "mv_stateful_row"}
                           {:table "$mlog$mv_stateful_base"}])]
    (is (= {:base-table-present? true
            :row-view-present? true
            :agg-view-present? false
            :mlog-present? true
            :log-table "$mlog$mv_stateful_base"
            :tables ["$mlog$mv_stateful_base" "jepsen_await" "mv_stateful_base" "mv_stateful_row"]}
           (mv/artifact-state ::conn))))
  (with-redefs [c/query (fn [_ [stmt] & _]
                          (is (= "SHOW TABLES" stmt))
                          [{:table "jepsen_await"}
                           {:table "mv_stateful_base"}])]
    (is (= {:base-table-present? true
            :row-view-present? false
            :agg-view-present? false
            :mlog-present? false
            :log-table nil
            :tables ["jepsen_await" "mv_stateful_base"]}
           (mv/artifact-state ::conn)))))
