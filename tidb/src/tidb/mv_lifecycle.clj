(ns tidb.mv-lifecycle
  (:refer-clojure :exclude [test])
  (:require [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [knossos.op :as op]
            [tidb.artifact :as artifact]
            [tidb.mview :as mv]
            [tidb.mv-stateful :as stateful]
            [tidb.sql :as c]))

(def management-ops
  [{:type :invoke
    :f :refresh-row
    :value :all
    :lifecycle-phase :baseline-row-refresh}
   {:type :invoke
    :f :refresh-agg
    :lifecycle-phase :baseline-agg-refresh}
   {:type :invoke
    :f :drop-row-view
    :expected-state {:base-table-present? true
                     :row-view-present? false}
    :lifecycle-phase :drop-row-view}
   {:type :invoke
    :f :create-row-view
    :expected-state {:base-table-present? true
                     :row-view-present? true
                     :mlog-present? true}
    :lifecycle-phase :recreate-row-view}
   {:type :invoke
    :f :refresh-row
    :value :all
    :lifecycle-phase :row-refresh-after-recreate}
   {:type :invoke
    :f :drop-agg-view
    :expected-state {:base-table-present? true
                     :agg-view-present? false}
    :lifecycle-phase :drop-agg-view}
   {:type :invoke
    :f :create-agg-view
    :expected-state {:base-table-present? true
                     :agg-view-present? true
                     :mlog-present? true}
    :lifecycle-phase :recreate-agg-view}
   {:type :invoke
    :f :refresh-agg
    :lifecycle-phase :agg-refresh-after-recreate}
   {:type :invoke
    :f :drop-row-view
    :expected-state {:base-table-present? true
                     :row-view-present? false}
    :lifecycle-phase :drop-row-before-chain-rebuild}
   {:type :invoke
    :f :drop-agg-view
    :expected-state {:base-table-present? true
                     :row-view-present? false
                     :agg-view-present? false}
    :lifecycle-phase :drop-agg-before-chain-rebuild}
   {:type :invoke
    :f :drop-mlog
    :expected-state {:base-table-present? true
                     :row-view-present? false
                     :agg-view-present? false
                     :mlog-present? false}
    :lifecycle-phase :drop-mlog}
   {:type :invoke
    :f :create-mlog
    :expected-state {:base-table-present? true
                     :row-view-present? false
                     :agg-view-present? false
                     :mlog-present? true}
    :lifecycle-phase :recreate-mlog}
   {:type :invoke
    :f :create-agg-view
    :expected-state {:base-table-present? true
                     :agg-view-present? true
                     :mlog-present? true}
    :lifecycle-phase :recreate-agg-before-row}
   {:type :invoke
    :f :create-row-view
    :expected-state {:base-table-present? true
                     :row-view-present? true
                     :agg-view-present? true
                     :mlog-present? true}
    :lifecycle-phase :recreate-row-after-agg}
   {:type :invoke
    :f :refresh-row
    :value :all
    :lifecycle-phase :row-refresh-after-chain-rebuild}
   {:type :invoke
    :f :refresh-agg
    :lifecycle-phase :agg-refresh-after-chain-rebuild}
   {:type :invoke
    :f :purge
    :lifecycle-phase :purge-after-chain-rebuild}])

(defn artifact-state-diff
  [actual expected]
  (->> expected
       (keep (fn [[key expected-value]]
               (when (not= expected-value (get actual key))
                 [key {:expected expected-value
                       :actual   (get actual key)}])))
       (into (sorted-map))))

(defn- transition-value
  [expected actual diff]
  (cond-> {:expected-state expected}
    actual (assoc :artifact-state actual)
    (seq diff) (assoc :diff diff)))

(defn- next-write-op
  [seqs process]
  (let [seq (get (swap! seqs update process (fnil inc 0)) process)]
    (stateful/stateful-write process seq)))

(defn generator
  []
  (let [seqs       (atom {})
        management (atom 0)]
    (reify gen/Generator
      (op [_ _ process]
        (let [process (stateful/process-id process)]
          (if (zero? process)
            (if-let [next-op (let [index @management]
                               (when (< index (count management-ops))
                                 (swap! management inc)
                                 (nth management-ops index)))]
              next-op
              (next-write-op seqs process))
            (next-write-op seqs process)))))))

(defn- lifecycle-transition!
  [conn op action]
  (let [expected (:expected-state op)]
    (try
      (action)
      (let [actual (mv/artifact-state conn)
            diff   (artifact-state-diff actual expected)]
        (if (seq diff)
          (assoc op :type :fail
                    :error :artifact-state-mismatch
                    :value (transition-value expected actual diff))
          (assoc op :type :ok
                    :value (transition-value expected actual diff))))
      (catch Throwable t
        (let [actual (try
                       (mv/artifact-state conn)
                       (catch Throwable _
                         nil))
              diff   (when actual
                       (artifact-state-diff actual expected))
              value  (transition-value expected actual diff)]
          (if (and actual
                   (empty? diff))
            (assoc op :type :ok
                      :resolved? true
                      :exception (.getMessage t)
                      :value value)
            (assoc op :type :fail
                      :error (keyword (str (name (:f op)) "-error"))
                      :exception (.getMessage t)
                      :value value)))))))

(defn- completed-op?
  [op]
  (or (op/ok? op)
      (op/fail? op)))

(defrecord MVLifecycleClient [conn node schema-created?]
  client/Client

  (open! [this test node]
    (assoc this :node node :conn (c/open node test)))

  (setup! [this test]
    (when (compare-and-set! schema-created? false true)
      (let [ids (map stateful/key-for-process
                     (range (long (max 1 (:concurrency test)))))]
        (c/with-conn-failure-retry conn
          (mv/setup-stateful-schema! conn ids))))
    this)

  (invoke! [this test op]
    (condp = (:f op)
      :insert       (stateful/apply-write! node test conn op)
      :update-value (stateful/apply-write! node test conn op)
      :move-group   (stateful/apply-write! node test conn op)
      :delete       (stateful/apply-write! node test conn op)

      :drop-row-view
      (lifecycle-transition! conn op #(mv/drop-row-view! conn))

      :create-row-view
      (lifecycle-transition! conn op #(mv/create-row-view! conn))

      :drop-agg-view
      (lifecycle-transition! conn op #(mv/drop-agg-view! conn))

      :create-agg-view
      (lifecycle-transition! conn op #(mv/create-agg-view! conn))

      :drop-mlog
      (lifecycle-transition! conn op #(mv/drop-mlog! conn))

      :create-mlog
      (lifecycle-transition! conn op #(mv/create-mlog! conn))

      :refresh-row
      (try
        (mv/refresh-view! conn mv/row-view)
        (let [comparison (if (= :all (:value op))
                           (stateful/compare-all-rows! conn test)
                           (stateful/compare-row! conn test (or (:value op)
                                                                (stateful/key-for-process (:process op)))))
              {:keys [type value]} comparison]
          (assoc op :type type :value value))
        (catch Throwable t
          (assoc op :type :fail :error :refresh-row-error :exception (.getMessage t))))

      :refresh-agg
      (try
        (mv/refresh-view! conn mv/agg-view)
        (let [{:keys [type value]} (stateful/compare-agg! conn test)]
          (assoc op :type type :value value))
        (catch Throwable t
          (assoc op :type :fail :error :refresh-agg-error :exception (.getMessage t))))

      :purge
      (try
        (assoc op :type :ok :value {:statement (mv/purge-log! conn)})
        (catch Throwable t
          (assoc op :type :fail :error :purge-error :exception (.getMessage t))))

      (assoc op :type :fail :error :unknown-op)))

  (teardown! [_ _])

  (close! [_ _]
    (c/close! conn)))

(defn checker*
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [refresh-row-ops      (filter #(and (= :refresh-row (:f %))
                                               (completed-op? %))
                                         history)
            refresh-agg-ops      (filter #(and (= :refresh-agg (:f %))
                                               (completed-op? %))
                                         history)
            purge-ops            (filter #(and (= :purge (:f %))
                                               (completed-op? %))
                                         history)
            lifecycle-ops        (filter #(and (:lifecycle-phase %)
                                               (completed-op? %))
                                         history)
            refresh-purge        (concat refresh-row-ops refresh-agg-ops purge-ops)
            recent-rp            (vec (take-last 20 refresh-purge))
            write-ops            (filter #(contains? #{:insert :update-value :move-group :delete} (:f %))
                                         history)
            failures             (filter op/fail? (concat lifecycle-ops refresh-purge))
            unresolved           (filter #(and (= :info (:type %))
                                               (contains? #{:insert :update-value :move-group :delete} (:f %)))
                                         write-ops)
            lifecycle-complete?  (= (count lifecycle-ops) (count management-ops))
            first-failure        (first failures)
            first-lifecycle-fail (first (filter op/fail? lifecycle-ops))
            final-row-check      (some-> refresh-row-ops last :value)
            final-agg-check      (some-> refresh-agg-ops last :value)
            lifecycle-history    (vec lifecycle-ops)
            summary              {:valid?                        (and lifecycle-complete?
                                                                        (empty? failures)
                                                                        (empty? unresolved))
                                  :lifecycle-complete?           lifecycle-complete?
                                  :expected-lifecycle-op-count   (count management-ops)
                                  :lifecycle-op-count            (count lifecycle-ops)
                                  :lifecycle-ok-count            (count (filter op/ok? lifecycle-ops))
                                  :lifecycle-fail-count          (count (filter op/fail? lifecycle-ops))
                                  :lifecycle-ok-by-op            (frequencies (map :f (filter op/ok? lifecycle-ops)))
                                  :lifecycle-fail-by-op          (frequencies (map :f (filter op/fail? lifecycle-ops)))
                                  :refresh-row-ok-count          (count (filter op/ok? refresh-row-ops))
                                  :refresh-row-fail-count        (count (filter op/fail? refresh-row-ops))
                                  :refresh-agg-ok-count          (count (filter op/ok? refresh-agg-ops))
                                  :refresh-agg-fail-count        (count (filter op/fail? refresh-agg-ops))
                                  :purge-ok-count                (count (filter op/ok? purge-ops))
                                  :purge-fail-count              (count (filter op/fail? purge-ops))
                                  :unresolved-write-count        (count unresolved)
                                  :first-failure                 first-failure
                                  :first-lifecycle-failure       first-lifecycle-fail
                                  :first-unresolved-write        (first unresolved)
                                  :final-row-check-path          "mv-lifecycle/final-row-check.edn"
                                  :final-row-check-json-path     "mv-lifecycle/final-row-check.json"
                                  :final-agg-check-path          "mv-lifecycle/final-agg-check.edn"
                                  :final-agg-check-json-path     "mv-lifecycle/final-agg-check.json"
                                  :lifecycle-history-path        "mv-lifecycle/lifecycle-history.edn"
                                  :lifecycle-history-json-path   "mv-lifecycle/lifecycle-history.json"
                                  :recent-refresh-purge-path     "mv-lifecycle/recent-refresh-purge.edn"
                                  :recent-refresh-purge-json-path "mv-lifecycle/recent-refresh-purge.json"
                                  :first-failure-path            "mv-lifecycle/first-failure.edn"
                                  :first-failure-json-path       "mv-lifecycle/first-failure.json"
                                  :summary-path                  "mv-lifecycle/summary.edn"
                                  :summary-json-path             "mv-lifecycle/summary.json"}]
        (when (and (:name test) (:start-time test))
          (artifact/write-edn+json! test ["mv-lifecycle" "final-row-check.edn"] final-row-check)
          (artifact/write-edn+json! test ["mv-lifecycle" "final-agg-check.edn"] final-agg-check)
          (artifact/write-edn+json! test ["mv-lifecycle" "lifecycle-history.edn"] lifecycle-history)
          (artifact/write-edn+json! test ["mv-lifecycle" "recent-refresh-purge.edn"] recent-rp)
          (artifact/write-edn+json! test ["mv-lifecycle" "first-failure.edn"] first-failure)
          (artifact/write-edn+json! test ["mv-lifecycle" "summary.edn"] summary))
        summary))))

(defn workload
  [_]
  {:client          (MVLifecycleClient. nil nil (atom false))
   :generator       (gen/stagger 1/5 (generator))
   :checker         (checker/compose {:mv-lifecycle (checker*)
                                      :timeline     (timeline/html)})
   :final-generator (gen/seq [{:type :invoke, :f :refresh-row, :value :all}
                              {:type :invoke, :f :refresh-agg}
                              {:type :invoke, :f :purge}
                              {:type :invoke, :f :refresh-row, :value :all}
                              {:type :invoke, :f :refresh-agg}])})
