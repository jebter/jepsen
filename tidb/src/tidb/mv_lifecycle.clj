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

(def write-op-fns
  #{:insert :update-value :move-group :delete})

(def refresh-op-fns
  #{:refresh-row :refresh-agg})

(def lifecycle-transition-settle-timeout-ms
  15000)

(def lifecycle-transition-settle-poll-ms
  500)

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

(defn- await-transition-state!
  [read-state! expected]
  (let [deadline (+ (System/nanoTime)
                    (* lifecycle-transition-settle-timeout-ms 1000000))]
    (loop [last-actual nil
           last-diff   nil]
      (let [actual' (try
                      (read-state!)
                      (catch Throwable _
                        nil))
            diff'   (when actual'
                      (artifact-state-diff actual' expected))
            actual  (or actual' last-actual)
            diff    (or diff' last-diff)]
        (cond
          (and actual' (empty? diff'))
          {:actual actual'
           :diff   diff'}

          (< (System/nanoTime) deadline)
          (do
            (Thread/sleep lifecycle-transition-settle-poll-ms)
            (recur actual diff))

          :else
          {:actual actual
           :diff   diff})))))

(defn- next-write-op
  [seqs process]
  (let [seq (get (swap! seqs update process (fnil inc 0)) process)]
    (stateful/stateful-write process seq)))

(defn- lifecycle-transition-on-conn!
  [conn op action]
  (let [expected (:expected-state op)]
    (action conn)
    (let [actual (mv/artifact-state conn)
          diff   (artifact-state-diff actual expected)]
      (if (seq diff)
        (assoc op :type :fail
                  :error :artifact-state-mismatch
                  :result (transition-value expected actual diff))
        (assoc op :type :ok
                  :result (transition-value expected actual diff))))))

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
      (lifecycle-transition-on-conn! conn op (fn [_] (action)))
      (catch Throwable t
        (let [{:keys [actual diff]}
              (await-transition-state! #(mv/artifact-state conn) expected)
              value  (transition-value expected actual diff)]
          (if (and actual
                   (empty? diff))
            (assoc op :type :ok
                      :resolved? true
                      :exception (.getMessage t)
                      :result value)
            (assoc op :type :fail
                      :error (keyword (str (name (:f op)) "-error"))
                      :exception (.getMessage t)
                      :result value)))))))

(defn- lifecycle-transition-with-reconnect!
  [conn-holder node test op action]
  (let [expected (:expected-state op)]
    (try
      (stateful/with-reconnect! conn-holder node test stateful/setup-retryable-error?
        (fn [conn]
          (lifecycle-transition-on-conn! conn op action)))
      (catch Throwable t
        (let [{:keys [actual diff]}
              (await-transition-state!
               #(stateful/with-reconnect! conn-holder node test stateful/setup-retryable-error?
                  mv/artifact-state)
               expected)
              value  (transition-value expected actual diff)]
          (if (and actual
                   (empty? diff))
            (assoc op :type :ok
                      :resolved? true
                      :exception (.getMessage t)
                      :result value)
            (assoc op :type :fail
                      :error (keyword (str (name (:f op)) "-error"))
                      :exception (.getMessage t)
                      :result value)))))))

(defn- lifecycle-refresh-row-on-conn!
  [conn op]
  (mv/refresh-view! conn mv/row-view)
  (let [actual (mv/query-mv-row-projection conn)]
    (assoc op :type :ok :result {:rows (count actual)
                                 :actual actual})))

(defn- lifecycle-refresh-agg-on-conn!
  [conn op]
  (mv/refresh-view! conn mv/agg-view)
  (let [actual (mv/query-mv-agg conn)]
    (assoc op :type :ok :result {:groups (count actual)
                                 :actual actual})))

(defn- lifecycle-refresh-failure
  [conn-holder node test op error t]
  (let [actual (try
                 (stateful/with-reconnect! conn-holder node test stateful/setup-retryable-error?
                   mv/artifact-state)
                 (catch Throwable _
                   nil))]
    (cond-> (assoc op :type :fail :error error :exception (.getMessage t))
      actual (assoc :result {:artifact-state actual}))))

(defn- lifecycle-refresh-row-with-reconnect!
  [conn-holder node test op]
  (try
    (stateful/with-reconnect! conn-holder node test stateful/ambiguous-write-error?
      #(lifecycle-refresh-row-on-conn! % op))
    (catch Throwable t
      (lifecycle-refresh-failure conn-holder node test op :refresh-row-error t))))

(defn- lifecycle-refresh-agg-with-reconnect!
  [conn-holder node test op]
  (try
    (stateful/with-reconnect! conn-holder node test stateful/ambiguous-write-error?
      #(lifecycle-refresh-agg-on-conn! % op))
    (catch Throwable t
      (lifecycle-refresh-failure conn-holder node test op :refresh-agg-error t))))

(defn- pair-history
  [history]
  (second
   (reduce (fn [[pending pairs] op]
             (cond
               (= :invoke (:type op))
               [(assoc pending (:process op) op) pairs]

               (stateful/completed-op? op)
               (if-let [invoke (get pending (:process op))]
                 [(dissoc pending (:process op))
                  (conj pairs {:invoke invoke
                               :complete op})]
                 [pending pairs])

               :else
               [pending pairs]))
           [{} []]
           history)))

(defn- write-pair?
  [pair]
  (contains? write-op-fns (get-in pair [:complete :f])))

(defn- lifecycle-refresh-pair?
  [pair]
  (and (get-in pair [:complete :lifecycle-phase])
       (contains? refresh-op-fns (get-in pair [:complete :f]))))

(defn- pair-index
  [pair key]
  (:index (key pair)))

(defn- write-value
  [pair]
  (get-in pair [:complete :value]))

(defn- write-process
  [pair]
  (get-in pair [:complete :process]))

(defn- live-row
  [{:keys [id g1 v1 version last-token deleted]}]
  (when (and (some? id) (not deleted))
    {:id         (long id)
     :g1         (long g1)
     :v1         (long v1)
     :version    (long version)
     :last-token last-token}))

(defn- row-model
  [state]
  (->> state
       (keep (fn [[id value]]
               (when-let [row (live-row value)]
                 [(long id) row])))
       (into (sorted-map))))

(defn- agg-model
  [state]
  (->> state
       vals
       (keep live-row)
       (reduce (fn [groups {:keys [g1 v1]}]
                 (update groups g1
                         (fn [row]
                           (if row
                             {:g1     g1
                              :cnt    (inc (:cnt row))
                              :sum-v1 (+ (:sum-v1 row) v1)
                              :min-v1 (min (:min-v1 row) v1)
                              :max-v1 (max (:max-v1 row) v1)}
                             {:g1     g1
                              :cnt    1
                              :sum-v1 v1
                              :min-v1 v1
                              :max-v1 v1}))))
               (sorted-map))))

(defn- refresh-window-state-choices
  [write-pairs refresh-pair]
  (let [invoke-index   (pair-index refresh-pair :invoke)
        complete-index (pair-index refresh-pair :complete)
        before         (->> write-pairs
                            (filter #(< (pair-index % :complete) invoke-index))
                            (sort-by #(pair-index % :complete))
                            (reduce (fn [state pair]
                                      (assoc state
                                             (write-process pair)
                                             (write-value pair)))
                                    {}))
        overlapping    (->> write-pairs
                            (remove #(< (pair-index % :complete) invoke-index))
                            (remove #(> (pair-index % :invoke) complete-index))
                            (sort-by #(pair-index % :invoke))
                            (group-by write-process))
        processes      (sort (distinct (concat (keys before)
                                               (keys overlapping))))]
    {:overlapping-write-count (reduce + 0 (map count (vals overlapping)))
     :states
     (letfn [(step [remaining current]
               (if-let [process (first remaining)]
                 (let [base-choice  (get before process)
                       next-choices (cons base-choice
                                          (map write-value (get overlapping process [])))]
                   (mapcat (fn [choice]
                             (step (rest remaining)
                                   (if choice
                                     (assoc current (:id choice) choice)
                                     current)))
                           next-choices))
                 [current]))]
       (step processes {}))}))

(defn- diff-score
  [diff]
  (+ (count (:missing-in-mv diff))
     (count (:unexpected-in-mv diff))
     (count (:mismatched diff))
     (count (:bad-live-cnt diff))))

(defn- refresh-window-check
  [refresh-pair write-pairs]
  (let [complete                    (:complete refresh-pair)
        actual                      (get-in complete [:result :actual])
        {:keys [states
                overlapping-write-count]} (refresh-window-state-choices write-pairs refresh-pair)
        candidates                  (map (fn [state]
                                           (let [expected (case (:f complete)
                                                            :refresh-row (row-model state)
                                                            :refresh-agg (agg-model state))
                                                 diff     (case (:f complete)
                                                            :refresh-row (mv/full-row-diff expected actual)
                                                            :refresh-agg (mv/agg-diff expected actual))]
                                             {:expected expected
                                              :diff diff}))
                                         states)
        candidate-count             (count states)
        window                      {:invoke-index   (pair-index refresh-pair :invoke)
                                     :complete-index (pair-index refresh-pair :complete)}
        matched?                    (some #(empty? (:diff %)) candidates)]
    (if matched?
      (update complete :result assoc
              :validation :window-compatible
              :candidate-count candidate-count
              :overlapping-write-count overlapping-write-count
              :window window)
      (let [{:keys [expected diff]} (apply min-key #(diff-score (:diff %)) candidates)]
        (assoc complete
               :type :fail
               :error :refresh-window-mismatch
               :result (merge (select-keys (:result complete) [:rows :groups :actual])
                              {:expected expected
                               :diff diff
                               :candidate-count candidate-count
                               :overlapping-write-count overlapping-write-count
                               :window window}))))))

(defn- validate-refresh-op
  [write-pairs op-pair]
  (let [complete (:complete op-pair)]
    (if (and (op/ok? complete)
             (map? (get-in complete [:result :actual])))
      (refresh-window-check op-pair write-pairs)
      complete)))

(defrecord MVLifecycleClient [conn-holder node schema-created?]
  client/Client

  (open! [this test node]
    (stateful/close-conn-holder! conn-holder)
    (let [conn-holder' (atom nil)]
      (stateful/with-reconnect! conn-holder' node test stateful/setup-retryable-error?
        identity)
      (assoc this :conn-holder conn-holder' :node node)))

  (setup! [this test]
    (when (compare-and-set! schema-created? false true)
      (let [ids (map stateful/key-for-process
                     (range (long (max 1 (:concurrency test)))))]
        (stateful/with-reconnect! conn-holder node test stateful/setup-retryable-error?
          #(mv/setup-stateful-schema! % ids))))
    this)

  (invoke! [this test op]
    (condp = (:f op)
      :insert       (stateful/apply-write-with-reconnect! conn-holder node test op)
      :update-value (stateful/apply-write-with-reconnect! conn-holder node test op)
      :move-group   (stateful/apply-write-with-reconnect! conn-holder node test op)
      :delete       (stateful/apply-write-with-reconnect! conn-holder node test op)

      :drop-row-view
      (lifecycle-transition-with-reconnect! conn-holder node test op mv/drop-row-view!)

      :create-row-view
      (lifecycle-transition-with-reconnect! conn-holder node test op mv/create-row-view!)

      :drop-agg-view
      (lifecycle-transition-with-reconnect! conn-holder node test op mv/drop-agg-view!)

      :create-agg-view
      (lifecycle-transition-with-reconnect! conn-holder node test op mv/create-agg-view!)

      :drop-mlog
      (lifecycle-transition-with-reconnect! conn-holder node test op mv/drop-mlog!)

      :create-mlog
      (lifecycle-transition-with-reconnect! conn-holder node test op mv/create-mlog!)

      :refresh-row  (if (:lifecycle-phase op)
                      (lifecycle-refresh-row-with-reconnect! conn-holder node test op)
                      (stateful/refresh-row-with-reconnect! conn-holder node test op))
      :refresh-agg  (if (:lifecycle-phase op)
                      (lifecycle-refresh-agg-with-reconnect! conn-holder node test op)
                      (stateful/refresh-agg-with-reconnect! conn-holder node test op))
      :purge        (stateful/purge-with-reconnect! conn-holder node test op)

      (assoc op :type :fail :error :unknown-op)))

  (teardown! [_ _])

  (close! [_ _]
    (stateful/close-conn-holder! conn-holder)))

(defn checker*
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [pairs                (pair-history history)
            write-pairs          (filter write-pair? pairs)
            validated-by-index   (->> pairs
                                      (filter lifecycle-refresh-pair?)
                                      (map (fn [pair]
                                             [(:index (:complete pair))
                                              (validate-refresh-op write-pairs pair)]))
                                      (into {}))
            validated-op         (fn [op]
                                   (get validated-by-index (:index op) op))
            completed-refresh-purge
            (->> history
                 (filter #(and (contains? (conj refresh-op-fns :purge) (:f %))
                               (stateful/completed-op? %)))
                 (map validated-op))
            refresh-row-ops      (filter #(= :refresh-row (:f %))
                                         completed-refresh-purge)
            refresh-agg-ops      (filter #(= :refresh-agg (:f %))
                                         completed-refresh-purge)
            purge-ops            (filter #(= :purge (:f %))
                                         completed-refresh-purge)
            lifecycle-ops        (->> history
                                      (filter #(and (:lifecycle-phase %)
                                                    (stateful/completed-op? %)))
                                      (map validated-op))
            recent-rp            (vec (take-last 20 completed-refresh-purge))
            write-ops            (filter #(contains? #{:insert :update-value :move-group :delete} (:f %))
                                         history)
            checked-ops          (->> history
                                      (filter #(and (stateful/completed-op? %)
                                                    (or (:lifecycle-phase %)
                                                        (contains? (conj refresh-op-fns :purge) (:f %)))))
                                      (map validated-op))
            failures             (filter op/fail? checked-ops)
            unresolved           (filter #(and (= :info (:type %))
                                               (contains? #{:insert :update-value :move-group :delete} (:f %)))
                                         write-ops)
            lifecycle-complete?  (= (count lifecycle-ops) (count management-ops))
            first-failure        (first failures)
            first-lifecycle-fail (first (filter op/fail? lifecycle-ops))
            final-row-check      (some-> refresh-row-ops last stateful/op-result)
            final-agg-check      (some-> refresh-agg-ops last stateful/op-result)
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
  {:client          (MVLifecycleClient. (atom nil) nil (atom false))
   :generator       (gen/stagger 1/5 (generator))
   :checker         (checker/compose {:mv-lifecycle (checker*)
                                      :timeline     (timeline/html)})
   :final-generator (gen/on #{0}
                            (gen/seq [{:type :invoke, :f :refresh-row, :value :all}
                                      {:type :invoke, :f :refresh-agg}
                                      {:type :invoke, :f :purge}
                                      {:type :invoke, :f :refresh-row, :value :all}
                                      {:type :invoke, :f :refresh-agg}]))})
