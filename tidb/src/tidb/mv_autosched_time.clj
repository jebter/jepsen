(ns tidb.mv-autosched-time
  (:refer-clojure :exclude [test])
  (:require [clojure.pprint :refer [pprint]]
            [jepsen
             [checker :as checker]
             [store :as store]]
            [jepsen.checker.timeline :as timeline]
            [knossos.op :as op]
            [tidb.artifact :as artifact]
            [tidb.mv-autosched :as autosched]))

(def analysis-subdir "mv-autosched-time")
(def analysis-path (str analysis-subdir "/analysis.edn"))
(def snapshot-path (str analysis-subdir "/snapshots.edn"))
(def skeleton-warning
  (str "mv-autosched-time is experimental: clock-skew is enabled for manual runs, "
       "but this workload is not yet part of the default gate suite."))

(declare anomaly)

(def residual-clock-offset-budget-ms 5000)
(def quiet-row-agg-change-budget 2)
(def quiet-signature-change-budget 4)

(defn schedule-budget-ms
  []
  (* 1000 (max autosched/row-refresh-seconds
               autosched/agg-refresh-seconds
               autosched/purge-next-seconds)))

(defn convergence-budget-ms
  []
  (* 2 (schedule-budget-ms)))

(defn stability-budget-ms
  []
  (* 3 (schedule-budget-ms)))

(defn purge-budget-ms
  []
  (* 3 1000 autosched/purge-next-seconds))

(defn converged?
  [snapshot]
  (and (:row-equal? snapshot)
       (:agg-equal? snapshot)))

(defn row-agg-signature
  [snapshot]
  [(:row-hash snapshot) (:agg-hash snapshot)])

(defn full-signature
  [snapshot]
  [(:row-hash snapshot) (:agg-hash snapshot) (:log-row-count snapshot)])

(defn ordered-snapshots
  [snapshots]
  (->> snapshots
       (sort-by (juxt :snapshot-at-ms :snapshot-node))
       vec))

(defn snapshots-by-node
  [snapshots]
  (->> (ordered-snapshots snapshots)
       (group-by :snapshot-node)
       (into (sorted-map))))

(defn- first-index
  [pred coll]
  (first (keep-indexed (fn [idx value]
                         (when (pred value)
                           idx))
                       coll)))

(defn- change-count
  [signature-fn snapshots]
  (->> (partition 2 1 snapshots)
       (filter (fn [[a b]]
                 (not= (signature-fn a)
                       (signature-fn b))))
       count))

(defn- stable-pair-second-index
  [snapshots]
  (->> (partition 2 1 snapshots)
       (keep-indexed (fn [idx [a b]]
                       (when (and (converged? a)
                                  (converged? b)
                                  (= (row-agg-signature a)
                                     (row-agg-signature b)))
                         (inc idx))))
       first))

(defn- lag-ms
  [snapshots idx]
  (when (and (seq snapshots) (some? idx))
    (- (:snapshot-at-ms (nth snapshots idx))
       (:snapshot-at-ms (first snapshots)))))

(defn- observed-window-ms
  [snapshots]
  (if (<= (count snapshots) 1)
    0
    (- (:snapshot-at-ms (last snapshots))
       (:snapshot-at-ms (first snapshots)))))

(defn- post-stable-regression
  [snapshots stable-idx]
  (when (some? stable-idx)
    (let [stable-signature (row-agg-signature (nth snapshots stable-idx))]
      (->> (subvec snapshots (inc stable-idx))
           (keep (fn [snapshot]
                   (when (or (not (converged? snapshot))
                             (not= stable-signature (row-agg-signature snapshot)))
                     {:snapshot-at-ms (:snapshot-at-ms snapshot)
                      :snapshot-node  (:snapshot-node snapshot)
                      :row-equal?     (:row-equal? snapshot)
                      :agg-equal?     (:agg-equal? snapshot)
                      :row-hash       (:row-hash snapshot)
                      :agg-hash       (:agg-hash snapshot)})))
           vec
           not-empty))))

(defn snapshot-spacing-ms
  [snapshots]
  (let [times  (map :snapshot-at-ms snapshots)
        deltas (->> (partition 2 1 times)
                    (mapv (fn [[a b]] (- b a))))]
    {:count   (count deltas)
     :min-ms  (when (seq deltas) (apply min deltas))
     :max-ms  (when (seq deltas) (apply max deltas))
     :avg-ms  (when (seq deltas)
                (long (/ (reduce + deltas) (count deltas))))
     :values  deltas}))

(def clock-op-fns #{:check-clock-offsets :reset-clock :strobe-clock :bump-clock})
(def skew-op-fns #{:strobe-clock :bump-clock})

(defn clock-op?
  [op]
  (contains? clock-op-fns (:f op)))

(defn flatten-offsets
  [clock-ops]
  (->> clock-ops
       (map :clock-offsets)
       (filter map?)
       (mapcat seq)))

(defn offset-summary
  [clock-ops]
  (let [pairs             (flatten-offsets clock-ops)
        values            (map second pairs)
        absvals           (map #(Math/abs (double %)) values)
        event-frequencies (frequencies (map :f clock-ops))]
    {:event-count               (count clock-ops)
     :event-frequencies         event-frequencies
     :offset-observation-count  (count values)
     :max-abs-offset-seconds    (when (seq absvals) (apply max absvals))
     :latest-offsets            (when (seq pairs)
                                  (into {} (last (map :clock-offsets clock-ops))))}))

(defn snapshot-offset-summary
  [snapshots]
  (let [pairs   (->> snapshots
                     (keep (fn [snapshot]
                             (when (some? (:db-client-offset-ms snapshot))
                               [(:snapshot-node snapshot)
                                (:db-client-offset-ms snapshot)]))))
        values  (map second pairs)
        absvals (map #(Math/abs (double %)) values)]
    {:observation-count  (count values)
     :max-abs-offset-ms  (when (seq absvals) (apply max absvals))
     :latest-offsets     (when (seq pairs)
                           (reduce (fn [offsets [node value]]
                                     (assoc offsets node value))
                                   {}
                                   pairs))}))

(defn purge-progress-state
  [snapshots]
  (let [counts (->> snapshots
                    (map :log-row-count)
                    (filter some?))]
    (cond
      (empty? counts) :unknown
      (some true? (map (fn [[a b]] (> a b)) (partition 2 1 counts))) :decreased
      (zero? (last counts)) :empty
      :else false)))

(def schedule-components
  {:row-refresh "row refresh"
   :agg-refresh "agg refresh"
   :log-purge   "log purge"})

(defn native-materialized-view-query?
  [query]
  (boolean (and query
                (re-find #"(?i)^SHOW CREATE MATERIALIZED VIEW\b" query))))

(defn first-native-clause-missing-anomaly
  [metadata-summary]
  (some (fn [[component summary]]
          (when-let [event (:first-missing summary)]
            (when (native-materialized-view-query? (:query event))
              (anomaly :schedule-clause-missing
                       :hard
                       (str (get schedule-components component)
                            " metadata lost its START WITH / NEXT clause")
                       {:component component
                        :event event}))))
        metadata-summary))

(defn first-best-effort-clause-missing-warning
  [metadata-summary]
  (some (fn [[component summary]]
          (when-let [event (:first-missing summary)]
            (when-not (native-materialized-view-query? (:query event))
              (anomaly :schedule-clause-unverified
                       :warning
                       (str (get schedule-components component)
                            " schedule metadata did not expose START WITH / NEXT via fallback SHOW CREATE")
                       {:component component
                        :event event}))))
        metadata-summary))

(defn component-observations
  [snapshots component]
  (->> snapshots
       (keep (fn [snapshot]
               (when-let [entry (get-in snapshot [:schedule-metadata component])]
                 {:snapshot-at-ms         (:snapshot-at-ms snapshot)
                  :snapshot-node          (:snapshot-node snapshot)
                  :available?             (:available? entry)
                  :schedule-visible?      (:schedule-visible? entry)
                  :next-literal           (:next-literal entry)
                  :next-matches-expected? (:next-matches-expected? entry)
                  :error                  (:error entry)
                  :query                  (:query entry)
                  :object                 (:object entry)})))
       vec))

(defn- earliest-event
  [events]
  (->> events
       (remove nil?)
       (sort-by (juxt :snapshot-at-ms :snapshot-node))
       first))

(defn- summarize-component-observations
  [label observations]
  (let [observations    (->> observations
                             (sort-by (juxt :snapshot-at-ms :snapshot-node))
                             vec)
        available-count (count (filter :available? observations))
        visible-count   (count (filter :schedule-visible? observations))
        mismatch-count  (count (filter #(false? (:next-matches-expected? %)) observations))]
    {:label                label
     :observation-count    (count observations)
     :available-count      available-count
     :visible-count        visible-count
     :mismatch-count       mismatch-count
     :fully-unavailable?   (and (seq observations)
                                (zero? available-count))
     :ever-visible?        (pos? visible-count)
     :first-available      (first (filter :available? observations))
     :first-missing        (first (filter #(and (:available? %)
                                                (false? (:schedule-visible? %)))
                                          observations))
     :first-mismatch       (first (filter #(false? (:next-matches-expected? %)) observations))
     :first-disappearance  (first (keep (fn [[a b]]
                                          (when (and (:available? a)
                                                     (not (:available? b)))
                                            b))
                                        (partition 2 1 observations)))}))

(defn schedule-metadata-summary
  [snapshots]
  (let [series-by-node (snapshots-by-node snapshots)]
    (into {}
          (for [[component label] schedule-components]
            (let [by-node         (into (sorted-map)
                                        (for [[node node-snapshots] series-by-node]
                                          [node (summarize-component-observations
                                                 label
                                                 (component-observations node-snapshots component))]))
                  node-summaries  (vals by-node)]
              [component {:label                label
                          :observation-count    (reduce + 0 (map :observation-count node-summaries))
                          :available-count      (reduce + 0 (map :available-count node-summaries))
                          :visible-count        (reduce + 0 (map :visible-count node-summaries))
                          :mismatch-count       (reduce + 0 (map :mismatch-count node-summaries))
                          :fully-unavailable?   (and (seq node-summaries)
                                                     (every? :fully-unavailable? node-summaries))
                          :ever-visible?        (boolean (some :ever-visible? node-summaries))
                          :first-available      (earliest-event (map :first-available node-summaries))
                          :first-missing        (earliest-event (map :first-missing node-summaries))
                          :first-mismatch       (earliest-event (map :first-mismatch node-summaries))
                          :first-disappearance  (earliest-event (map :first-disappearance node-summaries))
                          :by-node              by-node}])))))

(defn anomaly
  ([kind severity message]
   (anomaly kind severity message nil))
  ([kind severity message details]
   (cond-> {:kind kind :severity severity :message message}
     details (assoc :details details))))

(defn- first-node-match
  [node-analysis pred]
  (some (fn [[node summary]]
          (when (pred summary)
            [node summary]))
        node-analysis))

(defn- max-some
  [values]
  (let [present (seq (remove nil? values))]
    (when present
      (apply max present))))

(defn- analyze-snapshot-series
  [snapshots]
  (let [snapshots              (vec snapshots)
        converged-snapshots    (filter converged? snapshots)
        first-converged-idx    (first-index converged? snapshots)
        first-stable-idx       (stable-pair-second-index snapshots)
        first-converged-lag-ms (lag-ms snapshots first-converged-idx)
        first-stable-lag-ms    (lag-ms snapshots first-stable-idx)
        quiet-window-ms        (observed-window-ms snapshots)
        row-agg-change-count   (change-count row-agg-signature snapshots)
        signature-change-count (change-count full-signature snapshots)
        purge-state            (purge-progress-state snapshots)
        regression             (post-stable-regression snapshots first-stable-idx)]
    {:snapshot-count           (count snapshots)
     :converged-snapshot-count (count converged-snapshots)
     :first-converged-idx      first-converged-idx
     :first-stable-idx         first-stable-idx
     :first-converged-lag-ms   first-converged-lag-ms
     :first-stable-lag-ms      first-stable-lag-ms
     :quiet-window-ms          quiet-window-ms
     :row-agg-change-count     row-agg-change-count
     :signature-change-count   signature-change-count
     :purge-progress           purge-state
     :regression-events        regression
     :last-snapshot            (last snapshots)}))

(defn- aggregate-purge-progress
  [node-analysis]
  (let [states (map :purge-progress (vals node-analysis))]
    (cond
      (empty? states) :unknown
      (some #{:decreased} states) :decreased
      (some #{:empty} states) :empty
      (every? #{:unknown} states) :unknown
      :else false)))

(defn time-analysis
  [test history snapshots]
  (let [snapshots                  (ordered-snapshots snapshots)
        snapshot-series            (snapshots-by-node snapshots)
        node-analysis              (into (sorted-map)
                                        (for [[node node-snapshots] snapshot-series]
                                          [node (analyze-snapshot-series node-snapshots)]))
        node-summaries             (vals node-analysis)
        clock-ops                  (->> history
                                        (filter clock-op?)
                                        (filter :clock-offsets)
                                        (mapv #(select-keys % [:time :process :type :f :value :clock-offsets])))
        requested?                 (boolean (:clock-skew (or (:nemesis-spec test) {})))
        skew-ops                   (filterv #(contains? skew-op-fns (:f %)) clock-ops)
        reset-ops                  (filterv #(= :reset-clock (:f %)) clock-ops)
        first-converged-lag-ms     (max-some (map :first-converged-lag-ms node-summaries))
        first-stable-lag-ms        (max-some (map :first-stable-lag-ms node-summaries))
        quiet-window-ms            (or (max-some (map :quiet-window-ms node-summaries)) 0)
        row-agg-change-count       (or (max-some (map :row-agg-change-count node-summaries)) 0)
        signature-change-count     (or (max-some (map :signature-change-count node-summaries)) 0)
        purge-state                (aggregate-purge-progress node-analysis)
        residual-offsets           (snapshot-offset-summary snapshots)
        regression                 (some (fn [[node summary]]
                                           (when-let [events (:regression-events summary)]
                                             {:node node
                                              :events events
                                              :first-regression (first events)}))
                                         node-analysis)
        metadata-summary           (schedule-metadata-summary snapshots)
        missing-convergence        (first-node-match node-analysis
                                                    #(and (nil? (:first-converged-idx %))
                                                          (>= (:quiet-window-ms %) (convergence-budget-ms))))
        slow-convergence           (first-node-match node-analysis
                                                    #(and (some? (:first-converged-lag-ms %))
                                                          (> (:first-converged-lag-ms %) (convergence-budget-ms))))
        missing-stability          (first-node-match node-analysis
                                                    #(and (nil? (:first-stable-idx %))
                                                          (>= (:quiet-window-ms %) (stability-budget-ms))))
        slow-stability             (first-node-match node-analysis
                                                    #(and (some? (:first-stable-lag-ms %))
                                                          (> (:first-stable-lag-ms %) (stability-budget-ms))))
        stuck-purge                (first-node-match node-analysis
                                                    #(and (= false (:purge-progress %))
                                                          (>= (:quiet-window-ms %) (purge-budget-ms))))
        regressed-node             (first-node-match node-analysis #(seq (:regression-events %)))
        churned-row-agg            (first-node-match node-analysis
                                                    #(> (:row-agg-change-count %) quiet-row-agg-change-budget))
        churned-signature          (first-node-match node-analysis
                                                    #(> (:signature-change-count %) quiet-signature-change-budget))
        short-quiet-window         (first-node-match node-analysis
                                                    #(< (:quiet-window-ms %) (stability-budget-ms)))
        missing-convergence-anomaly
        (when (and requested? missing-convergence)
          (let [[node summary] missing-convergence]
            (anomaly :no-post-reset-convergence
                     :hard
                     "quiet phase never reached a converged snapshot within the convergence budget"
                     {:node node
                      :budget-ms (convergence-budget-ms)
                      :observed-window-ms (:quiet-window-ms summary)})))
        slow-convergence-anomaly
        (when (and requested? slow-convergence)
          (let [[node summary] slow-convergence]
            (anomaly :slow-post-reset-convergence
                     :hard
                     "first converged snapshot arrived too late after quiet phase start"
                     {:node node
                      :lag-ms (:first-converged-lag-ms summary)
                      :budget-ms (convergence-budget-ms)})))
        missing-stability-anomaly
        (when (and requested? missing-stability)
          (let [[node summary] missing-stability]
            (anomaly :no-post-reset-stability
                     :hard
                     "quiet phase never produced two consecutive equal converged snapshots within the stability budget"
                     {:node node
                      :budget-ms (stability-budget-ms)
                      :observed-window-ms (:quiet-window-ms summary)})))
        slow-stability-anomaly
        (when (and requested? slow-stability)
          (let [[node summary] slow-stability]
            (anomaly :slow-post-reset-stability
                     :hard
                     "stable converged pair arrived too late after quiet phase start"
                     {:node node
                      :lag-ms (:first-stable-lag-ms summary)
                      :budget-ms (stability-budget-ms)})))
        stuck-purge-anomaly
        (when (and requested? stuck-purge)
          (let [[node summary] stuck-purge]
            (anomaly :purge-not-progressing
                     :hard
                     "MLog purge did not visibly progress within the purge budget"
                     {:node node
                      :budget-ms (purge-budget-ms)
                      :observed-window-ms (:quiet-window-ms summary)})))
        regressed-node-anomaly
        (when (and requested? regressed-node)
          (let [[node summary] regressed-node]
            (anomaly :post-stable-regression
                     :hard
                     "row/agg projection regressed after a stable converged pair"
                     {:node node
                      :first-regression (first (:regression-events summary))})))
        schedule-next-mismatch-anomaly
        (some (fn [[component summary]]
                (when-let [event (:first-mismatch summary)]
                  (anomaly :schedule-next-mismatch
                           :hard
                           (str (get schedule-components component)
                                " metadata reported an unexpected NEXT interval")
                           {:component component
                            :event event})))
              metadata-summary)
        schedule-metadata-disappeared-anomaly
        (some (fn [[component summary]]
                (when-let [event (:first-disappearance summary)]
                  (anomaly :schedule-metadata-disappeared
                           :hard
                           (str (get schedule-components component)
                                " metadata became unavailable during quiet phase")
                           {:component component
                            :event event})))
              metadata-summary)
        row-agg-churn-warning
        (when churned-row-agg
          (let [[node summary] churned-row-agg]
            (anomaly :row-agg-churn
                     :warning
                     "row/agg hashes changed unusually often during quiet phase"
                     {:node node
                      :change-count (:row-agg-change-count summary)
                      :budget quiet-row-agg-change-budget})))
        excessive-quiet-churn-warning
        (when churned-signature
          (let [[node summary] churned-signature]
            (anomaly :excessive-quiet-churn
                     :warning
                     "quiet phase produced more state transitions than expected"
                     {:node node
                      :change-count (:signature-change-count summary)
                      :budget quiet-signature-change-budget})))
        short-quiet-window-warning
        (when (and requested? short-quiet-window)
          (let [[node summary] short-quiet-window]
            (anomaly :short-quiet-window
                     :warning
                     "observed quiet window was shorter than the configured stability budget"
                     {:node node
                      :observed-window-ms (:quiet-window-ms summary)
                      :budget-ms (stability-budget-ms)})))
        schedule-metadata-unavailable-warning
        (when (some (fn [[_ summary]]
                      (:fully-unavailable? summary))
                    metadata-summary)
          (anomaly :schedule-metadata-unavailable
                   :warning
                   "some schedule metadata could not be read via SHOW CREATE during quiet phase"
                   {:metadata-summary metadata-summary}))
        clause-unverified-warning  (first-best-effort-clause-missing-warning metadata-summary)
        hard-anomalies             (cond-> []
                                     (and requested? (empty? clock-ops))
                                     (conj (anomaly :missing-clock-events
                                                    :hard
                                                    "clock-skew was requested but no clock events were recorded"))

                                     (and requested? (empty? skew-ops))
                                     (conj (anomaly :missing-skew-injection
                                                    :hard
                                                    "clock-skew was requested but no bump/strobe operation completed"))

                                     (and requested? (empty? reset-ops))
                                     (conj (anomaly :missing-clock-reset
                                                    :hard
                                                    "clock-skew was requested but no reset-clock operation completed"))

                                     (and requested?
                                          (some? (:max-abs-offset-ms residual-offsets))
                                          (> (:max-abs-offset-ms residual-offsets)
                                             residual-clock-offset-budget-ms))
                                     (conj (anomaly :residual-clock-skew
                                                    :hard
                                                    "DB wall clock still diverged from the client after reset"
                                                    {:max-abs-offset-ms (:max-abs-offset-ms residual-offsets)
                                                     :budget-ms residual-clock-offset-budget-ms}))

                                     missing-convergence-anomaly
                                     (conj missing-convergence-anomaly)

                                     slow-convergence-anomaly
                                     (conj slow-convergence-anomaly)

                                     missing-stability-anomaly
                                     (conj missing-stability-anomaly)

                                     slow-stability-anomaly
                                     (conj slow-stability-anomaly)

                                     stuck-purge-anomaly
                                     (conj stuck-purge-anomaly)

                                     regressed-node-anomaly
                                     (conj regressed-node-anomaly)

                                     (first-native-clause-missing-anomaly metadata-summary)
                                     (conj (first-native-clause-missing-anomaly metadata-summary))

                                     schedule-next-mismatch-anomaly
                                     (conj schedule-next-mismatch-anomaly)

                                     schedule-metadata-disappeared-anomaly
                                     (conj schedule-metadata-disappeared-anomaly))
        warnings                   (cond-> []
                                     row-agg-churn-warning
                                     (conj row-agg-churn-warning)

                                     excessive-quiet-churn-warning
                                     (conj excessive-quiet-churn-warning)

                                     short-quiet-window-warning
                                     (conj short-quiet-window-warning)

                                     schedule-metadata-unavailable-warning
                                     (conj schedule-metadata-unavailable-warning)

                                     clause-unverified-warning
                                     (conj clause-unverified-warning))]
    {:warning                  skeleton-warning
     :clock-skew-supported?    true
     :clock-skew-requested?    requested?
     :snapshot-count           (count snapshots)
     :converged-snapshot-count (reduce + 0 (map :converged-snapshot-count node-summaries))
     :quiet-window-ms          quiet-window-ms
     :schedule-budgets-ms      {:convergence (convergence-budget-ms)
                                :stability   (stability-budget-ms)
                                :purge       (purge-budget-ms)
                                :residual-clock-offset residual-clock-offset-budget-ms}
     :first-converged-lag-ms   first-converged-lag-ms
     :first-stable-lag-ms      first-stable-lag-ms
     :row-agg-change-count     row-agg-change-count
     :signature-change-count   signature-change-count
     :snapshot-spacing-ms      (snapshot-spacing-ms snapshots)
     :snapshot-analysis-by-node node-analysis
     :snapshot-offset-summary  residual-offsets
     :schedule-metadata        metadata-summary
     :clock-events             clock-ops
     :clock-summary            (offset-summary clock-ops)
     :purge-progress           purge-state
     :regression-events        regression
     :anomalies                hard-anomalies
     :warnings                 warnings
     :last-snapshot            (last snapshots)}))

(defn checker*
  []
  (reify checker/Checker
    (check [_ test history _]
      (let [snapshot-values (->> history
                                 (filter #(and (= :snapshot (:f %))
                                               (op/ok? %)))
                                 (mapv :value))
            analysis        (time-analysis test history snapshot-values)
            anomalies       (:anomalies analysis)
            warnings        (:warnings analysis)]
        (let [summary {:valid?                   (empty? anomalies)
                       :experimental?            true
                       :warning                  skeleton-warning
                       :clock-skew-supported?    true
                       :clock-skew-requested?    (:clock-skew-requested? analysis)
                       :snapshot-count           (:snapshot-count analysis)
                       :converged-snapshot-count (:converged-snapshot-count analysis)
                       :quiet-window-ms          (:quiet-window-ms analysis)
                       :first-converged-lag-ms   (:first-converged-lag-ms analysis)
                       :first-stable-lag-ms      (:first-stable-lag-ms analysis)
                       :purge-progress           (:purge-progress analysis)
                       :anomaly-count            (count anomalies)
                       :warning-count            (count warnings)
                       :first-anomaly            (first anomalies)
                       :first-warning            (first warnings)
                       :analysis-path            analysis-path
                       :analysis-json-path       "mv-autosched-time/analysis.json"
                       :snapshot-path            snapshot-path
                       :snapshot-json-path       "mv-autosched-time/snapshots.json"
                       :summary-path             "mv-autosched-time/summary.edn"
                       :summary-json-path        "mv-autosched-time/summary.json"}]
          (when (and (:name test) (:start-time test))
            (artifact/write-edn+json! test [analysis-subdir "snapshots.edn"] (ordered-snapshots snapshot-values))
            (artifact/write-edn+json! test [analysis-subdir "analysis.edn"] analysis)
            (artifact/write-edn+json! test [analysis-subdir "summary.edn"] summary))
          summary)))))

(defn workload
  [opts]
  (let [base (autosched/workload opts)]
    {:client          (:client base)
     :generator       (:generator base)
     :final-generator (:final-generator base)
     :checker         (checker/compose {:mv-autosched      (autosched/checker*)
                                        :mv-autosched-time (checker*)
                                        :timeline          (timeline/html)})}))
