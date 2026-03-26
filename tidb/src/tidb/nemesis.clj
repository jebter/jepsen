(ns tidb.nemesis
  "Nemeses for TiDB"
  (:require [jepsen
             [client :as client]
             [control :as c]
             [nemesis :as nemesis]
             [net :as net]
             [generator :as gen]
             [util :as util :refer [letr]]]
            [jepsen.control.util :as cu]
            [jepsen.nemesis.time :as nt]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [tidb.db :as db]
            [tidb.util :as tu]
            [clojure.tools.logging :refer :all]
            [slingshot.slingshot :refer [try+]]))

(defn process-nemesis
  "A nemesis that can pause, resume, start, stop, and kill tidb, tikv, and pd."
  []
  (let [recovery-targets (atom {})]
    (letfn [(recovery-op [f]
              (case f
                (:kill-pd :stop-pd) :start-pd
                (:kill-kv :stop-kv) :start-kv
                (:kill-db :stop-db) :start-db
                :pause-pd           :resume-pd
                :pause-kv           :resume-kv
                :pause-db           :resume-db
                nil))
            (recovery-op? [f]
              (contains? #{:start-pd :start-kv :start-db
                           :resume-pd :resume-kv :resume-db}
                         f))
            (merge-targets [existing nodes]
              (->> (concat (or existing []) nodes)
                   distinct
                   vec))
            (remaining-targets [existing nodes]
              (let [remaining (vec (remove (set nodes) (or existing [])))]
                (when (seq remaining)
                  remaining)))
            (clear-recovery-targets [f nodes]
              (swap! recovery-targets
                     (fn [targets]
                       (let [remaining (remaining-targets (get targets f) nodes)]
                         (cond-> (dissoc targets f)
                           (seq remaining) (assoc f remaining))))))
            (choose-targets [test op]
              (let [nodes (:nodes test)]
                (vec
                 (cond
                   (:value op)
                   (:value op)

                   (recovery-op? (:f op))
                   (or (get @recovery-targets (:f op)) [])

                   :else
                   (case (:f op)
                     ; Process faults in the default suite are single-fault
                     ; cases, so select one target node only.
                     (:kill-pd :kill-kv :kill-db
                      :stop-pd :stop-kv :stop-db
                      :pause-pd :pause-kv :pause-db)
                     (take 1 (shuffle nodes))

                     nodes)))))]
      (reify nemesis/Nemesis
        (setup! [this test] this)

        (invoke! [this test op]
          (let [nodes       (choose-targets test op)
                recovery-fn (recovery-op (:f op))
                result      (assoc op :value
                                   (if (seq nodes)
                                     (c/on-nodes test nodes
                                                 (fn [test node]
                                                   (case (:f op)
                                                     :start-pd  (db/start-pd! test node)
                                                     :start-kv  (db/start-kv! test node)
                                                     :start-db  (db/start-db! test node)
                                                     :kill-pd   (db/stop-pd!  test node)
                                                     :kill-kv   (db/stop-kv!  test node)
                                                     :kill-db   (db/stop-db!  test node)
                                                     :stop-pd   (cu/signal! db/pd-bin :TERM)
                                                     :stop-kv   (cu/signal! db/kv-bin :TERM)
                                                     :stop-db   (cu/signal! db/db-bin :TERM)
                                                     :pause-pd  (cu/signal! db/pd-bin :STOP)
                                                     :pause-kv  (cu/signal! db/kv-bin :STOP)
                                                     :pause-db  (cu/signal! db/db-bin :STOP)
                                                     :resume-pd (cu/signal! db/pd-bin :CONT)
                                                     :resume-kv (cu/signal! db/kv-bin :CONT)
                                                     :resume-db (cu/signal! db/db-bin :CONT))))
                                     {}))]
            (cond
              recovery-fn
              (do (swap! recovery-targets update recovery-fn merge-targets nodes)
                  result)

              (recovery-op? (:f op))
              (do (clear-recovery-targets (:f op) nodes)
                  result)

              :else
              result)))

        (teardown! [this test])))))

(defn schedule-nemesis
  "A nemesis that can add stress test schedulers, shuffle-leader, shuffle-region
  and random-merge."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      ; We only need a node that has the pd-ctl utility.
      (let [nodes  (take 1 (util/random-nonempty-subset (:nodes test)))
            pd-ctl (fn [& cmds]
                      ; Execute a pd-ctl command.
                      (try (c/exec :echo cmds :| (str db/tidb-bin-dir "/" db/pdctl-bin))
                        (catch RuntimeException e
                          (info "fail to " cmds))))]
        (assoc op :value
          (c/on-nodes test nodes
             (fn [test node]
               (case (:f op)
                 :shuffle-leader
                   (pd-ctl :sched :add :shuffle-leader-scheduler)
                 :shuffle-region
                   (pd-ctl :sched :add :shuffle-region-scheduler)
                 :random-merge
                   (pd-ctl :sched :add :random-merge-scheduler)
                 :del-shuffle-leader
                   (pd-ctl :sched :remove :shuffle-leader-scheduler)
                 :del-shuffle-region
                   (pd-ctl :sched :remove :shuffle-region-scheduler)
                 :del-random-merge
                   (pd-ctl :sched :remove :random-merge-scheduler)))))))

    (teardown! [this test])))

(defn failpoint-nemesis
  [failpoints]
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [enable (fn [node]
                     [node (doall
                            (map #(try
                                    (apply tu/fail-enable! node %)
                                    [% :ok]
                                    (catch Exception _
                                      [% :fail]))
                                 (map #(cons (case (name (first %)) "tidb" 10080 "tikv" 20180) (rest %))
                                      (util/random-nonempty-subset failpoints))))])
            disable (fn [node]
                      [node (doall
                             (map #(try
                                     (apply tu/fail-disable! node %)
                                     [% :ok]
                                     (catch Exception _
                                       [% :fail]))
                                  (->> (concat
                                        (map #(cons 10080 [(first %)]) (try (tu/fail-list node 10080) (catch Exception _ nil)))
                                        (map #(cons 20180 [(first %)]) (try (tu/fail-list node 20180) (catch Exception _ nil))))
                                       (filter #(not (str/includes? (second %) "server/enableTestAPI"))))))])]
        (assoc op :value
               (case (:f op)
                 :enable-failpoint
                 (doall (pmap enable (util/random-nonempty-subset (:nodes test))))
                 :disable-failpoint
                 (doall (pmap disable (:nodes test)))))))

    (teardown! [this test])))

(defn netem-delay
  [test node]
  (c/su (c/exec "/sbin/tc" :qdisc :add :dev :eth0 :root :netem
                :delay :5ms :10ms :distribution :normal)))

(defn netem-loss
  [test node]
  (c/su (c/exec "/sbin/tc" :qdisc :add :dev :eth0 :root :netem
                :loss "0.05%")))

(defn netem-noop
  [test node]
  (warn "netem-type is unspecified"))

(def netem-types #{netem-delay netem-loss netem-noop})

(defn netem-nemesis
  [f]
  (reify nemesis/Nemesis
    (setup! [this test] this)
    (invoke! [this test op]
      (assoc op :value
             (case (:f op)
               :start-netem
               (c/on-nodes test (util/random-nonempty-subset (:nodes test)) f)
               :stop-netem
               (c/on-nodes
                test
                (fn [test node]
                  (try
                    (c/su (c/exec "/sbin/tc" :qdisc :del :dev :eth0 :root))
                    (catch Exception e
                      (if (or
                           (re-find #"RTNETLINK answers: No such file or directory" (.getMessage e))
                           (re-find #"Cannot delete qdisc with handle of zero" (.getMessage e)))
                        nil (throw e)))))))))
    (teardown! [this test])))

; (defn slow-primary-nemesis
;   "A nemesis for creating slow, isolated primaries."
;   []
;   (reify nemesis/Nemesis
;     (setup! [this test] this)
;
;     (invoke! [this test op]
;       (try+
;         ; Figure out who we're going to slow down
;         (let [contact     (first (:nodes test))
;               members     (:members (db/pd-members contact))
;               slow-leader (rand-nth members)
;               slow-node   (->> test db/tidb-map
;                                (keep (fn [[node m]]
;                                        (when (= (:name slow-leader) (:pd m))
;                                          node)))
;                                first)]
;           (info :members members)
;           (info :slow-leader slow-node slow-leader)
;
;           ; Slow down slow-leader and make sure other nodes are all running
;           ; normally.
;           (c/on-nodes test
;                       (fn [test node]
;                         ; (db/setup-faketime! db/pd-bin (if (= node slow-node)
;                         ;                                0.1
;                         ;                                1))
;                         (db/stop-pd! test node)
;                         (db/start-pd! test node)))
;
;           ; Transfer leadership to slow node
;           (info :leader (db/await-http (db/pd-leader contact)))
;           (db/pd-transfer-leader! contact slow-leader)
;           (info :leader' (db/await-http (db/pd-leader contact)))
;
;           ; Isolate slow node
;           (let [fast-nodes  (shuffle (remove #{slow-node} (:nodes test)))
;                 nodes       (cons slow-node fast-nodes)
;                 components  (nemesis/bisect nodes)
;                 grudge      (nemesis/complete-grudge components)]
;             (info :partitioning components)
;             (net/drop-all! test grudge)
;
;             ; Report on transition
;             (dotimes [i 30]
;               (info :leader (db/await-http
;                               (info "asking" (last nodes) "for current leader")
;                               (db/pd-leader (last nodes))))
;               (Thread/sleep 100))
;
;             (info :final-leader (db/await-http (db/pd-leader (last nodes)))))
;
;           (assoc op :value :done))
;         (catch [:status 503] e
;           (assoc op
;                  :type  :info
;                  :value :failed
;                  :error (dissoc e :http-client)))))
;
;     (teardown! [this test])))

(defn full-nemesis
  "Merges together all nemeses"
  [n]
  (nemesis/compose
    {#{:start-pd  :start-kv  :start-db
       :kill-pd   :kill-kv   :kill-db
       :stop-pd   :stop-kv   :stop-db
       :pause-pd  :pause-kv  :pause-db
       :resume-pd :resume-kv :resume-db}    (process-nemesis)
     #{:shuffle-leader  :del-shuffle-leader
       :shuffle-region  :del-shuffle-region
       :random-merge    :del-random-merge}  (schedule-nemesis)
     #{:enable-failpoint :disable-failpoint} (failpoint-nemesis (:failpoints n))
     #{:start-netem :stop-netem} (netem-nemesis (:netem-type n netem-noop))
     ; #{:slow-primary}                       (slow-primary-nemesis)
     {:start-partition :start
      :stop-partition  :stop}               (nemesis/partitioner nil)
     {:reset-clock          :reset
      :strobe-clock         :strobe
      :check-clock-offsets  :check-offsets
      :bump-clock           :bump}          (nt/clock-nemesis)
     }))

; Generators

(defn op
  "Shorthand for constructing a nemesis op"
  ([f]
   (op f nil))
  ([f v]
   {:type :info, :f f, :value v})
  ([f v & args]
   (apply assoc (op f v) args)))

(defn partition-one-gen
  "A generator for a partition that isolates one node."
  [test process]
  (op :start-partition
     (->> test :nodes nemesis/split-one nemesis/complete-grudge)
     :partition-type :single-node))

(defn partition-failure-detail
  [failure]
  (cond
    (map? failure)
    failure

    (instance? Throwable failure)
    (cond-> {:class (.getName (class failure))}
      (.getMessage ^Throwable failure)
      (assoc :message (.getMessage ^Throwable failure)))

    :else
    {:value (pr-str failure)}))

(defn partition-pd-leader-fallback-op
  [test process reason & [failure]]
  (let [detail   (some-> failure partition-failure-detail)
        log-data (cond-> {:requested-partition-type :pd-leader
                          :partition-fallback      :single-node
                          :reason                  reason}
                   detail (assoc :detail detail))]
    (warn "Unable to construct pd-leader partition nemesis op; falling back to single-node partition"
          log-data)
    (cond-> (assoc (partition-one-gen test process)
                   :requested-partition-type :pd-leader
                   :partition-fallback? true
                   :partition-fallback-reason reason)
      detail (assoc :partition-fallback-detail detail))))

(defn partition-pd-leader-gen
  "A generator for a partition that isolates the current PD leader in a
  minority."
  [test process]
  (try+
    (if-let [leader (db/await-http
                      (db/pd-leader-node test (rand-nth (:nodes test))))]
      (let [followers  (shuffle (remove #{leader} (:nodes test)))
            nodes      (cons leader followers)
            components (split-at 1 nodes) ; Maybe later rand(n/2+1?)
            grudge     (nemesis/complete-grudge components)]
        (op :start-partition, grudge, :partition-type :pd-leader))
      (partition-pd-leader-fallback-op test process :leader-unresolved))
    (catch Object e
      (partition-pd-leader-fallback-op test process :leader-resolution-error e))))

(defn partition-half-gen
  "A generator for a partition that cuts the network in half."
  [test process]
  (op :start-partition
      (->> test :nodes shuffle nemesis/bisect nemesis/complete-grudge)
      :partition-type :half))

(defn partition-ring-gen
  "A generator for a partition that creates overlapping majority rings"
  [test process]
  (op :start-partition
      (->> test :nodes nemesis/majorities-ring)
      :partition-type :ring))

(defn clock-gen
  "A mixture of clock operations."
  []
  (->> (nt/clock-gen)
       (gen/f-map {:check-offsets :check-clock-offsets
                   :reset         :reset-clock
                   :strobe        :strobe-clock
                   :bump          :bump-clock})))

(defn flip-flop
  "Switches between ops from two generators: a, b, a, b, ..."
  [a b]
  (gen/seq (cycle [a b])))

(defn opt-mix
  "Given a nemesis map n, and a map of options to generators to use if that
  option is present in n, constructs a mix of generators for those options. If
  no options match, returns `nil`."
  [n possible-gens]
  (let [gens (reduce (fn [gens [option gen]]
                       (if (option n)
                         (conj gens gen)
                         gens))
                     []
                     possible-gens)]
    (when (seq gens)
      (gen/mix gens))))

(defn mixed-generator
  "Takes a nemesis options map `n`, and constructs a generator for all nemesis
  operations. This generator is used during normal nemesis operations."
  [n]
  ; Shorthand: we're going to have a bunch of flip-flops with various types of
  ; failure conditions and a single recovery.
  (let [o (fn [possible-gens recovery]
            ; We return nil when mix does to avoid generating flip flops when
            ; *no* options are present in the nemesis opts.
            (when-let [mix (opt-mix n possible-gens)]
              (flip-flop mix recovery)))]

    ; Mix together our different types of process crashes, partitions, and
    ; clock skews.
    (->> [(o {:kill-pd (op :kill-pd)}
             (op :start-pd))
          (o {:kill-kv (op :kill-kv)}
             (op :start-kv))
          (o {:kill-db (op :kill-db)}
             (op :start-db))
          (o {:stop-pd (op :stop-pd)}
             (op :start-pd))
          (o {:stop-kv (op :stop-kv)}
             (op :start-kv))
          (o {:stop-db (op :stop-db)}
             (op :start-db))
          (o {:pause-pd (op :pause-pd)}
             (op :resume-pd))
          (o {:pause-kv (op :pause-kv)}
             (op :resume-kv))
          (o {:pause-db (op :pause-db)}
             (op :resume-db))
          (o {:shuffle-leader (op :shuffle-leader)}
             (op :del-shuffle-leader))
          (o {:shuffle-region (op :shuffle-region)}
             (op :del-shuffle-region))
          (o {:random-merge (op :random-merge)}
             (op :del-random-merge))
          (o {:partition-one        partition-one-gen
              :partition-pd-leader  partition-pd-leader-gen
              :partition-half       partition-half-gen
              :partition-ring       partition-ring-gen}
             (op :stop-partition))
          (o {:enable-failpoint (op :enable-failpoint)}
             (op :disable-failpoint))
          (o {:start-netem (op :start-netem)}
             (op :stop-netem))
          (opt-mix n {:clock-skew (clock-gen)})
          ]
         ; For all options relevant for this nemesis, mix them together
         (remove nil?)
         gen/mix
         ; Introduce either random or fixed delays between ops
         ((case (:schedule n)
            (nil :random)    gen/stagger
            :fixed           gen/delay-til)
          (:interval n)))))

(defn final-generator
  "Takes a nemesis options map `n`, and constructs a generator to stop all
  problems. This generator is called at the end of a test, before final client
  operations."
  [n]
  (->> (cond-> []
         (:clock-skew n)      (conj (fn [test _]
                                      (op :reset-clock (:nodes test))))
         (:pause-pd n)        (conj :resume-pd)
         (:pause-kv n)        (conj :resume-kv)
         (:pause-db n)        (conj :resume-db)
         (:kill-pd n)         (conj :start-pd)
         (:kill-kv n)         (conj :start-kv)
         (:kill-db n)         (conj :start-db)
         (:stop-pd n)         (conj :start-pd)
         (:stop-kv n)         (conj :start-kv)
         (:stop-db n)         (conj :start-db)
         (:shuffle-leader n)  (conj :del-shuffle-leader)
         (:shuffle-region n)  (conj :del-shuffle-region)
         (:random-merge n)    (conj :del-random-merge)
         (:start-netem n)     (conj :stop-netem)

         (:enable-failpoint n)
         (conj :disable-failpoint)
         (some n [:partition-one :partition-pd-leader
                  :partition-half :partition-ring])
         (conj :stop-partition))
       (map #(if (keyword? %)
               (op %)
               %))
       gen/seq))

(defn restart-kv-without-pd-generator
  "A special generator which pauses all PD nodes, restarts all KV nodes, waits
  a bit, and unpauses PD; the cluster should recover, but a finite retry loop
  causes it to fail."
  []
  (gen/seq [(gen/sleep 10)
            (fn [test _] {:type :info, :f :kill-kv,  :value (:nodes test)})
            (fn [test _] {:type :info, :f :pause-pd, :value (:nodes test)})
            (op :start-kv)
            (gen/sleep 70)
            (op :resume-pd)]))

; (defn slow-primary-generator
;   "A special generator which tries to create a situation in which a primary,
;   running slower than the rest of the cluster, is isolated from a majority
;   component of the cluster, which elects a new, faster primary. Because the old
;   primary's clock runs slow, we expect that the slow node may fail to step down
;   before the new primary comes to power, allowing the two to issue timestamps
;   concurrently."
;   []
;   ; First, pick a node to be our slow primary.
;   ; Force that node to
;   ; run at speed 2.
;   ; Make that node run at speed 2
;   ; Force that node to be the primary by...
;     ; Restarting every other node at speed 1
;   ; Force that node to be slow *and* the leader by...
;     ; Restarting every other node at speed 3
;   ; Isolate that node into a minority partition
;   (->> [{:type :info, :f :slow-primary}
;         (gen/sleep 30)
;         {:type :info, :f :stop-partition}
;         (gen/sleep 30)]
;        cycle
;        gen/seq))

(defn full-generator
  "Takes a nemesis options map `n`. If `n` has a :long-recovery option, builds
  a generator which alternates between faults (mixed-generator) and long
  recovery windows (final-generator). Otherwise, just emits faults from
  mixed-generator, or whatever special-case generator we choose."
  [n]
  (cond (:restart-kv-without-pd n)
        (restart-kv-without-pd-generator)

        ; (:slow-primary n)
        ; (slow-primary-generator)

        (:long-recovery n)
        (let [mix     #(gen/time-limit 120 (mixed-generator n))
              recover #(gen/phases (final-generator n)
                                   (gen/sleep 60))]
          (gen/seq-all (interleave (repeatedly mix)
                                   (repeatedly recover))))

        true
        (mixed-generator n)))

(defn expand-options
  "We support shorthand options in nemesis maps, like :kill, which expands to
  :kill-pd, :kill-kv, and :kill-db. This function expands those."
  [n]
  (cond-> n
    (:kill n) (assoc :kill-pd true
                     :kill-kv true
										 :kill-db true)
    (:stop n) (assoc :stop-pd true
                     :stop-kv true
                     :stop-db true)
    (:pause n) (assoc :pause-pd true
                      :pause-kv true
                      :pause-db true)
    (:schedules n) (assoc :shuffle-leader true
                          :shuffle-region true
                          :random-merge true)
    (:partition n) (assoc :partition-one        true
                          :partition-pd-leader  true
                          :partition-half      true
                          :partition-ring      true)
    (:failpoint n) (assoc :enable-failpoint true)
    (:netem n)     (assoc :start-netem true)))

(defn nemesis
  "Composite nemesis and generator, given test options."
  [opts]
  (let [n (expand-options (:nemesis opts))]
    {:nemesis         (full-nemesis n)
     :generator       (full-generator n)
     :final-generator (final-generator n)}))
