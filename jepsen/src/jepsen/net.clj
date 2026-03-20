(ns jepsen.net
  "Controls network manipulation.

  TODO: break this up into jepsen.net.proto (polymorphism) and jepsen.net
  (wrapper fns, default args, etc)"
  (:require [dom-top.core :refer [real-pmap]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.control :refer :all]
            [jepsen.control.net :as control.net]
            [jepsen.net.proto :as p]
            [jepsen.util :refer [meh]]))

; TODO: move this into jepsen.net.proto
(defprotocol Net
  (drop! [net test src dest] "Drop traffic between nodes src and dest.")
  (heal! [net test]          "End all traffic drops and restores network to fast operation.")
  (slow! [net test]
         [net test opts]
         "Delays network packets with options:

         :mean         (in ms)
         :variance     (in ms)
         :distribution (e.g. :normal)")
  (flaky! [net test]         "Introduces randomized packet loss")
  (fast! [net test]          "Removes packet loss and delays."))

; Top-level API functions
(defn drop-all!
  "Takes a test and a grudge: a map of nodes to collections of nodes they
  should drop messages from, and makes those changes to the test's network."
  [test grudge]
  (let [net (:net test)]
    (if (satisfies? p/PartitionAll net)
      ; Fast path
      (p/drop-all! net test grudge)

      ; Fallback
      (->> grudge
           ; We'll expand {dst [src1 src2]} into ((src1 dst) (src2 dst) ...)
           (mapcat (fn expand [[dst srcs]]
                     (map list srcs (repeat dst))))
           (real-pmap (partial apply drop! net test))
           dorun))))

(def tc "/sbin/tc")

(def partition-nemesis-keys
  #{:partition
    :partition-one
    :partition-pd-leader
    :partition-half
    :partition-ring})

(def netem-nemesis-keys
  #{:netem})

(defn best-effort-net?
  "Whether network rule operations should degrade instead of failing hard when
  the environment lacks NET_ADMIN-like privileges."
  []
  (contains? #{"1" "true" "yes"}
             (some-> (System/getenv "JEPSEN_BEST_EFFORT_NET")
                     str/lower-case)))

(defn permission-denied-net-op?
  [e]
  (let [msg (some-> e .getMessage str/lower-case)]
    (boolean
      (and msg
           (or (re-find #"permission denied" msg)
               (re-find #"operation not permitted" msg))))))

(defn allow-best-effort-net?
  [test]
  (and (best-effort-net?)
       (not (:requires-real-network? test))))

(defn required-network-capabilities
  "Returns the network capabilities that must be available for this test."
  [test]
  (let [nemesis-spec (:nemesis-spec test)]
    (cond-> []
      (some #(get nemesis-spec %) partition-nemesis-keys) (conj :iptables)
      (some #(get nemesis-spec %) netem-nemesis-keys) (conj :tc))))

(defn capability-label
  [capability]
  (case capability
    :iptables "iptables-based partition faults"
    :tc       "tc/netem-based network shaping"
    (name capability)))

(defn probe-network-capability!
  [test capability]
  (case capability
    :iptables (heal! (:net test) test)
    :tc       (fast! (:net test) test)
    (throw (IllegalArgumentException.
             (str "Unknown network capability probe: " capability)))))

(defn real-network-unavailable-error
  [test capability e]
  (let [requested-nemeses (-> (:nemesis-spec test)
                              (dissoc :interval :schedule :long-recovery :failpoints)
                              keys
                              sort
                              vec)]
    (ex-info
      (str "Real network fault injection is required for "
           (pr-str requested-nemeses)
           ", but the testbed lacks permissions for "
           (capability-label capability)
           ". Probe failed with: "
           (.getMessage e))
      {:type                    ::real-network-unavailable
       :capability              capability
       :capability-label        (capability-label capability)
       :nemesis-spec            (:nemesis-spec test)
       :requires-real-network?  (:requires-real-network? test)
       :best-effort-net?        (best-effort-net?)
       :hint                    "Use a NET_ADMIN-capable testbed for partition/netem faults, or skip those nemeses in this environment."}
      e)))

(defn ensure-required-network!
  [test]
  (doseq [capability (or (seq (required-network-capabilities test))
                         [:iptables])]
    (try
      (probe-network-capability! test capability)
      (catch RuntimeException e
        (if (permission-denied-net-op? e)
          (throw (real-network-unavailable-error test capability e))
          (throw e))))))

(defn prepare!
  "Resets network state during setup and fails fast when required network
  privileges are missing."
  [test]
  (if (:requires-real-network? test)
    (ensure-required-network! test)
    (meh (heal! (:net test) test))))

(defmacro with-best-effort-net
  [test & body]
  `(try
     ~@body
     (catch RuntimeException e#
       (if (and (allow-best-effort-net? ~test)
                (permission-denied-net-op? e#))
         (warn "Ignoring network operation without NET_ADMIN:" (.getMessage e#))
         (throw e#)))))

(def noop
  "Does nothing."
  (reify Net
    (drop! [net test src dest])
    (heal! [net test])
    (slow! [net test])
    (slow! [net test opts])
    (flaky! [net test])
    (fast! [net test])))

(def iptables
  "Default iptables (assumes we control everything)."
  (reify Net
    (drop! [net test src dest]
      (with-best-effort-net test
        (on dest (su (exec :iptables :-A :INPUT :-s (control.net/ip src) :-j
                           :DROP :-w)))))

    (heal! [net test]
      (with-best-effort-net test
        (with-test-nodes test
          (su
            (exec :iptables :-F :-w)
            (exec :iptables :-X :-w)))))

    (slow! [net test]
      (with-best-effort-net test
        (with-test-nodes test
          (su (exec tc :qdisc :add :dev :eth0 :root :netem :delay :50ms
                    :10ms :distribution :normal)))))

    (slow! [net test {:keys [mean variance distribution]
                      :or   {mean         50
                             variance     10
                             distribution :normal}}]
      (with-best-effort-net test
        (with-test-nodes test
          (su (exec tc :qdisc :add :dev :eth0 :root :netem :delay
                    (str mean "ms")
                    (str variance "ms")
                    :distribution distribution)))))

    (flaky! [net test]
      (with-best-effort-net test
        (with-test-nodes test
          (su (exec tc :qdisc :add :dev :eth0 :root :netem :loss "20%"
                    "75%")))))

    (fast! [net test]
      (with-best-effort-net test
        (with-test-nodes test
          (try
            (su (exec tc :qdisc :del :dev :eth0 :root))
            (catch RuntimeException e
              (if (re-find #"RTNETLINK answers: No such file or directory"
                           (.getMessage e))
                nil
                (throw e)))))))

    p/PartitionAll
    (drop-all! [net test grudge]
      (with-best-effort-net test
        (on-nodes test
                  (keys grudge)
                  (fn snub [_ node]
                    (su (exec :iptables :-A :INPUT :-s
                              (->> (get grudge node)
                                   (map control.net/ip)
                                   (str/join ","))
                              :-j :DROP :-w))))))))

(def ipfilter
  "IPFilter rules"
  (reify Net
    (drop! [net test src dest]
      (on dest (su (exec :echo :block :in :from src :to :any | :ipf :-f :-))))

    (heal! [net test]
      (with-test-nodes test
        (su (exec :ipf :-Fa))))

    (slow! [net test]
      (with-test-nodes test
        (su (exec :tc :qdisc :add :dev :eth0 :root :netem :delay :50ms
                  :10ms :distribution :normal))))

    (slow! [net test {:keys [mean variance distribution]
                      :or   {mean         50
                             variance     10
                             distribution :normal}}]
      (with-test-nodes test
        (su (exec tc :qdisc :add :dev :eth0 :root :netem :delay
                  (str mean "ms")
                  (str variance "ms")
                  :distribution distribution))))

    (flaky! [net test]
      (with-test-nodes test
        (su (exec :tc :qdisc :add :dev :eth0 :root :netem :loss "20%"
                  "75%"))))

    (fast! [net test]
      (with-test-nodes test
        (su (exec :tc :qdisc :del :dev :eth0 :root))))))
