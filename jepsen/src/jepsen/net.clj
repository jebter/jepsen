(ns jepsen.net
  "Controls network manipulation.

  TODO: break this up into jepsen.net.proto (polymorphism) and jepsen.net
  (wrapper fns, default args, etc)"
  (:require [dom-top.core :refer [real-pmap]]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.control :refer :all]
            [jepsen.control.net :as control.net]
            [jepsen.net.proto :as p]))

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
