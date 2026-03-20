(ns tidb.nemesis-test
  (:require [clojure.test :refer :all]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.nemesis :as jn]
            [slingshot.slingshot :refer [throw+ try+]]
            [tidb.db :as db]
            [tidb.nemesis :as nemesis]))

(deftest process-nemesis-single-node-process-faults
  (let [test    {:nodes [:node-0 :node-1 :node-2 :node-3 :node-4]}
        targets (atom nil)
        n       (nemesis/process-nemesis)]
    (with-redefs [shuffle (fn [xs] xs)
                  c/on-nodes (fn [_ nodes f]
                               (reset! targets (vec nodes))
                               (mapv #(f test %) nodes))
                  cu/signal! (fn [_ _] :sent)]
      (jn/invoke! n test {:type :info :f :stop-kv})
      (is (= [:node-0] @targets)))))

(deftest process-nemesis-recovery-without-recorded-targets-no-op
  (let [test    {:nodes [:node-0 :node-1 :node-2 :node-3 :node-4]}
        targets (atom nil)
        n       (nemesis/process-nemesis)]
    (with-redefs [c/on-nodes (fn [_ nodes f]
                               (reset! targets (vec nodes))
                               (mapv #(f test %) nodes))
                  db/start-kv! (fn [_ node] [:started node])]
      (jn/invoke! n test {:type :info :f :start-kv})
      (is (nil? @targets)))))

(deftest process-nemesis-recovery-targets-recorded-fault-node
  (let [test    {:nodes [:node-0 :node-1 :node-2 :node-3 :node-4]}
        targets (atom [])
        n       (nemesis/process-nemesis)]
    (with-redefs [shuffle (fn [xs] xs)
                  c/on-nodes (fn [_ nodes f]
                               (swap! targets conj (vec nodes))
                               (mapv #(f test %) nodes))
                  cu/signal! (fn [_ _] :sent)
                  db/start-kv! (fn [_ node] [:started node])]
      (jn/invoke! n test {:type :info :f :stop-kv})
      (jn/invoke! n test {:type :info :f :start-kv})
      (is (= [[:node-0] [:node-0]] @targets)))))

(deftest process-nemesis-explicit-targets-take-precedence
  (let [test    {:nodes [:node-0 :node-1 :node-2 :node-3 :node-4]}
        targets (atom nil)
        n       (nemesis/process-nemesis)]
    (with-redefs [shuffle (fn [xs] xs)
                  c/on-nodes (fn [_ nodes f]
                               (reset! targets (vec nodes))
                               (mapv #(f test %) nodes))
                  cu/signal! (fn [_ _] :sent)]
      (jn/invoke! n test {:type :info :f :stop-kv :value [:node-3]})
      (is (= [:node-3] @targets)))))

(deftest process-nemesis-final-healing-does-not-restart-already-recovered-nodes
  (let [test    {:nodes [:node-0 :node-1 :node-2 :node-3 :node-4]}
        targets (atom [])
        n       (nemesis/process-nemesis)]
    (with-redefs [shuffle (fn [xs] xs)
                  c/on-nodes (fn [_ nodes f]
                               (swap! targets conj (vec nodes))
                               (mapv #(f test %) nodes))
                  cu/signal! (fn [_ _] :sent)
                  db/start-kv! (fn [_ node] [:started node])]
      (jn/invoke! n test {:type :info :f :stop-kv})
      (jn/invoke! n test {:type :info :f :start-kv})
      (jn/invoke! n test {:type :info :f :start-kv})
      (is (= [[:node-0] [:node-0]] @targets)))))

(deftest process-nemesis-explicit-recovery-clears-only-recovered-nodes
  (let [test    {:nodes [:node-0 :node-1 :node-2 :node-3 :node-4]}
        targets (atom [])
        n       (nemesis/process-nemesis)]
    (with-redefs [c/on-nodes (fn [_ nodes f]
                               (swap! targets conj (vec nodes))
                               (mapv #(f test %) nodes))
                  cu/signal! (fn [_ _] :sent)
                  db/start-kv! (fn [_ node] [:started node])]
      (jn/invoke! n test {:type :info :f :stop-kv :value [:node-1 :node-2]})
      (jn/invoke! n test {:type :info :f :start-kv :value [:node-1]})
      (jn/invoke! n test {:type :info :f :start-kv})
      (is (= [[:node-1 :node-2] [:node-1] [:node-2]] @targets)))))

(deftest partition-pd-leader-gen-fails-when-leader-unresolved
  (let [test {:nodes [:node-0 :node-1 :node-2 :node-3 :node-4]}]
    (with-redefs [rand-nth first
                  db/pd-leader-node (fn [_ _] nil)]
      (try+
        (nemesis/partition-pd-leader-gen test nil)
        (is false "expected pd-leader partition generation to fail")
        (catch [:type ::nemesis/pd-leader-partition-unavailable] e
          (is (= :pd-leader (:requested-partition-type e)))
          (is (= :leader-unresolved (:reason e))))))))

(deftest partition-pd-leader-gen-fails-when-leader-resolution-errors
  (let [test {:nodes [:node-0 :node-1 :node-2 :node-3 :node-4]}]
    (with-redefs [rand-nth first
                  db/pd-leader-node (fn [_ _] (throw+ {:status 404 :type :test-error}))]
      (try+
        (nemesis/partition-pd-leader-gen test nil)
        (is false "expected pd-leader partition generation to fail")
        (catch [:type ::nemesis/pd-leader-partition-unavailable] e
          (is (= :pd-leader (:requested-partition-type e)))
          (is (map? (:reason e)))
          (is (= {:status 404 :type :test-error}
                 (:leader-resolution-error (:reason e)))))))))
