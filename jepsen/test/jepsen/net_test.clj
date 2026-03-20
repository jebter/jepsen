(ns jepsen.net-test
  (:require [clojure.test :refer :all]
            [jepsen.net :as net]))

(deftest with-best-effort-net-ignores-permission-errors-only-when-network-is-optional
  (with-redefs [net/best-effort-net? (constantly true)]
    (is (nil?
         (net/with-best-effort-net
           {}
           (throw (RuntimeException. "Permission denied")))))
    (is (thrown-with-msg?
          RuntimeException
          #"Permission denied"
          (net/with-best-effort-net
            {:requires-real-network? true}
            (throw (RuntimeException. "Permission denied")))))))

(deftest required-network-capabilities-follow-nemesis-spec
  (is (= [:iptables]
         (net/required-network-capabilities
           {:nemesis-spec {:partition true}})))
  (is (= [:tc]
         (net/required-network-capabilities
           {:nemesis-spec {:netem true}})))
  (is (= [:iptables :tc]
         (net/required-network-capabilities
           {:nemesis-spec {:partition true :netem true}}))))

(deftest prepare-ignores-permission-errors-when-real-network-is-optional
  (with-redefs [net/heal! (fn [_ _]
                            (throw (RuntimeException. "Permission denied")))]
    (is (instance? RuntimeException
                   (net/prepare! {:net ::fake-net})))))

(deftest prepare-fails-fast-with-environment-error-when-iptables-are-required
  (with-redefs [net/heal! (fn [_ _]
                            (throw (RuntimeException. "Permission denied")))]
    (let [e (try
              (net/prepare! {:net ::fake-net
                             :requires-real-network? true
                             :nemesis-spec {:partition true}})
              nil
              (catch clojure.lang.ExceptionInfo e
                e))]
      (is (instance? clojure.lang.ExceptionInfo e))
      (is (= ::net/real-network-unavailable
             (:type (ex-data e))))
      (is (= :iptables
             (:capability (ex-data e)))))))

(deftest prepare-fails-fast-with-environment-error-when-tc-is-required
  (with-redefs [net/fast! (fn [_ _]
                            (throw (RuntimeException. "Operation not permitted")))]
    (let [e (try
              (net/prepare! {:net ::fake-net
                             :requires-real-network? true
                             :nemesis-spec {:netem true}})
              nil
              (catch clojure.lang.ExceptionInfo e
                e))]
      (is (instance? clojure.lang.ExceptionInfo e))
      (is (= ::net/real-network-unavailable
             (:type (ex-data e))))
      (is (= :tc
             (:capability (ex-data e)))))))
