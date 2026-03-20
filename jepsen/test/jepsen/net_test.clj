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
