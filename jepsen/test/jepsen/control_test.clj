(ns jepsen.control-test
  (:require [clj-ssh.ssh :as ssh]
            [jepsen.control :as c]
            [jepsen.reconnect :as rc]
            [slingshot.slingshot :refer [try+ throw+]]
            [clojure.test :refer :all]
            [jepsen.util :refer [contains-many?]])
  (:import [com.jcraft.jsch JSchException]))

(deftest ^:integration session-test
  (testing "on failure, session throws debug data"
    (try+
     (c/with-ssh {}
       (c/on "thishostshouldnotresolve"
             (c/exec :echo "hello")))
     (catch Object m
       (is (contains-many? m :dir :username :port :host))))))

(deftest ^:integration exec-test
  (testing "simple exec"
    (c/with-ssh {}
      (c/on "n1"
            (is (= (c/exec :whoami) "root")))))

  (testing "on failure, exec throws debug data"
    (try+
     (c/with-ssh {}
       (c/on "n1" (c/exec :thiscmdshouldnotexist)))
     (catch Object m
       (is (contains-many? m :cmd :out :err :host :exit))))))

(deftest session-retries-jsch-read-npe-while-opening
  (let [calls (atom 0)
        npe   (doto (NullPointerException.)
                (.setStackTrace
                 (into-array
                  StackTraceElement
                  [(StackTraceElement.
                    "com.jcraft.jsch.Session"
                    "read"
                    "Session.java"
                    918)])))]
    (with-redefs [c/clj-ssh-session (fn [_]
                                      (if (= 1 (swap! calls inc))
                                        (throw npe)
                                        :session))
                  ssh/disconnect     identity]
      (c/with-ssh {:dummy? false}
        (let [session (c/session "n1")]
          (is (= 2 @calls))
          (is (= :session (rc/conn session)))
          (rc/close! session))))))

(deftest session-retries-jsch-connect-npe-while-opening
  (let [calls (atom 0)
        npe   (doto (NullPointerException.)
                (.setStackTrace
                 (into-array
                  StackTraceElement
                  [(StackTraceElement.
                    "com.jcraft.jsch.Session"
                    "connect"
                    "Session.java"
                    256)])))]
    (with-redefs [c/clj-ssh-session (fn [_]
                                      (if (= 1 (swap! calls inc))
                                        (throw npe)
                                        :session))
                  ssh/disconnect     identity]
      (c/with-ssh {:dummy? false}
        (let [session (c/session "n1")]
          (is (= 2 @calls))
          (is (= :session (rc/conn session)))
          (rc/close! session))))))

(deftest retryable-jsch-exception-test
  (is (true? (#'c/retryable-jsch-exception?
              (JSchException. "channel is not opened."))))
  (is (false? (#'c/retryable-jsch-exception?
               (JSchException. "Auth fail")))))

(deftest parse-port-map-test
  (is (= {"node-a" 2201
          "node-b" 2202}
         (#'c/parse-port-map "{\"node-a\":2201,\"node-b\":2202}")))
  (is (nil? (#'c/parse-port-map "{bad json"))))

(deftest ssh-target-prefers-local-bridge-tunnel
  (with-redefs [c/ssh-tunnel-port-map (constantly {"node-a" 2201})]
    (c/with-ssh {:port 22}
      (is (= {:host "127.0.0.1"
              :port 2201
              :proxy? false}
             (#'c/ssh-target "node-a")))
      (is (= {:host "node-b"
              :port 22
              :proxy? true}
             (#'c/ssh-target "node-b"))))))
