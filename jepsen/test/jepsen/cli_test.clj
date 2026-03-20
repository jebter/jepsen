(ns jepsen.cli-test
  (:require [clojure.test :refer :all]
            [jepsen.cli :as cli]))

(deftest test-opt-fn-uses-bridge-env-when-cli-omits-nodes-and-ssh-key
  (with-redefs [cli/getenv (fn [k]
                             (get {"JEPSEN_NODES" "node-a,node-b"
                                   "JEPSEN_SSH_PRIVATE_KEY" "/tmp/bridge/jepsen.pem"}
                                  k))]
    (let [parsed {:options {:node cli/default-nodes
                            :nodes nil
                            :nodes-file nil
                            :username "root"
                            :password "root"
                            :strict-host-key-checking false
                            :ssh-private-key nil
                            :concurrency "2n"}}
          opts   (:options (cli/test-opt-fn parsed))]
      (is (= ["node-a" "node-b"] (:nodes opts)))
      (is (= 4 (:concurrency opts)))
      (is (= "/tmp/bridge/jepsen.pem"
             (get-in opts [:ssh :private-key-path]))))))

(deftest test-opt-fn-keeps-explicit-cli-overrides-ahead-of-bridge-env
  (with-redefs [cli/getenv (fn [k]
                             (get {"JEPSEN_NODES" "env-a,env-b"
                                   "JEPSEN_SSH_PRIVATE_KEY" "/tmp/env/jepsen.pem"}
                                  k))]
    (let [parsed {:options {:node cli/default-nodes
                            :nodes ["cli-a"]
                            :nodes-file nil
                            :username "root"
                            :password "root"
                            :strict-host-key-checking false
                            :ssh-private-key "/tmp/cli/jepsen.pem"
                            :concurrency "2n"}}
          opts   (:options (cli/test-opt-fn parsed))]
      (is (= ["cli-a"] (:nodes opts)))
      (is (= 2 (:concurrency opts)))
      (is (= "/tmp/cli/jepsen.pem"
             (get-in opts [:ssh :private-key-path]))))))
