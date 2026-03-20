(ns tidb.manifest-test
  (:require [clojure.test :refer :all]
            [jepsen.checker :as checker]
            [tidb.manifest :as manifest]))

(deftest checker-passes-test-and-manifest-in-correct-order
  (let [test      {:name "manifest-check"
                   :start-time "20260316T062819.000Z"
                   :workload :mv-lifecycle
                   :nemesis-spec {:interval 10}
                   :time-limit 300
                   :concurrency 10
                   :version "devbuild-9957"
                   :txn-mode "optimistic"
                   :isolation :repeatable-read
                   :auto-retry :default
                   :auto-retry-limit :default}
        expected  (manifest/build-manifest test)
        observed  (atom nil)
        checker*  (manifest/checker*)]
    (with-redefs [manifest/write-manifest!
                  (fn [actual-test actual-manifest]
                    (reset! observed [actual-test actual-manifest])
                    actual-manifest)]
      (is (= {:valid? true
              :path "build/manifest.edn"
              :json-path "build/manifest.json"
              :manifest expected}
             (checker/check checker* test nil nil)))
      (is (= [test expected] @observed)))))

(deftest repro-command-includes-explicit-testbed-routing-args-when-present
  (let [command (manifest/repro-command
                  {:workload :mv-autosched
                   :nemesis-spec {:interval 10 :partition true}
                   :time-limit 60
                   :test-count 1
                   :concurrency 10
                   :version "v3.0.0-beta.1"
                   :nodes ["node-0.example" "node-1.example"]
                   :ssh {:private-key-path "/tmp/testbed/jepsen.pem"}
                   :tarball-url "https://files.example.com/tidb.tar.gz"
                   :binary-urls ["https://files.example.com/tikv.tar.gz"]
                   :txn-mode "optimistic"
                   :isolation :repeatable-read
                   :build-notes "bridge-debug"})]
    (is (re-find #"--nodes 'node-0\.example,node-1\.example'" command))
    (is (re-find #"--ssh-private-key '/tmp/testbed/jepsen\.pem'" command))
    (is (re-find #"--nemesis 'partition'" command))))
