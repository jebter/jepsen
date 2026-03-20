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
