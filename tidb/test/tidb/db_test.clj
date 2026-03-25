(ns tidb.db-test
  (:require [clojure.test :refer :all]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [slingshot.slingshot :refer [try+]]
            [tidb.db :as db]))

(deftest start-kv-does-not-pass-advertise-status-addr
  (let [argv (atom nil)]
    (with-redefs [db/prepare-daemon-files! (fn [_] nil)
                  cu/start-daemon!         (fn [& args]
                                             (reset! argv (vec args))
                                             :started)]
      (is (= :started
             (db/start-kv! {:nodes [:n1 :n2]
                            :version "v8.5.0"
                            :binary-urls ["https://example.invalid/tikv.tar.gz"]}
                           :n1)))
      (is (not-any? #{:--advertise-status-addr} @argv))
      (is (some #{:--advertise-addr} @argv))
      (is (some #{:--config} @argv)))))

(deftest normalize-binary-override-url-strips-known-component-prefix
  (is (= "https://example.invalid/tidb.tar.gz"
         (#'db/normalize-binary-override-url
           "tidb:https://example.invalid/tidb.tar.gz")))
  (is (= "https://example.invalid/tikv.tar.gz"
         (#'db/normalize-binary-override-url
           "tikv:https://example.invalid/tikv.tar.gz")))
  (is (= "file:///tmp/pd.tar.gz"
         (#'db/normalize-binary-override-url
           "pd:file:///tmp/pd.tar.gz")))
  (is (= "https://example.invalid/plain.tar.gz"
         (#'db/normalize-binary-override-url
           "https://example.invalid/plain.tar.gz"))))

(deftest start-pd-service-prepares-daemon-files-for-split-services
  (doseq [svc [:api :tso :scheduling]]
    (let [prepared (atom nil)
          argv     (atom nil)
          svc-info (get db/pd-services svc)]
      (with-redefs [db/prepare-daemon-files! (fn [files]
                                               (reset! prepared files)
                                               nil)
                    cu/start-daemon!         (fn [& args]
                                               (reset! argv (vec args))
                                               :started)]
        (is (= :started
               (db/start-pd-service! {:nodes [:n1 :n2]
                                      :pd-services true}
                                     :n1
                                     svc)))
        (is (= {:stdout (:stdout svc-info)
                :log    (:log-file svc-info)
                :pid    (:pid-file svc-info)}
               @prepared))
        (is (= {:logfile (:stdout svc-info)
                :pidfile (:pid-file svc-info)
                :chdir   db/tidb-dir
                :process-name (:bin svc-info)}
               (first @argv)))
        (is (= (str "./bin/" (:bin svc-info))
               (second @argv)))))))

(deftest pd-get-json-parses-successful-responses
  (c/with-ssh {:dummy? true}
    (with-redefs [c/exec (fn [& _]
                           "{\"leader\":{\"name\":\"pd1\"}}\n200")]
      (is (= {:leader {:name "pd1"}}
             (db/pd-get-json :n1 "leader"))))))

(deftest pd-get-json-throws-structured-error-on-404
  (c/with-ssh {:dummy? true}
    (with-redefs [c/exec (fn [& _]
                           "not found\n404")]
      (let [error (try+
                    (db/pd-get-json :n1 "leader")
                    (catch [:type ::db/pd-http-error] e
                      e))]
        (is (= 404 (:status error)))
        (is (= "leader" (:path error)))
        (is (= "not found" (:body error)))))))

(deftest pd-post-throws-structured-error-on-409
  (c/with-ssh {:dummy? true}
    (with-redefs [c/exec (fn [& _]
                           "conflict\n409")]
      (let [error (try+
                    (db/pd-post! :n1 "leader" "transfer" "pd2")
                    (catch [:type ::db/pd-http-error] e
                      e))]
        (is (= 409 (:status error)))
        (is (= "leader/transfer/pd2" (:path error)))
        (is (= "conflict" (:body error)))))))

(deftest await-http-retries-pd-503-until-success
  (let [responses (atom ["temporarily unavailable\n503"
                         "{\"members\":[]}\n200"])]
    (c/with-ssh {:dummy? true}
      (with-redefs [c/exec (fn [& _]
                             (let [response (first @responses)]
                               (swap! responses rest)
                               response))]
        (is (= {:members []}
               (db/await-http
                 (db/pd-get-json :n1 "members"))))))))
