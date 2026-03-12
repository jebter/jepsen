(ns tidb.manifest
  (:require [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [jepsen [checker :as checker]
             [store :as store]]))

(defn parse-feature-flags
  [s]
  (try
    (json/parse-string s true)
    (catch Throwable _
      (edn/read-string s))))

(defn- present?
  [v]
  (cond
    (nil? v) false
    (string? v) (not (str/blank? v))
    (map? v) (not (empty? v))
    (sequential? v) (not (empty? v))
    :else true))

(defn- assoc-present
  [m k v]
  (if (present? v)
    (assoc m k v)
    m))

(defn- shell-quote
  [v]
  (str "'"
       (str/replace (str v) #"'" "'\"'\"'")
       "'"))

(defn- cli-opt
  [flag value]
  (str flag " " (shell-quote value)))

(defn nemesis-spec-string
  [test]
  (let [spec (-> (or (:nemesis-spec test) {})
                 (dissoc :interval :schedule :long-recovery :failpoints)
                 keys
                 sort)]
    (if (empty? spec)
      "none"
      (str/join "," (map name spec)))))

(defn repro-command
  [test]
  (let [parts (cond-> ["lein run test"
                       (cli-opt "--workload" (name (:workload test)))
                       (cli-opt "--nemesis" (nemesis-spec-string test))
                       (cli-opt "--time-limit" (:time-limit test))
                       (cli-opt "--test-count" 1)
                       (cli-opt "--concurrency" (:concurrency test))
                       (cli-opt "--version" (:version test))]
                (:tarball-url test)
                (conj (cli-opt "--tarball-url" (:tarball-url test)))

                (present? (:binary-urls test))
                (conj (cli-opt "--binary-urls" (str/join "," (:binary-urls test))))

                (:txn-mode test)
                (conj (cli-opt "--txn-mode" (:txn-mode test)))

                (:isolation test)
                (conj (cli-opt "--isolation" (name (:isolation test))))

                (not= :default (:auto-retry test))
                (conj (cli-opt "--auto-retry" (:auto-retry test)))

                (not= :default (:auto-retry-limit test))
                (conj (cli-opt "--auto-retry-limit" (:auto-retry-limit test)))

                (:table-cache test)
                (conj "--table-cache")

                (:pd-services test)
                (conj "--pd-services")

                (:test-foreign-key test)
                (conj "--test-foreign-key")

                (:single-stmt-write test)
                (conj "--single-stmt-write")

                (:predicate-read test)
                (conj "--predicate-read")

                (:read-lock test)
                (conj (cli-opt "--read-lock" "update"))

                (:update-in-place test)
                (conj "--update-in-place")

                (:use-index test)
                (conj "--use-index")

                (:index-lookup test)
                (conj "--index-lookup")

                (:follower-read test)
                (conj "--follower-read")

                (present? (:build-branch test))
                (conj (cli-opt "--build-branch" (:build-branch test)))

                (present? (:build-commit-sha test))
                (conj (cli-opt "--build-commit-sha" (:build-commit-sha test)))

                (present? (:build-time test))
                (conj (cli-opt "--build-time" (:build-time test)))

                (present? (:feature-flags test))
                (conj (cli-opt "--feature-flags" (json/generate-string (:feature-flags test))))

                (present? (:build-notes test))
                (conj (cli-opt "--build-notes" (:build-notes test))))]
    (str/join " " parts)))

(defn build-manifest
  [test]
  (-> {}
      (assoc-present :branch (:build-branch test))
      (assoc-present :commit_sha (:build-commit-sha test))
      (assoc-present :tarball_url (:tarball-url test))
      (assoc-present :binary_urls (vec (:binary-urls test)))
      (assoc-present :feature_flags (:feature-flags test))
      (assoc-present :build_time (:build-time test))
      (assoc :workload (some-> (:workload test) name)
             :nemesis (nemesis-spec-string test)
             :version (:version test)
             :txn_mode (:txn-mode test)
             :isolation (some-> (:isolation test) name)
             :time_limit (:time-limit test)
             :concurrency (:concurrency test)
             :generated_at (str (:start-time test))
             :repro_command (repro-command test))
      (assoc-present :notes (:build-notes test))))

(defn write-manifest!
  [test manifest]
  (store/with-out-file test ["build" "manifest.edn"]
    (pprint manifest))
  (spit (store/path! test ["build" "manifest.json"])
        (str (json/generate-string manifest {:pretty true}) "\n"))
  manifest)

(defn checker*
  []
  (reify checker/Checker
    (check [_ test _ _]
      (try
        (let [manifest (-> test build-manifest (write-manifest! test))]
          {:valid? true
           :path "build/manifest.edn"
           :json-path "build/manifest.json"
           :manifest manifest})
        (catch Throwable t
          {:valid? true
           :error (.getMessage t)})))))
