(ns tidb.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [dom-top.core :refer [with-retry]]
            [fipp.edn :refer [pprint]]
            [jepsen [core :as jepsen]
                    [control :as c]
                    [db :as db]
                    ; [faketime :as faketime]
                    [util :as util]]
            [jepsen.control.util :as cu]
            [slingshot.slingshot :refer [try+ throw+]]
            [tidb.sql :as sql]))

(def replica-count
  "This should probably be used to generate max-replicas in pd.conf as well,
  but for now we'll just write it in both places."
  3)

(def tidb-dir       "/opt/tidb")
(def tidb-bin-dir   "/opt/tidb/bin")
(def pd-bin         "pd-server")
(def pdctl-bin      "pd-ctl")
(def kv-bin         "tikv-server")
(def db-bin         "tidb-server")
(def pd-config-file (str tidb-dir "/pd.conf"))
(def pd-log-file    (str tidb-dir "/pd.log"))
(def pd-stdout      (str tidb-dir "/pd.stdout"))
(def pd-pid-file    (str tidb-dir "/pd.pid"))
(def pd-data-dir    (str tidb-dir "/data/pd"))
(def kv-config-file (str tidb-dir "/kv.conf"))
(def kv-log-file    (str tidb-dir "/kv.log"))
(def kv-stdout      (str tidb-dir "/kv.stdout"))
(def kv-pid-file    (str tidb-dir "/kv.pid"))
(def kv-data-dir    (str tidb-dir "/data/kv"))
(def db-config-file (str tidb-dir "/db.conf"))
(def db-log-file    (str tidb-dir "/db.log"))
(def db-slow-file   (str tidb-dir "/slow.log"))
(def db-stdout      (str tidb-dir "/db.stdout"))
(def db-pid-file    (str tidb-dir "/db.pid"))
(def install-diagnostics-file (str tidb-dir "/install-diagnostics.txt"))
(def system-db-config-file (str tidb-dir "/system-db.conf"))
(def system-db-log-file    (str tidb-dir "/system-db.log"))
(def system-db-slow-file   (str tidb-dir "/system-slow.log"))
(def system-db-stdout      (str tidb-dir "/system-db.stdout"))
(def system-db-pid-file    (str tidb-dir "/system-db.pid"))
(def system-db-port        14000)
(def system-db-status-port 11080)
(def binary-override-prefix-re #"^(tidb|tikv|pd):((?:https?|file)://.+)$")
(def pd-services
  {:api
   {:bin "pd-api"
    :stdout (str tidb-dir "/pd-api.stdout")
    :log-file (str tidb-dir "/pd-api.log")
    :pid-file (str tidb-dir "/pd-api.pid")
    :port-offset 0}
   :tso
   {:bin "pd-tso"
    :stdout (str tidb-dir "/pd-tso.stdout")
    :log-file (str tidb-dir "/pd-tso.log")
    :pid-file (str tidb-dir "/pd-tso.pid")
    :port-offset 100}
   :scheduling
   {:bin "pd-scheduling"
    :stdout (str tidb-dir "/pd-scheduling.stdout")
    :log-file (str tidb-dir "/pd-scheduling.log")
    :pid-file (str tidb-dir "/pd-scheduling.pid")
    :port-offset 200}})

(declare ensure-parent-dir!
         normalize-file-path!
         prepare-daemon-files!
         process-running?
         cleanup-db-runtime-artifacts!)

(def client-port 2379)
(def peer-port   2380)

(defn tidb-map
  "Computes node IDs for a test."
  [test]
  (->> (:nodes test)
       (map-indexed (fn [i node]
                      [node {:pd (str "pd" (inc i))
                             :kv (str "kv" (inc i))}]))
       (into {})))

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" (name node) ":" port))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (node-url node client-port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node peer-port))

(defn- normalize-binary-override-url
  [url]
  (if-let [[_ _ normalized-url]
           (and (string? url)
                (re-matches binary-override-prefix-re url))]
    normalized-url
    url))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node] (str (get-in (tidb-map test) [node :pd])
                            "=" (peer-url node))))
       (str/join ",")))

(defn pd-endpoints
  "Constructs an initial pd cluster string for a test, like
  \"foo:2379,bar:2379,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node] (str (name node) ":" client-port)))
       (str/join ",")))

(defn configure-pd!
  "Writes configuration file for placement driver"
  []
  (c/su
    (ensure-parent-dir! pd-config-file)
    (normalize-file-path! pd-config-file)
    (c/exec :echo (slurp (io/resource "pd.conf")) :> pd-config-file)))

(defn configure-kv!
  "Writes configuration file for tikv"
  []
  (c/su
    (ensure-parent-dir! kv-config-file)
    (normalize-file-path! kv-config-file)
    (c/exec :echo (slurp (io/resource "tikv.conf")) :> kv-config-file)))

(defn configure-db!
  "Writes configuration file for tidb"
  []
  (c/su
    (ensure-parent-dir! db-config-file)
    (normalize-file-path! db-config-file)
    (c/exec :echo (slurp (io/resource "tidb.conf")) :> db-config-file)))

(defn configure-system-db!
  "Writes configuration file for the SYSTEM keyspace TiDB instance"
  []
  (c/su
    (ensure-parent-dir! system-db-config-file)
    (normalize-file-path! system-db-config-file)
    (c/exec :echo (slurp (io/resource "system-tidb.conf"))
            :> system-db-config-file)))

(defn configure!
  "Write all config files."
  []
  (configure-pd!)
  (configure-kv!)
  (configure-db!))

(defn pd-api-path
  "Constructs an API path for the PD located on the given node."
  [node & path-components]
  (str "http://" node ":2379/pd/api/v1/" (str/join "/" path-components)))

(defn pd-api-local-path
  "Constructs a loopback PD API path to be queried from inside a node over SSH."
  [& path-components]
  (str "http://127.0.0.1:2379/pd/api/v1/" (str/join "/" path-components)))

(defn pd-success-status?
  [status]
  (and (integer? status)
       (<= 200 status 299)))

(defn pd-http-error!
  [{:keys [status path body]}]
  (throw+ {:type   ::pd-http-error
           :status status
           :path   path
           :body   body}))

(defn pd-request!
  "Executes a PD API request from inside the target node and returns a response
  map with :status, :path, and :body."
  [node method & path-components]
  (let [path   (str/join "/" path-components)
        raw    (c/on node
                  (apply c/exec
                         (concat [:curl :--silent :--show-error
                                  :--write-out "\\n%{http_code}"]
                                 (when method
                                   [:-X method])
                                 [(apply pd-api-local-path path-components)])))
        lines  (str/split-lines raw)
        status (some-> (last lines) Integer/parseInt)
        body   (str/join "\n" (butlast lines))
        resp   {:status status
                :path   path
                :body   body}]
    (if (pd-success-status? status)
      resp
      (pd-http-error! resp))))

(defn pd-get-json
  "Fetches PD JSON by curling the local node over SSH, so the control node does
  not need direct DNS or network access to the cluster-internal address."
  [node & path-components]
  (json/parse-string (:body (apply pd-request! node nil path-components)) true))

(defn pd-post!
  "Posts to a PD API endpoint via loopback curl on the target node."
  [node & path-components]
  (:body (apply pd-request! node :POST path-components)))

(defn pd-members
  "All members of the cluster"
  [node]
  (pd-get-json node "members"))

(defn pd-leader
  "Gets the current PD leader."
  [node]
  (pd-get-json node "leader"))

(defn pd-leader-node
  "Returns the name of the node which is the current PD leader."
  [test node]
  (let [leader-name (:name (pd-leader node))]
    (->> (tidb-map test)
         (keep (fn [[node m]]
                 (when (= leader-name (:pd m))
                   node)))
         first)))

(defn pd-transfer-leader!
  "Transfer leadership to the given leader map."
  [node next-leader]
  (assert (map? next-leader))
  (pd-post! node "leader" "transfer" (:name next-leader)))

(defn pd-regions
  "All PD regions"
  [node]
  (pd-get-json node "regions"))

(declare prepare-daemon-files!
         process-running?)

(defmacro await-http
  "Loops body until HTTP call returns, retrying 500 and 503 errors."
  [& body]
  `(loop []
     (let [res# (try+ ~@body
                      (catch [:status 500] _# ::retry)
                      (catch [:status 503] _# ::retry))]
       (if (= ::retry res#)
         (recur)
         res#))))

(defn start-pd-service!
  [test node svc]
  (let [svc-info (get pd-services svc)]
    (c/su
      (prepare-daemon-files! {:stdout (:stdout svc-info)
                              :log    (:log-file svc-info)
                              :pid    (:pid-file svc-info)})
      (cu/start-daemon!
       {:logfile (:stdout svc-info)
        :pidfile (:pid-file svc-info)
        :chdir   tidb-dir
        :process-name (:bin svc-info)}
       (str "./bin/" (:bin svc-info))
       "services" svc
       :--log-file                 (:log-file svc-info)
       :--config                   pd-config-file
       (condp = svc
         :api
         [:--name                  (get-in (tidb-map test) [node :pd])
          :--data-dir              pd-data-dir
          :--client-urls           (str "http://0.0.0.0:" client-port)
          :--peer-urls             (str "http://0.0.0.0:" peer-port)
          :--advertise-client-urls (client-url node)
          :--advertise-peer-urls   (peer-url node)
          :--initial-cluster       (initial-cluster test)]
         [:--listen-addr           (node-url node (+ client-port (:port-offset svc-info)))
          :--advertise-listen-addr (node-url node (+ client-port (:port-offset svc-info)))
          :--backend-endpoints     (str "http://0.0.0.0:" client-port)])))))

(defn start-pd!
  "Starts the placement driver daemon"
  [test node]
  (if (:pd-services test)
    (do
      (start-pd-service! test node :api)
      (start-pd-service! test node :tso)
      (start-pd-service! test node :scheduling))
    (c/su
     (prepare-daemon-files! {:stdout pd-stdout
                             :log    pd-log-file
                             :pid    pd-pid-file})
     (cu/start-daemon!
      {:logfile pd-stdout
       :pidfile pd-pid-file
       :chdir   tidb-dir
       :env {:GO_FAILPOINTS (-> (System/getenv) (get "PD_FAILPOINTS" ""))}
       }
      (str "./bin/" pd-bin)
      :--name                  (get-in (tidb-map test) [node :pd])
      :--data-dir              pd-data-dir
      :--client-urls           (str "http://0.0.0.0:" client-port)
      :--peer-urls             (str "http://0.0.0.0:" peer-port)
      :--advertise-client-urls (client-url node)
      :--advertise-peer-urls   (peer-url node)
      :--initial-cluster       (initial-cluster test)
      :--log-file              pd-log-file
      :--config                pd-config-file))))

(defn start-kv!
  "Starts the TiKV daemon"
  [test node]
  (c/su
    (prepare-daemon-files! {:stdout kv-stdout
                            :log    kv-log-file
                            :pid    kv-pid-file})
    (apply cu/start-daemon!
           {:logfile kv-stdout
            :pidfile kv-pid-file
            :chdir   tidb-dir
            :env {:FAILPOINTS (-> (System/getenv) (get "KV_FAILPOINTS" ""))}
            }
           (str "./bin/" kv-bin)
           [:--pd             (pd-endpoints test)
            :--addr           (str "0.0.0.0:20160")
            :--advertise-addr (str (name node) ":" "20160")
            :--data-dir       kv-data-dir
            :--log-file       kv-log-file
            :--config         kv-config-file])))

(defn start-db!
  "Starts the TiDB daemon"
  [test node]
  (c/su
    (prepare-daemon-files! {:stdout db-stdout
                            :log    db-log-file
                            :pid    db-pid-file})
    (cu/start-daemon!
      {:logfile db-stdout
       :pidfile db-pid-file
       :chdir   tidb-dir
       :env {:GO_FAILPOINTS (-> (System/getenv) (get "DB_FAILPOINTS" ""))}
       }
      (str "./bin/" db-bin)
      :--store     (str "tikv")
      :--path      (pd-endpoints test)
      :--config    db-config-file
      :--log-file  db-log-file)))

(defn start-system-db!
  "Starts the SYSTEM keyspace TiDB daemon"
  [test node]
  (c/su
    (prepare-daemon-files! {:stdout system-db-stdout
                            :log    system-db-log-file
                            :pid    system-db-pid-file})
    (cu/start-daemon!
      {:logfile system-db-stdout
       :pidfile system-db-pid-file
       :chdir   tidb-dir
       :env     {:GO_FAILPOINTS (-> (System/getenv) (get "DB_FAILPOINTS" ""))}
       }
      (str "./bin/" db-bin)
      :--store          (str "tikv")
      :--path           (pd-endpoints test)
      :--config         system-db-config-file
      :--log-file       system-db-log-file
      :-P               (str system-db-port)
      :--status         (str system-db-status-port))))

(defn page-ready?
  "Fetches a status page URL on the local node, and returns true iff the page
  was available."
  [url]
  (try+
    (c/exec :curl :--fail url)
    (catch [:type :jepsen.control/nonzero-exit] _ false)))

(defn pd-ready?
  "Is Placement Driver ready?"
  []
  (page-ready? (str "http://127.0.0.1:" client-port "/health")))

(defn kv-ready?
  "Is TiKV ready?"
  []
  (page-ready? "http://127.0.0.1:20180/status"))

(defn db-ready?
  "Is TiDB ready?"
  []
  (page-ready? "http://127.0.0.1:10080/status"))

(defn system-db-ready?
  "Is the SYSTEM TiDB instance ready?"
  []
  (page-ready? (str "http://127.0.0.1:" system-db-status-port "/status")))

(defn restart-loop*
  "TiDB is fragile on startup; processes love to crash if they can't complete
  their initial requests to network dependencies. We try to work around this by
  checking whether the daemon is running, and restarting it if necessary.

  Takes a name (used for error messages and logging), a function which starts
  the node, and a status function which returns one of three states:

  :starting - The node is still starting up
  :ready    - The node is (hopefully) ready to serve requests
  :crashed  - The node crashed

  We call start, then poll the status function until we see :ready or :crashed.
  If ready, returns. If :crashed, restarts the node and tries again."
  [name start! get-status]
  (let [deadline (+ (util/linear-time-nanos) (util/secs->nanos 300))]
    ; First startup!
    (start!)

    (loop [status :init]
      (when (< deadline (util/linear-time-nanos))
        ; Out of time
        (throw+ {:type :restart-loop-timed-out, :service name}))

      (when (= status :crashed)
        ; Need to restart
        (info name "crashed during startup; restarting")
        (start!))

      ; Give it a bit
      (Thread/sleep 10000)

      ; OK, how's it doing?
      (let [status (get-status)]
        (if (= status :ready)
          ; Done
          status
          ; Still working
          (recur status))))))

(defmacro restart-loop
  "Macro form of restart-loop*: takes two forms instead of two functions."
  [name start! get-status]
  `(restart-loop* ~name
                  (fn ~'start!     [] ~start!)
                  (fn ~'get-status [] ~get-status)))

(defn start-wait-pd!
  "Starts PD, waiting for the health page to come online."
  [test node]
  (restart-loop :pd (start-pd! test node)
                (cond (pd-ready?)                       :ready
                      (cu/daemon-running? pd-pid-file)  :starting
                      (process-running? pd-config-file) :starting
                      true                              :crashed)))

(defn start-wait-kv!
  "Starts TiKV, waiting for the health page to come online."
  [test node]
  (restart-loop :kv (start-kv! test node)
                (cond (kv-ready?)                       :ready
                      (cu/daemon-running? kv-pid-file)  :starting
                      (process-running? kv-config-file) :starting
                      true                              :crashed)))

(defn start-wait-db!
  "Starts TiDB, waiting for the health page to come online."
  [test node]
  (restart-loop :db (start-db! test node)
                (cond (db-ready?)                       :ready
                      (cu/daemon-running? db-pid-file)  :starting
                      (process-running? db-config-file) :starting
                      true                              :crashed)))

(defn start-wait-system-db!
  "Starts SYSTEM TiDB, waiting for its health page to come online."
  [test node]
  (restart-loop :system-db (start-system-db! test node)
                (cond (system-db-ready?)                       :ready
                      (cu/daemon-running? system-db-pid-file)  :starting
                      (process-running? system-db-config-file) :starting
                      true                                     :crashed)))

(defn stop-pd-service! [test node svc]
  (c/su
    (cu/stop-daemon! (get-in pd-services [svc :bin]) (get-in pd-services [svc :pid-file]))
    (cu/grepkill! (get-in pd-services [svc :bin]))))

(defn stop-pd!
  [test node]
  (if (:pd-services test)
    (do
      (stop-pd-service! test node :scheduling)
      (stop-pd-service! test node :tso)
      (stop-pd-service! test node :api))
    (c/su
     (cu/stop-daemon! pd-bin pd-pid-file)
     (cu/grepkill! pd-bin))))

(defn stop-kv! [test node] (c/su (cu/stop-daemon! kv-bin kv-pid-file)
                                 (cu/grepkill! kv-bin)))

(defn stop-system-db! [test node]
  (c/su
    (cu/stop-daemon! db-bin system-db-pid-file)
    (cu/grepkill! db-bin)))

(defn stop-db! [test node] (c/su (cu/stop-daemon! db-bin db-pid-file)
                                 (cu/grepkill! db-bin)))

(defn stop!
  "Stops all daemons"
  [test node]
  (when (:enable-system-tidb test)
    (stop-system-db! test node))
  (stop-db! test node)
  (stop-kv! test node)
  (stop-pd! test node))

(defn tarball-url
  "Constructs the URL for a tarball; either passing through the test's URL, or
  constructing one from the version."
  [test]
  (or (:tarball-url test)
      (str "http://download.pingcap.org/tidb-" (:version test)
           "-linux-amd64.tar.gz")))

(defn directory?
  [path]
  (try
    (c/exec :test :-d path)
    true
    (catch RuntimeException _ false)))

(defn ensure-parent-dir!
  [path]
  (when-let [parent (.getParent (io/file path))]
    (c/exec :mkdir :-p parent)))

(defn normalize-file-path!
  "Ensures a path intended to be a regular file is not occupied by a directory."
  [path]
  (ensure-parent-dir! path)
  (when (directory? path)
    (warn "Removing directory occupying file path" path)
    (c/exec :rm :-rf path)))

(defn prepare-daemon-files!
  "Normalizes stdout/log/pid paths before daemon start."
  [{:keys [stdout log pid]}]
  (doseq [path [stdout log]]
    (when path
      (normalize-file-path! path)
      (c/exec :touch path)))
  (when pid
    (normalize-file-path! pid)
    (c/exec :rm :-f pid)))

(defn process-running?
  "Best-effort process liveness check using a distinctive command-line pattern."
  [pattern]
  (try+
    (c/exec :ps :aux
            c/| :grep pattern
            c/| :grep :-v "grep"
            c/| :grep :-v "bash -c")
    true
    (catch [:type :jepsen.control/nonzero-exit] _ false)))

(defn cleanup-db-runtime-artifacts!
  "Removes TiDB runtime temp dirs left behind by prior runs."
  []
  (doseq [path ["/tmp/0_tidb"
                "/tmp/tidb"
                "/tmp/tidb-4000.sock"
                "/tmp/tidb-14000.sock"]]
    (c/exec :rm :-rf path)))

(defn ensure-bin-layout!
  "Normalizes devbuild tarballs so /opt/tidb is always a directory and
  /opt/tidb/bin always exists before component override tarballs are applied."
  []
  (let [root-bin    (str tidb-dir ".root-bin")
        db-bin-path (str tidb-dir "/" db-bin)]
    ; Recover from an interrupted normalization that already moved the binary
    ; aside but did not recreate tidb-dir/tidb-server.
    (when (and (cu/exists? root-bin)
               (not (cu/exists? tidb-dir)))
      (info "Recovering interrupted TiDB bin layout normalization")
      (c/exec :mkdir :-p tidb-dir)
      (c/exec :mv :-f root-bin db-bin-path))

    (when (and (cu/exists? tidb-dir)
               (not (directory? tidb-dir)))
      (info "Normalizing single-file TiDB tarball layout")
      (when (cu/exists? root-bin)
        (warn "Removing stale TiDB normalization staging file" root-bin)
        (c/exec :rm :-f root-bin))
      (c/exec :mv :-f tidb-dir root-bin)
      (c/exec :mkdir :-p tidb-dir)
      (c/exec :mv :-f root-bin db-bin-path))

    ; Recover from an interrupted normalization that already created tidb-dir
    ; but crashed before moving the staged binary into place.
    (when (and (cu/exists? root-bin)
               (directory? tidb-dir)
               (not (cu/exists? db-bin-path)))
      (info "Finishing interrupted TiDB bin layout normalization")
      (c/exec :mv :-f root-bin db-bin-path))

    (when (and (cu/exists? root-bin)
               (cu/exists? db-bin-path))
      (warn "Removing stale TiDB normalization staging file" root-bin)
      (c/exec :rm :-f root-bin))

    ; Some tarballs place binaries directly in tidb-dir instead of tidb/bin.
    ; Ensure a consistent ./bin layout before applying component overrides.
    (when (not (cu/exists? tidb-bin-dir))
      (info "Creating bin layout for TiDB tarball")
      (c/exec :mkdir :-p tidb-bin-dir)
      (doseq [b [pd-bin kv-bin db-bin pdctl-bin]]
        (when (cu/exists? (str tidb-dir "/" b))
          (c/exec :ln :-sf (str tidb-dir "/" b)
                  (str tidb-bin-dir "/" b)))))))

(defn ensure-component-bin-links!
  "Links nested binaries from component override tarballs back into tidb/bin.
  Some component archives unpack under an extra top-level directory or nested
  bin/ directory, which leaves start-daemon looking in the wrong place."
  []
  (doseq [b [pd-bin pdctl-bin kv-bin db-bin]]
    (let [root-path (str tidb-bin-dir "/" b)]
      (when (directory? root-path)
        (warn "Removing directory occupying binary path" root-path)
        (c/exec :rm :-rf root-path))
      (when-not (cu/exists? root-path)
        (let [candidate (-> (try+
                              (c/exec :find tidb-bin-dir
                                      :-mindepth 2
                                      :-name b
                                      c/| :head :-n 1)
                              (catch [:type :jepsen.control/nonzero-exit] _
                                ""))
                            str/trim)]
          (when (and (not (str/blank? candidate))
                     (not (directory? candidate)))
            (info "Linking nested component binary" b "from" candidate)
            (c/exec :ln :-sf candidate root-path)))))))

(def required-binaries
  "The binary entrypoints Jepsen expects under tidb/bin before setup starts
  services."
  [pd-bin kv-bin db-bin])

(defn path-live?
  "Returns true when a path resolves to an existing filesystem object. Unlike
  jepsen.control.util/exists?, this treats broken symlinks as missing."
  [path]
  (try
    (c/exec :test :-e path)
    true
    (catch RuntimeException _
      false)))

(defn missing-required-binaries
  "Returns required binaries missing from tidb/bin, including broken symlinks."
  []
  (->> required-binaries
       (remove #(path-live? (str tidb-bin-dir "/" %)))
       vec))

(defn install-required-reason
  "Explains why the base TiDB archive must be reinstalled."
  [test]
  (cond
    (:force-reinstall test)
    :force-reinstall

    (not (cu/exists? tidb-dir))
    :missing-install-dir

    :else
    (let [missing (missing-required-binaries)]
      (when (seq missing)
        [:missing-binaries missing]))))

(defn install-elapsed-ms
  [started-at]
  (long (/ (double (- (System/nanoTime) started-at)) 1000000.0)))

(defn install-rethrow!
  [e]
  (if (instance? Throwable e)
    (throw e)
    (throw+ e)))

(defn install-error-summary
  [e]
  (cond
    (map? e)
    (merge (select-keys e [:type :exit :status :path])
           (when-let [err (:err e)]
             {:err (str/trim err)})
           (when-let [body (:body e)]
             {:body body}))

    (instance? Throwable e)
    {:exception-class (.getName (class e))
     :message         (.getMessage e)}

    :else
    {:error e}))

(defn write-install-diagnostics!
  "Persist pre-start install state so setup failures before daemon launch still
  leave behind useful artifacts."
  [node]
  (c/su
    (normalize-file-path! install-diagnostics-file)
    (c/exec :bash :-lc
            (str "set -euo pipefail\n"
                 "{\n"
                 "  echo \"timestamp: $(date -Is)\"\n"
                 "  echo \"node: " node "\"\n"
                 "  echo \"tidb-dir: " tidb-dir "\"\n"
                 "  echo \"tidb-bin-dir: " tidb-bin-dir "\"\n"
                 "  echo\n"
                 "  echo \"[df -h /opt /tmp]\"\n"
                 "  df -h /opt /tmp || true\n"
                 "  echo\n"
                 "  echo \"[du -sh " tidb-dir " " tidb-bin-dir "]\"\n"
                 "  du -sh " tidb-dir " " tidb-bin-dir " || true\n"
                 "  echo\n"
                 "  echo \"[ls -lah " tidb-dir "]\"\n"
                 "  ls -lah " tidb-dir " || true\n"
                 "  echo\n"
                 "  echo \"[ls -lah " tidb-bin-dir "]\"\n"
                 "  ls -lah " tidb-bin-dir " || true\n"
                 "  echo\n"
                 "  echo \"[required-binaries]\"\n"
                 "  for b in " pd-bin " " kv-bin " " db-bin "; do\n"
                 "    echo \"-- $b\"\n"
                 "    ls -l " tidb-bin-dir "/$b || true\n"
                 "    readlink -f " tidb-bin-dir "/$b || true\n"
                 "    stat -c '%n %s %y' " tidb-bin-dir "/$b || true\n"
                 "  done\n"
                 "  echo\n"
                 "  echo \"[find " tidb-bin-dir " -maxdepth 2]\"\n"
                 "  find " tidb-bin-dir " -maxdepth 2 -print | sort || true\n"
                 "} > " install-diagnostics-file " 2>&1"))))

(defn log-install-stage
  [node stage fields]
  (info node "TiDB install" (merge {:stage stage} fields)))

(defn run-install-stage!
  [node stage fields f]
  (let [started-at (System/nanoTime)]
    (log-install-stage node (keyword (str (name stage) "-start")) fields)
    (try+
      (let [result (f)]
        (log-install-stage node (keyword (str (name stage) "-done"))
                           (merge fields {:elapsed-ms (install-elapsed-ms started-at)}))
        result)
      (catch Object e
        (log-install-stage node (keyword (str (name stage) "-failed"))
                           (merge fields
                                  {:elapsed-ms (install-elapsed-ms started-at)}
                                  (install-error-summary e)))
        (install-rethrow! e)))))

; (defn setup-faketime!
;   "Configures the faketime wrapper for this node, so that the given binary runs
;   at the given rate."
;   [bin rate]
;   (info "Configuring" bin "to run at" (str rate "x realtime"))
;   (c/su (faketime/wrap! (str tidb-bin-dir "/" bin) 0 rate)))

(defn install!
  "Downloads archive and extracts it to our local tidb-dir, if it doesn't exist
  already. If test contains a :force-reinstall key, we always install a fresh
  copy.

  Calls `sync`; this tarball is *massive* (1.1G), and when we start tidb, it'll
  try to fsync, and cause, like, 60s stalls on single nodes, wrecking the
  cluster."
  [test node]
  (c/su
    (when-let [reason (install-required-reason test)]
      (info node "installing TiDB")
      (info node "TiDB install reason" reason)
      (log-install-stage node :plan {:reason reason
                                     :tarball-url (tarball-url test)
                                     :binary-override-count (count (:binary-urls test))})
      (run-install-stage! node :base-archive
                          {:tarball-url (tarball-url test)
                           :dest tidb-dir
                           :failure-bucket :download-or-extract-failed}
                          #(cu/install-archive! (tarball-url test) tidb-dir))
      (run-install-stage! node :bin-layout
                          {:tidb-dir tidb-dir
                           :tidb-bin-dir tidb-bin-dir
                           :failure-bucket :bin-layout-failed}
                          #(ensure-bin-layout!))
      (doseq [url (:binary-urls test)]
        (let [download-url (normalize-binary-override-url url)]
          (run-install-stage! node :binary-override
                              (cond-> {:url download-url
                                       :dest tidb-bin-dir
                                       :failure-bucket :override-download-or-extract-failed}
                                (not= download-url url) (assoc :requested-url url))
                              #(let [f (cu/cached-wget! download-url)]
                                 (c/exec :tar :-xf f :-C tidb-bin-dir)))))
      (run-install-stage! node :component-links
                          {:dest tidb-bin-dir
                           :failure-bucket :override-link-failed}
                          #(ensure-component-bin-links!))
      (let [missing (missing-required-binaries)]
        (if (seq missing)
          (warn node "Missing required TiDB binaries after install" missing)
          (log-install-stage node :required-binaries-ready
                             {:binaries required-binaries})))
      (when (:pd-services test)
        (info "Creating symbol links for PD services")
        (doseq [[_ info] pd-services]
          (c/exec :ln :-sf (str tidb-bin-dir "/" pd-bin) (str tidb-bin-dir "/" (get info :bin)))))
      (try+
        (write-install-diagnostics! node)
        (catch Object e
          (warn node "Unable to persist install diagnostics"
                (install-error-summary e))))
      (run-install-stage! node :sync
                          {:failure-bucket :sync-failed}
                          #(do
                             (info "Syncing disks to avoid slow fsync on db start")
                             (c/exec :sync)))
      (info "Syncing disks done")
    ; (if-let [ratio (:faketime test)]
    ;   (do ; We need a special fork of faketime specifically for tikv, which
    ;       ; uses CLOCK_MONOTONIC_COARSE (not supported by 0.9.6 stock), and
    ;       ; jemalloc (segfaults on 0.9.7).
    ;       (faketime/install-0.9.6-jepsen1!)
    ;       ; Add faketime wrappers
    ;       (setup-faketime! pd-bin (faketime/rand-factor ratio))
    ;       (setup-faketime! kv-bin (faketime/rand-factor ratio))
    ;       (setup-faketime! db-bin (faketime/rand-factor ratio)))
    ;   (c/cd tidb-bin-dir
    ;         ; Destroy faketime wrappers, if applicable.
    ;         (faketime/unwrap! pd-bin)
    ;         (faketime/unwrap! kv-bin)
    ;         (faketime/unwrap! db-bin)))
   )))

(defn region-ready?
  "Does the given region have enough replicas?"
  [region]
  (->> (:peers region)
       (remove :is_learner)
       count
       (<= replica-count)))

(defn wait-for-replica-count
  "TiDB start with a single replica, and only adds more later. We have to wait
  for it to do that before starting our tests if we want to be able to test
  things like failover. <sigh>"
  [node]
  (loop [tries 30]
    (when (zero? tries)
      (throw+ {:type :gave-up-waiting-for-replica-count}))

    (let [regions (await-http (pd-regions node))]
      ; (info :regions (with-out-str (pprint regions)))
      (info :region-replicas (->> (:regions regions)
                                  (map (fn [region]
                                         [(:id region)
                                          (->> (:peers region)
                                               (remove :is_learner)
                                               count)]))
                                  (into (sorted-map))
                                  pprint
                                  with-out-str
                                  clojure.string/trim))
      (if (every? region-ready? (:regions regions))
        true
        (do (Thread/sleep 10000)
            (recur (dec tries)))))))

(defn db
  "TiDB"
  []
  (reify db/DB
    (setup! [_ test node]
      (let [enable-system? (:enable-system-tidb test)]
        (info node "resetting TiDB")
        (c/su
          (stop! test node)
          (cleanup-db-runtime-artifacts!)
          (try+ (->> (cu/ls tidb-dir)
                     (remove #{"bin"})
                     (map (partial str tidb-dir "/"))
                     (c/exec :rm :-rf))
                (catch [:type :jepsen.control/nonzero-exit, :exit 2] e
                   ; No such dir
                  nil)))
        (c/su
          (install! test node)
          (configure!)
          (when enable-system?
            (configure-system-db!))
          (jepsen/synchronize test 180)

          (try+ (start-wait-pd! test node)
                ; If we don't synchronize, KV might explode because PD isn't
                ; fully available
                (jepsen/synchronize test)
                (Thread/sleep 5000)

                (start-wait-kv! test node)
                (jepsen/synchronize test)

                ; We have to wait for every region to become totally replicated
                ; before starting any TiDB instance: if we start TiDB first, it
                ; might take 80+ minutes to converge.
                (wait-for-replica-count node)
                (jepsen/synchronize test)

                (Thread/sleep 5000)

                (when enable-system?
                  (start-wait-system-db! test node)
                  (jepsen/synchronize test)
                  (Thread/sleep 10000))

                ; OK, now we can start the primary TiDB itself
                (start-wait-db! test node)

                (Thread/sleep 30000)

                ; For reasons I cannot explain, sometimes TiDB just... fails to
                ; reach a usable state despite waiting hundreds of seconds to
                ; open a connection. I've lowered the await-node timeout, and if
                ; we fail here, we'll nuke the entire setup process and try
                ; again. <sigh>
                (sql/await-node test node)

                (catch [:type :gave-up-waiting-for-replica-count] e
                  (throw+ {:type :jepsen.db/setup-failed}))

                (catch [:type :restart-loop-timed-out] e
                  (throw+ {:type :jepsen.db/setup-failed}))

                (catch [:type :connect-timed-out] e
                  ; sigh
                  (throw+ {:type :jepsen.db/setup-failed}))

                (catch java.sql.SQLException e
                  ; siiiiiiiigh
                  (throw+ {:type :jepsen.db/setup-failed}))))))

    (teardown! [_ test node])

    db/LogFiles
    (log-files [_ test node]
      (when-not (:skip-collect-logs test)
        (let [base (cond-> [db-log-file
                            db-slow-file
                            db-stdout
                            install-diagnostics-file
                            kv-log-file
                            kv-stdout]
                     (:enable-system-tidb test)
                     (into [system-db-log-file
                            system-db-slow-file
                            system-db-stdout]))
              pd-logs (if (:pd-services test)
                        [(get-in pd-services [:api :log-file])
                         (get-in pd-services [:api :stdout])
                         (get-in pd-services [:tso :log-file])
                         (get-in pd-services [:tso :stdout])
                         (get-in pd-services [:scheduling :log-file])
                         (get-in pd-services [:scheduling :stdout])]
                        [pd-log-file pd-stdout])]
          (concat base pd-logs))))))
