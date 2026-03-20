(ns jepsen.control.util
  "Utility functions for scripting installations."
  (:require [jepsen.control :refer :all]
            [jepsen.util :refer [meh]]
            [clojure.data.codec.base64 :as b64]
            [clojure.java.io :refer [file]]
            [clojure.tools.logging :refer [info warn]]
            [clojure.string :as str]
            [slingshot.slingshot :refer [try+ throw+]]))

(def tmp-dir-base "Where should we put temporary files?" "/tmp/jepsen")

(defn file?
  [filename]
  (throw (RuntimeException. "Use exists? instead; file? will be used to tell if
                            something is a file, as opposed to a directory or
                            link.")))

(defn exists?
  "Is a path present?"
  [filename]
  (try (exec :stat filename)
       true
       (catch RuntimeException _ false)))

(defn ls
  "A seq of directory entries (not including . and ..). TODO: escaping for
  control chars in filenames (if you do this, WHO ARE YOU???)"
  ([] (ls "."))
  ([dir]
   (->> (str/split (exec :ls :-A dir) #"\n")
        (remove str/blank?))))

(defn ls-full
  "Like ls, but prepends dir to each entry."
  [dir]
  (let [dir (if (re-find #"/$" dir)
              dir
              (str dir "/"))]
    (->> dir
         ls
         (map (partial str dir)))))

(defn tmp-dir!
  "Creates a temporary directory under /tmp/jepsen and returns its path."
  []
  (let [dir (str tmp-dir-base "/" (rand-int Integer/MAX_VALUE))]
    (if (exists? dir)
      (recur)
      (do
        (exec :mkdir :-p dir)
        dir))))

(def std-wget-opts
  "A list of standard options we pass to wget"
  [:--tries 20
   :--waitretry 60
   :--retry-connrefused
   :--dns-timeout 60
   :--connect-timeout 60
   :--read-timeout 60])

(defn wget-helper!
  "A helper for wget! and cached-wget!. Calls wget with options; catches name
  resolution and other network errors, and retries them. EC2 name resolution
  can be surprisingly flaky."
  [& args]
  (let [url          (last args)
        max-retries  5]
    (loop [tries max-retries
           attempt 1]
      (let [res (try+
                  (exec :wget args)
                  (catch [:type :jepsen.control/nonzero-exit, :exit 4] e
                    (if (pos? tries)
                      (do (warn "jepsen.install" {:stage :download-retry
                                                  :url url
                                                  :attempt attempt
                                                  :retries-left tries
                                                  :exit (:exit e)})
                          ::retry)
                      (throw e))))]
        (if (= ::retry res)
          (recur (dec tries) (inc attempt))
          res)))))

(defn wget!
  "Downloads a string URL and returns the filename as a string. Skips if the
  file already exists."
  ([url]
   (wget! url false))
  ([url force?]
   (let [filename (.getName (file url))]
     (when force?
       (exec :rm :-f filename))
     (when (not (exists? filename))
       (wget-helper! std-wget-opts url))
     filename)))

(def wget-cache-dir
  "Directory for caching files from the web."
  (str tmp-dir-base "/wget-cache"))

(defn elapsed-ms
  [started-at]
  (long (/ (double (- (System/nanoTime) started-at)) 1000000.0)))

(defn install-log
  [stage fields]
  (info "jepsen.install" (merge {:stage stage} fields)))

(defn rethrow-any!
  [e]
  (if (instance? Throwable e)
    (throw e)
    (throw+ e)))

(defn cached-wget!
  "Downloads a string URL to the Jepsen wget cache directory, and returns the
  full local filename as a string. Skips if the file already exists. Local
  filenames are base64-encoded URLs, as opposed to the name of the file--this
  is helpful when you want to download a package like
  https://foo.com/v1.2/foo.tar; since the version is in the URL but not a part
  of the filename, downloading a new version could silently give you the old
  version instead.

  Options:

    :force?     Even if we have this cached, download the tarball again anyway."
  ([url]
   (cached-wget! url {:force? false}))
  ([url opts]
   (let [encoded-url (String. (b64/encode (.getBytes url)) "UTF-8")
         dest-file   (str wget-cache-dir "/" encoded-url)]
     (when (:force? opts)
       (install-log :download-cache-cleared {:url url
                                             :dest-file dest-file})
       (exec :rm :-rf dest-file))
     (if (exists? dest-file)
       (install-log :download-cache-hit {:url url
                                         :dest-file dest-file})
       (let [started-at (System/nanoTime)]
         (install-log :download-start {:url url
                                       :dest-file dest-file})
         (try+
           (do (exec :mkdir :-p wget-cache-dir)
               (cd wget-cache-dir
                   (wget-helper! std-wget-opts :-O dest-file url))
               (install-log :download-complete {:url url
                                                :dest-file dest-file
                                                :elapsed-ms (elapsed-ms started-at)}))
           (catch Object e
             (install-log :download-failed {:url url
                                            :dest-file dest-file
                                            :failure-bucket :download-failed
                                            :elapsed-ms (elapsed-ms started-at)})
             (rethrow-any! e)))))
     dest-file)))

(defn install-archive!
  "Gets the given tarball URL, caching it in /tmp/jepsen/, and extracts its
  sole top-level directory to the given dest directory. Deletes
  current contents of dest. Supports both zip files and tarballs, compressed or
  raw. Returns dest.

  Standard practice for release tarballs is to include a single directory,
  often named something like foolib-1.2.3-amd64, with files inside it. If only
  a single directory is present, its *contents* will be moved to dest, so
  foolib-1.2.3-amd64/my.file becomes dest/my.file. If the tarball includes
  multiple files, those files are moved to dest, so my.file becomes
  dest/my.file."
  ([url dest]
   (install-archive! url dest false))
  ([url dest force?]
   (let [started-at  (System/nanoTime)
         local-file  (nth (re-find #"file://(.+)" url) 1)
         file        (or local-file (cached-wget! url {:force? force?}))
         tmpdir      (tmp-dir!)
         dest        (expand-path dest)
         archive-kind (if (re-find #".*\.zip$" url) :zip :tar)]

     (install-log :archive-install-start {:url url
                                          :dest dest
                                          :source (if local-file :local-file :cache)
                                          :archive-kind archive-kind})

     ; Clean up old dest and make sure parent directory is ready
     (exec :rm :-rf dest)
     (let [parent (exec :dirname dest)]
       (exec :mkdir :-p parent))

     (try+
       (cd tmpdir
           ; Extract archive to tmpdir
           (install-log :archive-extract-start {:url url
                                                :dest dest
                                                :archive-file file
                                                :archive-kind archive-kind})
           (if (re-find #".*\.zip$" url)
             (exec :unzip file)
             (exec :tar :--no-same-owner :--no-same-permissions
                   :--extract :--file file))
           (install-log :archive-extract-complete {:url url
                                                   :dest dest
                                                   :archive-file file})

           ; Force ownership
           (when (= "root" *sudo*)
             (exec :chown :-R "root:root" "."))

           ; Get archive root paths
           (let [roots (ls)]
             (assert (pos? (count roots)) "Archive contained no files")
             (install-log :archive-roots-detected {:url url
                                                   :dest dest
                                                   :roots roots})

             (if (= 1 (count roots))
               ; Move root's contents to dest
               (exec :mv (first roots) dest)

               ; Move all roots to dest
               (exec :mv tmpdir dest))))

       (install-log :archive-install-complete {:url url
                                               :dest dest
                                               :archive-file file
                                               :elapsed-ms (elapsed-ms started-at)})

       (catch [:type :jepsen.control/nonzero-exit] e
         (let [err (:err e)]
           (if (or (re-find #"tar: Unexpected EOF" err)
                   (re-find #"This does not look like a tar archive" err))
             (if local-file
               ; Nothing we can do to recover here
               (throw (RuntimeException.
                        (str "Local archive " local-file " on node "
                             *host*
                             " is corrupt: " err)))
               ; Retry download once; maybe it was abnormally terminated
               (do (install-log :archive-corrupt-download-retry {:url url
                                                                 :dest dest
                                                                 :archive-file file
                                                                 :elapsed-ms (elapsed-ms started-at)})
                   (exec :rm :-rf file)
                   (install-archive! url dest force?)))

             ; Throw by default
             (do (install-log :archive-install-failed {:url url
                                                       :dest dest
                                                       :archive-file file
                                                       :failure-bucket :extract-failed
                                                       :elapsed-ms (elapsed-ms started-at)
                                                       :exit (:exit e)})
                 (throw+ e)))))

       (catch Object e
         (install-log :archive-install-failed {:url url
                                               :dest dest
                                               :archive-file file
                                               :failure-bucket :extract-failed
                                               :elapsed-ms (elapsed-ms started-at)})
         (rethrow-any! e))

       (finally
         ; Clean up tmpdir
         (exec :rm :-rf tmpdir))))
   dest))

(defn install-tarball!
  ([node url dest]
   (install-tarball! node url dest false))
  ([node url dest force?]
   (warn "DEPRECATED: jepsen.control.util/install-tarball! is now named jepsen.control.util/install-archive!, and the `node` argument is no longer required.")
   (install-archive! url dest force?)))

(defn ensure-user!
  "Make sure a user exists."
  [username]
  (try (su (exec :adduser :--disabled-password :--gecos (lit "''") username))
       (catch RuntimeException e
         (when-not (re-find #"already exists" (.getMessage e))
           (throw e))))
  username)

(defn grepkill!
  "Kills processes by grepping for the given string."
  ([pattern]
   (grepkill! 9 pattern))
  ([signal pattern]
   ; Hahaha we'd like to use pkill here, but because we run sudo commands in a
   ; bash wrapper (`bash -c "pkill ..."`), we'd end up matching the bash wrapper
   ; and killing that as WELL, so... grep and awk it is! The grep -v makes sure
   ; we don't kill the grep process OR the bash process executing it.
   (try+ (exec :ps :aux
               | :grep pattern
               | :grep :-v "grep"
               | :awk "{print $2}"
               | :xargs :--no-run-if-empty :kill (str "-" signal))
         (catch [:type :jepsen.control/nonzero-exit, :exit 0] _
           nil))))

(defn start-daemon!
  "Starts a daemon process, logging stdout and stderr to the given file.
  Invokes `bin` with `args`. Options are:

  :background?
  :chdir
  :logfile
  :make-pidfile?
  :match-executable?
  :match-process-name?
  :pidfile
  :env
  :process-name"
  [opts bin & args]
  (let [bin (if (and (:chdir opts) (not (.isAbsolute (file bin))))
              (.getPath (file (:chdir opts) bin))
              bin)]
    (info "starting" (:process-name opts (.getName (file bin))))
    (exec :echo (lit "`date +'%Y-%m-%d %H:%M:%S'`")
        "Jepsen starting" bin (escape args)
        :>> (:logfile opts))
    (apply exec (map #(lit (str (name (first %)) "=" (escape (second %)))) (:env opts)) :start-stop-daemon :--start
           (when (:background? opts true) [:--background :--no-close])
           (when (:make-pidfile? opts true) :--make-pidfile)
           (when (:match-executable? opts true) [:--exec bin])
           (when (:match-process-name? opts false)
             [:--name (:process-name opts (.getName (file bin)))])
           :--pidfile  (:pidfile opts)
           :--chdir    (:chdir opts)
           :--oknodo
           :--startas  bin
           :--
           (concat args [:>> (:logfile opts) (lit "2>&1")]))))

(defn stop-daemon!
  "Kills a daemon process by pidfile, or, if given a command name, kills all
  processes with that command name, and cleans up pidfile."
  ([pidfile]
   (when (exists? pidfile)
     (info "Stopping" pidfile)
     (let [pid (Long/parseLong (exec :cat pidfile))]
       (meh (exec :kill :-9 pid))
       (meh (exec :rm :-rf pidfile)))))

  ([cmd pidfile]
   (info "Stopping" cmd)
   (meh (exec :killall :-9 :-w cmd))
   (meh (exec :rm :-rf pidfile))))

(defn daemon-running?
  "Given a pidfile, returns true if the pidfile is present and the process it
  contains is alive, nil if the pidfile is absent, false if it's present and
  the process doesn't exist.

  Strictly this doesn't mean the process is RUNNING; it could be asleep or a
  zombie, but you know what I mean. ;-)"
  [pidfile]
  (when-let [pid (meh (exec :cat pidfile))]
    (try (exec :ps :-o "pid=" :-p pid)
         (catch RuntimeException e
           false))))

(defn signal!
  "Sends a signal to a named process by signal number or name."
  [process-name signal]
  (meh (exec :pkill :--signal signal process-name))
  :signaled)
