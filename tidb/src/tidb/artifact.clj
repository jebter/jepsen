(ns tidb.artifact
  (:require [cheshire.core :as json]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as str]
            [jepsen.store :as store]))

(defn- json-path-for
  [path]
  (let [filename (last path)]
    (conj (vec (butlast path))
          (if (.endsWith filename ".edn")
            (str (subs filename 0 (- (count filename) 4)) ".json")
            (str filename ".json")))))

(defn write-edn+json!
  [test path value]
  (store/with-out-file test path
    (pprint value))
  (spit (store/path! test (json-path-for path))
        (str (json/generate-string value {:pretty true}) "
"))
  {:edn-path  (str/join "/" path)
   :json-path (str/join "/" (json-path-for path))
   :value     value})
