(ns workshop.challenge-3-0
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :alternate-case]
   [:alternate-case :exclaim]
   [:exclaim :write-segments]])

;;; Catalogs ;;;

(defn build-catalog
  ([] (build-catalog 5 50))
  ([batch-size batch-timeout]
     [{:onyx/name :read-segments
       :onyx/plugin :onyx.plugin.core-async/input
       :onyx/type :input
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Reads segments from a core.async channel"}

      {:onyx/name :alternate-case
       :onyx/fn :workshop.challenge-3-0/alternate-case
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Alternates capitalization in :name"}

      {:onyx/name :exclaim
       :onyx/fn :workshop.challenge-3-0/exclaim
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Adds a bang to the end of :name"}

      {:onyx/name :write-segments
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;

;;; <<< BEGIN READ ME >>>

;; Functions in Onyx are literally Clojure functions.
;; In the typical case, the contract that they obey
;; is that they take exactly one segment (a plain Clojure map)
;; as an argument, and return exactly one Clojure map to be
;; sent to the next tasks. This is the basic principle.
;; They can do a little more though, which we'll describe next.
;;
;; Functions may also return a seq of segments, which will
;; cause all segments to be emitted to the next tasks.
;;
;; Functions may be parameterized. It's good practice to take
;; all values and state that a function needs through parameters,
;; never reaching into the global context. Parameters can be added
;; in a few ways, but typically it's most convenient to use
;; :onyx/params in the catalog entry. Parameters are prepended
;; to *the front* of the function signature. The segment always
;; comes last. That is, a function with two added parameters
;; will have signature:
;;
;; (fn name [param-1 param-2 segment])

;; Our first function, used by catalog entry :alternate-case.
;; It takes a segment (a Clojure map) as its argument - so we can
;; destructure it. It returns a map.
(defn alternate-case [{:keys [name]}]
  {:name
   (->> (cycle [(memfn toUpperCase) (memfn toLowerCase)])
        (map #(%2 (str %1)) name)
        (apply str))})

;; Usually, it's a good idea to apply one or more transformations
;; to the map's keys and return the entire map. That way, each function
;; can simply pass through keys that it doesn't need to know about.
(defn exclaim [segment]
  (update-in segment [:name] #(str % "!")))

;;; <<< END READ ME >>>

;;; Lifecycles ;;;

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.workshop-utils/in-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :workshop.challenge-3-0/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])
