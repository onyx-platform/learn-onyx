(ns workshop.challenge-2-0
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :times-two]
   [:times-two :write-segments]])

;;; Catalogs ;;;

;;; <<< BEGIN READ ME >>>

;; Catalogs describe what tasks actually do and wire them up
;; to their concrete Clojure implementations. Here, we define
;; a function that returns a catalog. It takes two possible
;; parameters - batch-size and batch-timeout. These are both
;; throughput and latency tuning knobs.
;;
;; There are three "types" of catalog entries:
;; :input, :function, and :output. These are denoted by
;; :onyx/type.
(defn build-catalog
  ([] (build-catalog 5 50))
  ([batch-size batch-timeout]
     [;; The first catalog entry we'll look at is an :input
      ;; type. :input and :output catalog entries talk to plugins.
      ;; Plugins are the mechanism by which data is moved in and out of
      ;; Onyx. The important thing to note here is :onyx/plugin.
      ;; This is mapped to a predefined keyword (resolved to a fn in that namespace) 
      ;; by the plugin author. You'd typically find this
      ;; catalog entry in the README of the plugin repository.
      {:onyx/name :read-segments
       :onyx/plugin :onyx.plugin.core-async/input
       :onyx/type :input
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Reads segments from a core.async channel"}

      ;; Onyx function tasks require a :onyx/fn parameter to
      ;; be defined, which is a keyword that maps to a namespace and var name
      ;; The function's ns *must* be required onto the classpath before it is
      ;; resolved. Unless otherwise specified, it should take exactly one
      ;; parameter - a single segment.
      {:onyx/name :times-two
       :onyx/fn :workshop.challenge-2-0/times-two
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Multiplies :n in the segment by 2"}

      ;; The last catalog entry to observe is the :output type.
      ;; As with the :input type, the critical piece is :onyx/plugin.
      ;; This maps to a predefined keyword in the plugin. Under the
      ;; covers, a multimethod is dispatching and looking for this keyword.
      {:onyx/name :write-segments
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; <<< END READ ME >>>

;;; Functions ;;;

(defn times-two [segment]
  (update-in segment [:n] (partial * 2)))

;;; Lifecycles ;;;

(defn inject-reader-ch [event lifecycle]
  {:core.async/chan (u/get-input-channel (:core.async/id lifecycle))})

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

(def reader-lifecycle
  {:lifecycle/before-task-start inject-reader-ch})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.challenge-2-0/reader-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :workshop.challenge-2-0/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])
