(ns lambdajam.challenge-0-0
  (:require [lambdajam.workshop-utils :as u]))

(def batch-size 20)

;;; Workflows ;;;

(def workflow
  [[:read-segments :no-transform]
   [:no-transform :write-segments]])

;;; Catalogs ;;;

(def catalog
  [{:onyx/name :read-segments
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :no-transform
    :onyx/fn :lambdajam.challenge-0-0/no-transform
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/doc "Returns segments unchanged."}

   {:onyx/name :write-segments
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

;;; Functions ;;;

(defn no-transform
  "The identity functions. Takes a segment and returns its value."
  [segment]
  segment)

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
    :lifecycle/calls :lambdajam.challenge-0-0/reader-lifecycle
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :lambdajam.challenge-0-0/writer-lifecycle
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "core.async plugin base lifecycle"}])
