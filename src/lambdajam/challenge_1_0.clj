(ns lambdajam.challenge-1-0
  (:require [lambdajam.workshop-utils :as u]))

(def batch-size 20)

;;; Workflows ;;;

(def workflow
  [[:read-segments :increment-n]
   [:increment-n :square-n]
   [:square-n :write-segments]])

;;; Catalogs ;;;

(defn build-catalog
  ([] (build-catalog 5 50))
  ([batch-size batch-timeout]
     [{:onyx/name :read-segments
       :onyx/ident :core.async/read-from-chan
       :onyx/type :input
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Reads segments from a core.async channel"}

      {:onyx/name :increment-n
       :onyx/fn :lambdajam.challenge-1-0/increment-n
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Increments :n in the segment by 1"}

      {:onyx/name :square-n
       :onyx/fn :lambdajam.challenge-1-0/square-n
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Multiplies :n in the segment by itself"}

      {:onyx/name :write-segments
       :onyx/ident :core.async/write-to-chan
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;

(defn increment-n [segment]
  (update-in segment [:n] inc))

(defn square-n [segment]
  (update-in segment [:n] (partial * (:n segment))))

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
    :lifecycle/calls :lambdajam.challenge-1-0/reader-lifecycle
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :lambdajam.challenge-1-0/writer-lifecycle
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "core.async plugin base lifecycle"}])