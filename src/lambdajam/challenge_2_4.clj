(ns lambdajam.challenge-2-4
  (:require [lambdajam.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :identity]
   [:identity :write-segments]])

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

      ;; <<< BEGIN FILL ME IN >>>

      {:onyx/name :identity
       :onyx/fn :clojure.core/identity
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 3
       :onyx/doc "Passes the segment through unchanged"}

      ;; <<< END FILL ME IN >>>

      {:onyx/name :write-segments
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Lifecycles ;;;

(def printer (agent nil))

(defn inject-reader-ch [event lifecycle]
  {:core.async/chan (u/get-input-channel (:core.async/id lifecycle))})

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

(defn echo-task-name [event lifecycle]
  (send printer
        (fn [_]
          (println (format "Peer executing task %s" (:onyx.core/task event)))))
  {})

(def reader-lifecycle
  {:lifecycle/before-task-start inject-reader-ch})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(def echo-task-lifecycle
  {:lifecycle/before-task-start echo-task-name})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :lambdajam.challenge-2-4/reader-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :lambdajam.challenge-2-4/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :lambdajam.challenge-2-4/echo-task-lifecycle
    :onyx/doc "Echos the task's name on boot up"}

   {:lifecycle/task :identity
    :lifecycle/calls :lambdajam.challenge-2-4/echo-task-lifecycle
    :onyx/doc "Echos the task's name on boot up"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :lambdajam.challenge-2-4/echo-task-lifecycle
    :onyx/doc "Echos the task's name on boot up"}])