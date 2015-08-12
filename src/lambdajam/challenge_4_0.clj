(ns lambdajam.challenge-4-0
  (:require [lambdajam.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :times-two]
   [:times-two :write-segments]])

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

      {:onyx/name :times-two
       :onyx/fn :lambdajam.challenge-4-0/times-two
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Multiplies :n in the segment by 2"}

      {:onyx/name :write-segments
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;

(defn times-two [segment]
  (update-in segment [:n] (partial * 2)))

;;; Lifecycles ;;;

;;; <<< BEGIN READ ME >>>

;; Lifecycles are an important part of Onyx that allow you to
;; make limited modifications to tasks. This is especially useful
;; when you're working with 3rd party code.
;;
;; There are 5 lifecycle points that can be leveraged:
;;
;; - :lifecycle/start-task? 
;; - :lifecycle/before-task-start
;; - :lifecycle/after-task-stop
;; - :lifecycle/before-batch
;; - :lifecycle/after-batch
;; 
;; All lifecycle functions except :start-task? *must* return maps.
;; These maps are merged back into the main event map, and the
;; returned map's keys get priority over any pre-existing keys.
;; 
;; :start-task? should return a boolean value. If this value returns
;; false, the task backs off for a predetermined amount of time
;; and calls this function again until it returns true. This is useful
;; for acquiring locks.
;;
;; Lifecycle functions take two parameters: the "event" map, and the
;; lifecycle map. You can leverage both of them to runtime parameterize
;; your job. Examples will follow.
;;

(def logger (agent nil))

;; Below are the functions that are invoked during the lifecycle.
;; They all take two parameters, and return maps. The last two
;; return new values that are merged into the maps and used later.

;; Log when the task starts. Return no new values for the event map.
(defn log-task-start [event lifecycle]
  (send logger (fn [_] (println "Starting task: " (:onyx.core/task event))))
  {})

;; Log when the task stops. Return no new values for the event map.
(defn log-task-stop [event lifecycle]
  (send logger (fn [_] (println "Stopping task: " (:onyx.core/task event))))
  {})

;; This function is used for the core.async input plugin. We use
;; a memoized function to obtain a reference to the channel and add
;; the core.async channel into the event map so that peers can read
;; from it.
(defn inject-reader-ch [event lifecycle]
  {:core.async/chan (u/get-input-channel (:core.async/id lifecycle))})

;; We do the same here as above, except for a writer output channel.
(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

;; Now we take the functions and tell Onyx when to invoke them in
;; the lifecycles. We map the predefined keywords to Clojure functions.

(def logger-lifecycle
  {:lifecycle/before-task-start log-task-start
   :lifecycle/after-task-stop log-task-stop})

(def reader-lifecycle
  {:lifecycle/before-task-start inject-reader-ch})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

;; Finally, we craft the data structure that we send to Onyx along
;; with our job.
(defn build-lifecycles []
  [;; We name the task that this lifecycle should be applied to
   ;; and name a keyword that points to the event lifecycle map.
   ;; A docstring is always helpful too.
   {:lifecycle/task :times-two
    :lifecycle/calls :lambdajam.challenge-4-0/logger-lifecycle
    :onyx/doc "Logs messages about the task"}

   ;; We get a little creative to make the core.async plugin easier
   ;; to use. We add our own core.async/id parameter to the lifecycle
   ;; and reference this later to obtain a shared reference to a channel
   ;; via memoization.
   {:lifecycle/task :read-segments
    :lifecycle/calls :lambdajam.challenge-4-0/reader-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :lambdajam.challenge-4-0/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])

;;; <<< END READ ME >>>
