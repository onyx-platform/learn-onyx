(ns workshop.challenge-4-2
  (:require [workshop.workshop-utils :as u]))

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

      {:onyx/name :identity
       :onyx/fn :workshop.challenge-4-2/my-identity-fn
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Returns the segment"}

      {:onyx/name :write-segments
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;

(defn my-identity-fn [segment]
  segment)

;;; Lifecycles ;;;

(def state (atom nil))

;; <<< BEGIN FILL ME IN >>>

(def state (atom nil))

(defn inject-state [event lifecycle]
  {:challenge/state state})

(defn compute-max [event lifecycle]
  (when (seq (:onyx.core/batch event))
    (let [max-val (apply max (map (comp :n :message) (:onyx.core/batch event)))]
      (swap! (:challenge/state event)
             (fn [state]
               (if (or (nil? state) (> max-val state))
                 max-val
                 state)))))
  {})

(defn inject-reader-ch [event lifecycle]
  {:core.async/chan (u/get-input-channel (:core.async/id lifecycle))})

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

(def aggregate-lifecycle
  {:lifecycle/before-task-start inject-state
   :lifecycle/after-batch compute-max})

(def reader-lifecycle
  {:lifecycle/before-task-start inject-reader-ch})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(defn build-lifecycles []
  [{:lifecycle/task :identity
    :lifecycle/calls :workshop.challenge-4-2/aggregate-lifecycle
    :onyx/doc "Computes an aggregate over the event stream"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :workshop.challenge-4-2/reader-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :workshop.challenge-4-2/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])

;; <<< END FILL ME IN >>>
