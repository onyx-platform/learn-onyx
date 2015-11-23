(ns workshop.challenge-6-1
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :bucket-page-views]])

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
      
      {:onyx/name :bucket-page-views
       :onyx/plugin :onyx.peer.function/function
       :onyx/fn :clojure.core/identity
       :onyx/type :output
       :onyx/medium :function
       :onyx/uniqueness-key :event-id
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Identity function, used for windowing segments unchanged."}]))

;;; Lifecycles ;;;

(defn inject-reader-ch [event lifecycle]
  {:core.async/chan (u/get-input-channel (:core.async/id lifecycle))})

(def reader-lifecycle
  {:lifecycle/before-task-start inject-reader-ch})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.challenge-6-1/reader-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}])

;; <<< BEGIN FILL ME IN PART 1 >>>

(def windows
  [])

;; <<< END FILL ME IN PART 1 >>>

(def triggers
  [{:trigger/window-id :collect-segments
    :trigger/refinement :accumulating
    :trigger/on :segment
    :trigger/fire-all-extents? true
    :trigger/threshold [10 :elements]
    :trigger/sync ::deliver-promise!
    :trigger/doc "Fires against all extents every 10 segments processed."}])

(def fired-window-state (atom {}))

(defn deliver-promise! [event window-id lower-bound upper-bound state]
  ;; <<< BEGIN FILL ME IN PART 2 >>>

  ;; <<< END FILL ME IN PART 2 >>>
  )
