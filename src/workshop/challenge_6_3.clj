(ns workshop.challenge-6-3
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
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Identity function, used for windowing segments unchanged."}]))

;;; Lifecycles ;;;

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.workshop-utils/in-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}])

;; <<< BEGIN FILL ME IN PART 1 >>>

(def windows
  [{:window/id :collect-segments
    :window/task :bucket-page-views
    :window/type :fixed
    :window/aggregation :onyx.windowing.aggregation/conj
    :window/window-key :event-time
    :window/range [1 :hour]
    :window/doc "Conj's segments in one hour fixed windows."}])

;; <<< END FILL ME IN PART 1 >>>

;; <<< BEGIN FILL ME IN PART 2 >>>

(def triggers
  [{:trigger/window-id :collect-segments
    :trigger/id :sync-collect
    :trigger/refinement :onyx.refinements/accumulating
    :trigger/on :onyx.triggers/watermark
    :trigger/sync ::deliver-promise!
    :trigger/doc "Fires when this window's watermark has been exceeded"}])

;; <<< END FILL ME IN PART 2 >>>

(def fired-window-state (atom {}))

(defn deliver-promise! [event window trigger {:keys [window-id lower-bound upper-bound]} state]
  ;; <<< BEGIN FILL ME IN PART 3 >>>
  (let [lower (java.util.Date. lower-bound)
        upper (java.util.Date. upper-bound)]
    (swap! fired-window-state assoc [lower upper] state))
  ;; <<< END FILL ME IN PART 3 >>>
  )
