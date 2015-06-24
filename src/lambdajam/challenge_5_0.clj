(ns lambdajam.challenge-5-0
  (:require [lambdajam.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :squared]
   [:squared :write-even-segments]
   [:squared :write-odd-segments]])

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

      {:onyx/name :squared
       :onyx/fn :lambdajam.challenge-5-0/squared
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Multiplies :n in the segment by itself"}

      {:onyx/name :write-even-segments
       :onyx/ident :core.async/write-to-chan
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}

      {:onyx/name :write-odd-segments
       :onyx/ident :core.async/write-to-chan
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;

(defn squared [segment]
  (update-in segment [:n] (fn [n] (* n n))))

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
    :lifecycle/calls :lambdajam.challenge-5-0/reader-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-even-segments
    :lifecycle/calls :lambdajam.challenge-5-0/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-even-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-odd-segments
    :lifecycle/calls :lambdajam.challenge-5-0/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-odd-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])

;;; <<< BEGIN READ ME >>>

;; If a workflow data structure represents the nodes in the
;; computational graph (with implicit edges to all immediate
;; nodes), then flow conditions are the explicit edges. We should
;; start out by saying that flow conditions are entirely optional.

;; Flow conditions route segments to specific workflow graph nodes
;; based off of predicates. Predicate functions are resolved in the
;; same way that catalog entry functions are resolved - via fully
;; namespaced qualified keywords.

;; Predicate function signatures take 4 arguments:
;;
;; - event: the event context map seen in lifecycle hooks
;; - old-segment: the segment *before* it entered the task
;; - new-segment: the segment *after* it entered the task
;; - all-new-segments: Remember that tasks can emit multiple segments
;;                     from a single segment by returning a vector.
;;                     This parameter contains all the new segments
;;                     produced from this segment in a vector.
;;
;; If the predicate function returns true, the flow conditions routes
;; accordingly. If not, the segment is not sent to the specified downstream
;; task.
(defn even-segment? [event old-segment new-segment all-new-segments]
  (even? (:n new-segment)))

(defn odd-segment? [event old-segment new-segment all-new-segments]
  (odd? (:n new-segment)))

;; The flow conditions data structure is a vector of maps. You name
;; which node segments are processed at, and which nodes segments
;; should be emitted to if :flow/predicate returns true. You can
;; specific :all or :none for :flow/to as a convenience, or a vector
;; of tasks to send to more than one task.
;;
;; Flow conditions support a form of pattern matching. Each flow condition
;; entry is scanned in order to try and match the segment. If no entries
;; match, the segment is dropped. If multiple entries match, the segment
;; is only sent once to the specified task.
(def flow-conditions
  [{:flow/from :squared
    :flow/to [:write-even-segments]
    :flow/predicate :lambdajam.challenge-5-0/even-segment?
    :flow/doc "Route to :write-even-segments if :in in this segment is even"}

   {:flow/from :squared
    :flow/to [:write-odd-segments]
    :flow/predicate :lambdajam.challenge-5-0/odd-segment?
    :flow/doc "Route to :write-odd-segments if :in in this segment is odd"}])

;;; <<< END READ ME >>>
