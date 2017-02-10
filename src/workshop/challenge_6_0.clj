(ns workshop.challenge-6-0
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

;; <<< BEGIN READ ME PART 1 >>>

;; We're going to do something a little different with this workflow
;; from what we've seen in the past. Since our challenge is to maintain
;; an aggregate, there's no data that we'll want to send to a downstream task.
;; Therefore, our workflow is simply a two-node graph sending data from an input
;; task to the aggregation task :bucket-page-views. :bucket-page-views is a
;; function that acts as an output. It doesn't send data anywhere - it simply
;; drops it on the floor.

(def workflow
  [[:read-segments :bucket-page-views]])

;; <<< END READ ME PART 1 >>>

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

      ;; <<< BEGIN READ ME PART 2 >>>

      ;; There are a few special keys we need to set
      ;; since this is a windowed task, and also a function
      ;; acting as an output that discards data.
      
      {:onyx/name :bucket-page-views
       ;; :onyx.peer.function/function is a value for plugins
       ;; which are functions in the leaf position of a workflow.
       :onyx/plugin :onyx.peer.function/function

       ;; We don't want to transform the data in this case, so
       ;; we merely use the identity function.
       :onyx/fn :clojure.core/identity

       ;; Note that even though this is a function, this is still
       ;; a task in the leaf position. Therefore, we must indicate
       ;; that this is an output task.
       :onyx/type :output

       ;; While there are no functional effects at this time by
       ;; setting :onyx/medium, it's convention to simply say
       ;; that the output medium is a :function.
       :onyx/medium :function

       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Identity function, used for windowing segments unchanged."}
      
      ;; <<< END READ ME PART 2 >>>
      ]))

;;; Lifecycles ;;;

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.workshop-utils/in-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}])

;; <<< BEGIN READ ME PART 3 >>>

;; With the initial set up in the catalog out of the way,
;; we can get onto the meat of this challenge - the window
;; and trigger constructs. Windows indicate *how* to accrete
;; state over time. Triggers indicate *what* to do with the data
;; in reaction to a specific stimulus.

(def windows
  ;; :window/id is a unique name for this window. We need it
  ;; to link it up with one or more triggers, seen below.
  [{:window/id :collect-segments

    ;; :window/task names a task in the catalog for which we'll
    ;; maintain state over.
    :window/task :bucket-page-views

    ;; The "type" of window we're using for this challenge is a fixed
    ;; window. Fixed windows span a particular duration, and do not
    ;; overlap with other windows. See the User Guide for a full explanation
    ;; of window types.
    :window/type :fixed

    ;; This window will "conj" values into a buffer - meaning they won't
    ;; be compacted. Be careful when using this that you don't end up
    ;; hogging memory - flush the window state often with a trigger!
    :window/aggregation :onyx.windowing.aggregation/conj

    ;; Windows *do not* bucket time based on the wall clock of the machine
    ;; executing the task. Instead, we can do something much more powerful.
    ;; Here, we indicate a "window key". This lets us peek into the segment
    ;; itself and pull out the :event-time key. We use *this* value to bucket
    ;; the segment. Onyx is also capable of bucketing by wall clock (normally
    ;; called "Processing Time") by using a Timer Trigger. See the User Guide for
    ;; more information on this.
    :window/window-key :event-time

    ;; This window should span one hour. This is how we derive 3 distinct buckets
    ;; for our toy data set.
    :window/range [1 :hour]
    :window/doc "Conj's segments into one hour fixed windows."}])

(def triggers
  ;; Each trigger stimulates a particular window. This must match a :window/id.
  [{:trigger/window-id :collect-segments
    :trigger/id :sync

    ;; When the trigger "fires", we refine the window state. In this example,
    ;; we'll leave our state untouched and simply accumulate it. We can also
    ;; choose to use a :discarding refinement mode - throwing away the state
    :trigger/refinement :onyx.refinements/accumulating

    ;; Different "types" of trigger react to different stimuli. This is a
    ;; segment-based trigger. It fires each time it sees N segments. After
    ;; it fires, it resets its counter and repeats the same process.
    :trigger/on :onyx.triggers/segment

    ;; When set to true, if any "extent", an instance of a window, fires, all
    ;; extents will fire. This is desirable behavior for segment-based triggers,
    ;; so we turn it on.
    :trigger/fire-all-extents? true

    ;; Fire the trigger every 10 segments we see. This corresponds to the number
    ;; of items in our toy data set - which gives us the predictability of
    ;; knowing that it will fire 3 times.
    :trigger/threshold [10 :elements]

    ;; The final piece of triggers are figuring out what to *do* with the
    ;; content contents. This sync function points to a user defined function,
    ;; seen below.
    :trigger/sync ::deliver-promise!
    :trigger/doc "Fires against all extents every 10 segments processed."}])

;; One last piece of state we maintain, for this challenge, is an atom.
;; We want to globally track all fired trigger state so that we can verify
;; it from our tests.
(def fired-window-state (atom {}))

;; deliver-promise! is the function refered to by :trigger/sync above.
;; Syncing functions take 5 arguments.
;; The window ID is an internally unique value used to track
;; this specific window extent. The upper and lower bounds are, in this
;; case, UNIX timestamps indicating the boundaries for this extent. We
;; convert these to java.util.Date objects for readability. We swap
;; the state into the atom, which we deref in the tests. We expect
;; to see a map with three keys (one for each hour), and every piece
;; of the data present exactly once, sinced Fixed windows do not overlap.
;;
;; While we use an atom here for ease, this is the ideal place to synchronize
;; window state to an external database or other 3rd party medium.
(defn deliver-promise! [event window  {:keys [trigger/window-id] :as trigger} {:keys [lower-bound upper-bound] :as state-event} state]
  (let [lower (java.util.Date. lower-bound)
        upper (java.util.Date. upper-bound)]
    (println "Trigger for" window-id "window")    
    (swap! fired-window-state assoc [lower upper] (into #{} state))))

;; Finally, it's important to note that if a machine fails before
;; window data is flushed with a trigger, it is *not* lost. All window
;; contents are durably written to disk using Apache BookKeeper. In this
;; workshop, we're running BookKeeper in memory for you. See the User Guide
;; for our recommendations on setting this up in production.

;; <<< END READ ME PART 3 >>>
