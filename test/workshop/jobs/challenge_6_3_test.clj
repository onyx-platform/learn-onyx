(ns workshop.jobs.challenge-6-3-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-6-3 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; In practice, you'll frequently need to respond to events
;; other than N number of segments processed. Let's explore
;; the watermark trigger. Watermark's represent the upper and
;; lower bounds of a window (say, 1:00 - 1:59:59...). Use
;; a fixed window of range 1 hour with a Watermark trigger.
;; Use the Conj aggregate to maintain all of the segments in
;; memory.

;; The idea with this challenge is that we pipe through the first
;; 8 segments, all within the 2:00 window, and then drop a segment
;; with timestamp 3:05. Onyx sees that this segment is beyond the
;; 2:00 window, so it fires *only* the 2 hour window.

;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-6-3-test`
;;

(def input
  [{:event-id 1 :event-time #inst "2015-11-20T02:59:00.000-00:00"}
   {:event-id 2 :event-time #inst "2015-11-20T02:46:00.000-00:00"}
   {:event-id 3 :event-time #inst "2015-11-20T02:31:00.000-00:00"}
   {:event-id 4 :event-time #inst "2015-11-20T02:54:00.000-00:00"}
   {:event-id 5 :event-time #inst "2015-11-20T02:00:00.000-00:00"}
   {:event-id 6 :event-time #inst "2015-11-20T02:05:00.000-00:00"}
   {:event-id 7 :event-time #inst "2015-11-20T02:11:00.000-00:00"}
   {:event-id 8 :event-time #inst "2015-11-20T02:18:00.000-00:00"}
   {:event-id 9 :event-time #inst "2015-11-20T03:05:00.000-00:00"}])

(def expected-output
  {[#inst "2015-11-20T02:00:00.000-00:00"
    #inst "2015-11-20T02:59:59.999-00:00"]
   [{:event-id 1
     :event-time #inst "2015-11-20T02:59:00.000-00:00"}
    {:event-id 2
     :event-time #inst "2015-11-20T02:46:00.000-00:00"}
    {:event-id 3
     :event-time #inst "2015-11-20T02:31:00.000-00:00"}
    {:event-id 4
     :event-time #inst "2015-11-20T02:54:00.000-00:00"}
    {:event-id 5
     :event-time #inst "2015-11-20T02:00:00.000-00:00"}
    {:event-id 6
     :event-time #inst "2015-11-20T02:05:00.000-00:00"}
    {:event-id 7
     :event-time #inst "2015-11-20T02:11:00.000-00:00"}
    {:event-id 8
     :event-time #inst "2015-11-20T02:18:00.000-00:00"}]})

(deftest test-level-6-challenge-3
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        n-peers (u/n-peers catalog c/workflow)
        p (promise)]

    (reset! c/fired-window-state {})

    (with-test-env
      [test-env [n-peers env-config peer-config]]

      (add-watch c/fired-window-state :watcher
                 (fn [k r old new]
                   ;; This trigger fires exactly once (for this data set)
                   ;; when we exceed the watermark with the last
                   ;; segment (at 3:05, outside of the 2:00 - 2:59:59 window).
                   ;;
                   ;; Triggers also fire on task completion, so technically
                   ;; the last segment we send through will be synced. We
                   ;; simply return the first value via the promise to avoid
                   ;; a race condition of seeing the next state sync.                   
                   (deliver p new)))
      
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :windows c/windows
                 :triggers c/triggers
                 :task-scheduler :onyx.task-scheduler/balanced}
            job-id (:job-id (onyx.api/submit-job peer-config job))]
        (feedback-exception! peer-config job-id)
        (is (= expected-output @p))))))
