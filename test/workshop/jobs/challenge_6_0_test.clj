(ns workshop.jobs.challenge-6-0-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-6-0 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; One of the major problems that we haven't tackled yet are
;; fault tolerant, stateful streaming computations. Previously,
;; we used in-memory atoms to count values and maintain general
;; aggregates over a stream of data. This works well enough for some
;; uses cases, but it has its limitations.

;; To achieve fault tolerance, Onyx will replay segments from the root
;; input task of which it was emitted if the segment failed to complete
;; within a configurable duration (the default is 60 seconds). It could be
;; the case that segments successfully make it through some of your stateful
;; tasks, only to fail later in the workflow. The segments would then pass
;; through your stateful tasks again. Even if Onyx didn't use this specific
;; technique for fault tolerance, this would be still be problematic because
;; it is provably impossible to execute a distributed instruction exactly once -
;; see the Two Generals problem for why this is the case.

;; Clearly, many computations need to have perfectly correct aggregate answers,
;; even in the face of machine failure. We tackled this problem in Onyx and
;; made a set of first class features to support it. These features are Windows,
;; Triggers, and Refinement Modes. While the idea of windowing is relatively
;; straightforward, the number of combinations between how each of these pieces
;; can be put together merits a close read of the documentation.

;; You can read about Windowing at:
;; http://www.onyxplatform.org/docs/user-guide/latest/windowing.html

;; After you've read about Windowing, it's important to understand Triggers:
;; http://www.onyxplatform.org/docs/user-guide/latest/triggers.html

;; You'll notice that in this example, we use more stateful constructs like
;; atoms and promises. Some of this is used to coordinate knowledge of
;; completion. We'll explain what we're doing in each step for this challenge,
;; since we understand that it increases the level of complexity as a learner.
;; If you get stuck reading any particular part, keep moving. You might see
;; something further down the source or test file that helps you understand
;; what's going on.

;;
;; This challenge is an already-working example of Fixed windowing. We recommend
;; reading the entirety of the source code for this example - there are more
;; moving parts than before.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-6-0-test`
;;

(def input
  ;; In this example, :event-id is meant to be a globally unique value. We're
  ;; going to group these segments into hourly buckets based on the :event-time.
  [{:event-id 1 :event-time #inst "2015-11-20T03:15:00.000-00:00" :page-visited "/"}
   {:event-id 2 :event-time #inst "2015-11-20T04:45:00.000-00:00" :page-visited "/login"}
   {:event-id 3 :event-time #inst "2015-11-20T03:50:00.000-00:00" :page-visited "/purchase"}
   {:event-id 4 :event-time #inst "2015-11-20T03:59:00.000-00:00" :page-visited "/about"}
   {:event-id 5 :event-time #inst "2015-11-20T04:00:00.000-00:00" :page-visited "/login"}
   {:event-id 6 :event-time #inst "2015-11-20T04:30:00.000-00:00" :page-visited "/logout"}
   {:event-id 7 :event-time #inst "2015-11-20T05:00:00.000-00:00" :page-visited "/"}
   {:event-id 8 :event-time #inst "2015-11-20T05:43:00.000-00:00" :page-visited "/purchase"}
   {:event-id 9 :event-time #inst "2015-11-20T04:59:59.000-00:00" :page-visited "/about"}])

(def expected-output
  ;; We have a map with three groups - each of them represents a one
  ;; hour time interval. These keys are derived from the fact that
  ;; we're using a Fixed Window with a 1 hour range.
  {[#inst "2015-11-20T03:00:00.000-00:00"
    #inst "2015-11-20T03:59:59.999-00:00"]
   #{{:event-id 1 :event-time #inst "2015-11-20T03:15:00.000-00:00" :page-visited "/"}
     {:event-id 4 :event-time #inst "2015-11-20T03:59:00.000-00:00" :page-visited "/about"}
     {:event-id 3 :event-time #inst "2015-11-20T03:50:00.000-00:00" :page-visited "/purchase"}}

   [#inst "2015-11-20T04:00:00.000-00:00"
    #inst "2015-11-20T04:59:59.999-00:00"]
   #{{:event-id 2 :event-time #inst "2015-11-20T04:45:00.000-00:00" :page-visited "/login"}
     {:event-id 5 :event-time #inst "2015-11-20T04:00:00.000-00:00" :page-visited "/login"}
     {:event-id 9 :event-time #inst "2015-11-20T04:59:59.000-00:00" :page-visited "/about"}
     {:event-id 6 :event-time #inst "2015-11-20T04:30:00.000-00:00" :page-visited "/logout"}}

   [#inst "2015-11-20T05:00:00.000-00:00"
    #inst "2015-11-20T05:59:59.999-00:00"]
   #{{:event-id 8 :event-time #inst "2015-11-20T05:43:00.000-00:00" :page-visited "/purchase"}
     {:event-id 7 :event-time #inst "2015-11-20T05:00:00.000-00:00" :page-visited "/"}}})

(deftest test-level-6-challenge-0
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        n-peers (u/n-peers catalog c/workflow)
        ;; The trigger is going to fire once for each window.
        ;; We're waiting for the trigger to fire 3 times.
        ;; When it does, we'll deliver on the promise "p",
        ;; indicating that we've seen every state update.
        n-trigger-fires (atom 0)
        p (promise)]

    ;; We reset the atom in the source file to its initial
    ;; value. This is just a convenience to help you avoid
    ;; a gotcha of having stale state if you forgot to reload
    ;; the namespace.
    (reset! c/fired-window-state {})

    (with-test-env
      [test-env [n-peers env-config peer-config]]

      ;; We watch the atom, waiting for it to be updated
      ;; 3 times. We get the number 3 from the fact that
      ;; in our toy data set, we have three distinct hour
      ;; values (3, 4, and 5). Since we're using a fixed,
      ;; hourly window, this means we'll have three distinct
      ;; buckets.
      (add-watch c/fired-window-state :watcher
                 (fn [k r old new]
                   (let [n (swap! n-trigger-fires inc)]
                     (when (= n 3)
                       (deliver p true)))))
      
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :windows c/windows
                 :triggers c/triggers
                 :task-scheduler :onyx.task-scheduler/balanced}
            job-id (:job-id (onyx.api/submit-job peer-config job))]
        (assert job-id "Job was not successfully submitted")
        (feedback-exception! peer-config job-id)
        ;; Wait for promise p to be delivered, which indicates
        ;; that the computation is finished. Then we check
        ;; the results.
        @p
        (is (= expected-output @c/fired-window-state))))))
