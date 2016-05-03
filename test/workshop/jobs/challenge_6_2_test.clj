(ns workshop.jobs.challenge-6-2-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-6-2 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; So far, our only exposure to windows has been in the Fixed form.
;; Let's explore Sliding windows. Sliding windows span a particular
;; length, just as Fixed windows do. The difference is that Sliding
;; windows overlap by a particular amount. The effect is that segments
;; may fall into *multiple* windows. Refer to the User Guide to read
;; at length about the characteristics of Sliding windows.

;; If you get confused about which segments fall into which windows,
;; a great debugging strategy is to temporarily use the Conj aggregate,
;; printing out the contents inside your Trigger sync function.

;; In this challenge, we're going to set up 1 hour Sliding windows that
;; slide every 30 minutes. This time, we'll maintain a running average
;; of :bytes-sent in each window. Use the out-of-the-box
;; :onyx.windowing.aggregation/average aggregate operation to maintain
;; the running total.

;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-6-2-test`
;;

(def input
  [{:event-id 1 :event-time #inst "2015-11-20T02:59:00.000-00:00" :bytes-sent 400}
   {:event-id 2 :event-time #inst "2015-11-20T06:58:00.000-00:00" :bytes-sent 50}
   {:event-id 3 :event-time #inst "2015-11-20T09:04:00.000-00:00" :bytes-sent 320}
   {:event-id 4 :event-time #inst "2015-11-20T04:00:00.000-00:00" :bytes-sent 1560}
   {:event-id 5 :event-time #inst "2015-11-20T06:05:00.000-00:00" :bytes-sent 350}
   {:event-id 6 :event-time #inst "2015-11-20T03:00:00.000-00:00" :bytes-sent 120}
   {:event-id 7 :event-time #inst "2015-11-20T07:23:00.000-00:00" :bytes-sent 640}
   {:event-id 8 :event-time #inst "2015-11-20T04:26:00.000-00:00" :bytes-sent 560}
   {:event-id 9 :event-time #inst "2015-11-20T01:41:59.000-00:00" :bytes-sent 1024}])

(def expected-output
  {[#inst "2015-11-20T01:00:00.000-00:00"
    #inst "2015-11-20T01:59:59.999-00:00"]
   {:n 1 :sum 1024 :average 1024}
   
   [#inst "2015-11-20T01:30:00.000-00:00"
    #inst "2015-11-20T02:29:59.999-00:00"]
   {:n 1 :sum 1024 :average 1024}

   [#inst "2015-11-20T02:00:00.000-00:00"
    #inst "2015-11-20T02:59:59.999-00:00"]
   {:n 1 :sum 400 :average 400}

   [#inst "2015-11-20T02:30:00.000-00:00"
    #inst "2015-11-20T03:29:59.999-00:00"]
   {:n 2 :sum 520 :average 260}

   [#inst "2015-11-20T03:00:00.000-00:00"
    #inst "2015-11-20T03:59:59.999-00:00"]
   {:n 1 :sum 120 :average 120}

   [#inst "2015-11-20T03:30:00.000-00:00"
    #inst "2015-11-20T04:29:59.999-00:00"]
   {:n 2 :sum 2120 :average 1060}

   [#inst "2015-11-20T04:00:00.000-00:00"
    #inst "2015-11-20T04:59:59.999-00:00"]
   {:n 2 :sum 2120 :average 1060}

   [#inst "2015-11-20T05:30:00.000-00:00"
    #inst "2015-11-20T06:29:59.999-00:00"]
   {:n 1 :sum 350 :average 350}

   [#inst "2015-11-20T06:00:00.000-00:00"
    #inst "2015-11-20T06:59:59.999-00:00"]
   {:n 2 :sum 400 :average 200}

   [#inst "2015-11-20T06:30:00.000-00:00"
    #inst "2015-11-20T07:29:59.999-00:00"]
   {:n 2 :sum 690 :average 345}

   [#inst "2015-11-20T07:00:00.000-00:00"
    #inst "2015-11-20T07:59:59.999-00:00"]
   {:n 1 :sum 640 :average 640}

   [#inst "2015-11-20T08:30:00.000-00:00"
    #inst "2015-11-20T09:29:59.999-00:00"]
   {:n 1 :sum 320 :average 320}

   [#inst "2015-11-20T09:00:00.000-00:00"
    #inst "2015-11-20T09:59:59.999-00:00"]
   {:n 1 :sum 320 :average 320}})

(deftest test-level-6-challenge-2
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        n-peers (u/n-peers catalog c/workflow)
        n-trigger-fires (atom 0)
        p (promise)]

    (reset! c/fired-window-state {})

    (with-test-env
      [test-env [n-peers env-config peer-config]]

      (add-watch c/fired-window-state :watcher
                 (fn [k r old new]
                   (let [n (swap! n-trigger-fires inc)]
                     ;; It ends up that there are 13
                     ;; distinct windows, given what the data is,
                     ;; and the fact that we're using a sliding
                     ;; window per half hour.
                     (when (= n 13)
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
        @p
        (is (= expected-output @c/fired-window-state))))))
