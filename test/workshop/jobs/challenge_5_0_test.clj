(ns workshop.jobs.challenge-5-0-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-5-0 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; By now you're probably wondering about the limitations of
;; the workflow data structure. By default, all segments flow through
;; all branches of a workflow. What if you want to conditionally route
;; segments to one branch or another in a workflow? Flow conditions are
;; a construct that describes how to route segments around your workflow.
;;
;; Onyx has an entire chapter in its user guided dedicated to flow conditions:
;; http://www.onyxplatform.org/docs/user-guide/latest/#flow-conditions
;;
;; This challenge is an already-working example of flow conditions. Read
;; the source and get familiar with their basic usage.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-5-0-test`
;;

(def input (map (fn [n] {:n n}) (range 10)))

(def expected-output-squared-evens
  (map (fn [n] {:n (* n n)}) (filter even? (range 10))))

(def expected-output-squared-odds
  (map (fn [n] {:n (* n n)}) (filter odd? (range 10))))

(deftest test-level-5-challenge-0
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        outputs [:write-even-segments :write-odd-segments]
        n-peers (u/n-peers catalog c/workflow)]
    (with-test-env
      [test-env [n-peers env-config peer-config]]
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :flow-conditions c/flow-conditions
                 :task-scheduler :onyx.task-scheduler/balanced}
            job-id (:job-id (onyx.api/submit-job peer-config job))]
        (assert job-id "Job was not successfully submitted")
        (feedback-exception! peer-config job-id)
        (let [[even-outputs odd-outputs] (u/collect-outputs! lifecycles outputs)]
          (u/segments-equal? expected-output-squared-evens even-outputs)
          (u/segments-equal? expected-output-squared-odds odd-outputs))))))
