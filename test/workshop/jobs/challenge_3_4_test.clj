(ns workshop.jobs.challenge-3-4-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-3-4 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; Sometimes it's desirable to have a function act on more than
;; one segment at a time for performance reasons, like writing to
;; an intermediate database during a workflow. Onyx supports
;; this with the catalog entry :onyx/batch-fn? set to a boolean variable.
;; When this is set to true, rather than supplying the function
;; with a single segment, the function is given the entire batch
;; of segments being processed. The catch is that the return value
;; of the function that has :onyx/batch-fn? set to true is that its
;; return value must return a sequence of segments of the same length
;; as its input. The elements of the two sequences are "zipped up" by
;; position to form the parent/child relationship throughout the tree.
;; For example, the element at index 0 of the input batch has children
;; located at position 0 of the output batch.
;;
;; Batched functions are useful when the operation is more efficient
;; operating over a group of segments than it would be over one segment
;; per invocation.
;;
;; Try implementing a batched function that capitalizes the :name key
;; in each segment.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-3-4-test`
;;

(def input
  [{:name "mike"}
   {:name "lucas"}
   {:name "tim"}
   {:name "aaron"}
   {:name "lauren"}
   {:name "bob"}
   {:name "fred"}
   {:name "lisa"}
   {:name "tina"}])

(def expected-output
  [{:name "Mike"}
   {:name "Lucas"}
   {:name "Tim"}
   {:name "Aaron"}
   {:name "Lauren"}
   {:name "Bob"}
   {:name "Fred"}
   {:name "Lisa"}
   {:name "Tina"}])

(deftest test-level-3-challenge-4
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        n-peers (u/n-peers catalog c/workflow)]
    (with-test-env
      [test-env [n-peers env-config peer-config]]
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :task-scheduler :onyx.task-scheduler/balanced}
            job-id (:job-id (onyx.api/submit-job peer-config job))]
        (assert job-id "Job was not successfully submitted")
        (feedback-exception! peer-config job-id)
        (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
          (u/segments-equal? expected-output results))))))
