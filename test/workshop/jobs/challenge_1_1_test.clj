(ns workshop.jobs.challenge-1-1-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-1-1 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; This challenge builds on the previous challenge - you'll implement
;; your first workflow. Below is a pictorial description of what
;; the workflow should look like:
;;
;;    read-segments
;;         |
;;         v
;;       cube-n
;;         |
;;         v
;;      add-ten
;;         |
;;         v
;;    multiply-by-5
;;         |
;;         v
;;    write-segments
;;
;; Open the corresponding src file for this challenge and fill in the workflow.
;; The workflow is left as an undefined var. Add the appropriate data
;; structure.
;;
;; Look for the "<<< BEGIN FILL ME IN >>>" and "<<< END FILL ME IN >>>"
;; comments to start your work.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-1-1-test`

(def input (mapv (fn [n] {:n n}) (range 10)))

(def expected-output
  [{:n 50}
   {:n 55}
   {:n 90}
   {:n 185}
   {:n 370}
   {:n 675}
   {:n 1130}
   {:n 1765}
   {:n 2610}
   {:n 3695}])

(deftest test-level-1-challenge-1
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
