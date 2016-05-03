(ns workshop.jobs.challenge-1-0-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-1-0 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; This level is all about workflows. Workflows define the graph of all
;; possible routes where data can flow through your job. You can think
;; of this as isolating the "structure" of your computation.
;;
;; Onyx's information model is documented in the cheat sheet: 
;; http://www.onyxplatform.org/docs/cheat-sheet/latest/
;;
;; This challenge is an already-working example to get your started.
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-1-0-test`
;;
;; Here's a picture of the workflow that we're crafting. Data
;; starts at the top and flows downward in all directions. This
;; workflow happens to be flat, so data moves in a straight line.
;;
;;    read-segments
;;         |
;;         v
;;     increment-n
;;         |
;;         v
;;      square-n
;;         |
;;         v
;;    write-segments
;;
;; The representation of a workflow is a Clojure vector of vectors.
;; The inner vectors contain two elements: source, and destination, respectively.

(def input
  [{:n 0}
   {:n 1}
   {:n 2}
   {:n 3}
   {:n 4}
   {:n 5}
   {:n 6}
   {:n 7}
   {:n 8}
   {:n 9}])

(def expected-output
  [{:n 1}
   {:n 4}
   {:n 9}
   {:n 16}
   {:n 25}
   {:n 36}
   {:n 49}
   {:n 64}
   {:n 81}
   {:n 100}])

(deftest test-level-1-challenge-0
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
