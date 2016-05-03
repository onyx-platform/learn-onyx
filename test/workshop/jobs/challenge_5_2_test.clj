(ns workshop.jobs.challenge-5-2-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-5-2 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; Flow conditions have a lot of extra power besides basic
;; routing. Flow condition predicates also support composite
;; function construction. A :flow/predicate can take either
;; a keyword (naming a function) or a vector. In the case
;; of a vector, the first value of the vector can be one of
;; :and, :or, or :not. Subsequent elements can contain
;; either keywords or vectors - meanining that composite
;; predicates nest arbitrarily. Predicates can also take parameters.
;; We leave parameterization out of scope in this training material.
;;
;; Use composite flow conditions to route segments with a single
;; flow condition entry. Pass the segment through if the user
;; is either an :admin or a :user. Drop the segment otherwise.
;; Use only one flow condition.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-5-2-test`
;;

(def input
  [{:username "Mike" :status :admin}
   {:username "Linda" :status :user}
   {:username "Lucas" :status :admin}
   {:username "Ron" :status :guest}
   {:username "Kara" :status :user}])

(def expected-output
  [{:username "Mike" :status :admin}
   {:username "Lucas" :status :admin}
   {:username "Linda" :status :user}
   {:username "Kara" :status :user}])

(deftest test-level-5-challenge-2
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
                 :flow-conditions c/flow-conditions
                 :task-scheduler :onyx.task-scheduler/balanced}
            job-id (:job-id (onyx.api/submit-job peer-config job))]
        (assert job-id "Job was not successfully submitted")
        (feedback-exception! peer-config job-id)
        (let [[output] (u/collect-outputs! lifecycles [:write-segments])]
          (u/segments-equal? expected-output output))))))
