(ns lambdajam.jobs.test-5-2
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-5-2 :as c]
            [lambdajam.workshop-utils :as u]
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
;; `lein test lambdajam.jobs.test-5-2`
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
  (try
    (let [catalog (c/build-catalog)
          lifecycles (c/build-lifecycles)]
      (user/go (u/n-peers catalog c/workflow))
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [peer-config (u/load-peer-config (:onyx-id user/system))
            job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :flow-conditions c/flow-conditions
                 :task-scheduler :onyx.task-scheduler/balanced}]
        (onyx.api/submit-job peer-config job)
        (let [[output] (u/collect-outputs! lifecycles [:write-segments])]
          (u/segments-equal? expected-output output))))
    (catch InterruptedException e
      (Thread/interrupted))
    (finally
     (user/stop))))
