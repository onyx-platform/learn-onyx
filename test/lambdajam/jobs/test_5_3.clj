(ns lambdajam.jobs.test-5-3
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-5-3 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]))

;; Another powerful feature of flow conditions is its
;; exception handling mechanism. If a task throws an
;; exception, flow conditions can catch it and react in a flexible
;; manner.
;;
;; To write a flow condition to handle exceptions, add :flow/thrown-exception?
;; set to true in the flow condition entry. Instead of the new segment
;; being recevied in the third parameter of the predicate, the exception
;; object is received instead.
;;
;; Keep in mind that exceptions cannot be serialized. You can transform
;; an exception to a new segment and send it to subsequent tasks by using
;; the :flow/post-transform key in the flow condition entry. Map this key
;; to a keyword that represents a function. This function takes two args -
;; the event map, and the exception object. This function must return a
;; new segment.
;;
;; There is one more caveat. Flow conditions with :flow/thrown-exception?
;; must "short circuit". This topic is out of scope for this training.
;; Set :flow/short-circuit? to true and all will be well.
;;
;; Use exception handlers and post-transforms to pass the test below.
;;
;; Try it with:
;;
;; `lein test lambdajam.jobs.test-5-3`
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
   {:username "Kara" :status :user}
   {:username "Ron" :status :guest :error "Insufficient access level"}])

(deftest test-level-5-challenge-3
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
