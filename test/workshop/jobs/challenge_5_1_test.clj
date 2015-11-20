(ns workshop.jobs.challenge-5-1-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env]]
            [workshop.challenge-5-1 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; Try your hand at flow conditions by routing
;; user segments to different tasks based on their :role key
;; from the :identity task. Check the expected outputs to create
;; the correct flow conditions data structure and predicates.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-5-1-test`
;;

(def input
  [{:username "Mike" :status :admin}
   {:username "Linda" :status :user}
   {:username "Lucas" :status :admin}
   {:username "Ron" :status :guest}
   {:username "Kara" :status :user}])

(def expected-admins
  [{:username "Mike" :status :admin}
   {:username "Lucas" :status :admin}])

(def expected-users
  [{:username "Linda" :status :user}
   {:username "Kara" :status :user}])

(def expected-guests
  [{:username "Ron" :status :guest}])

(deftest test-level-5-challenge-1
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        outputs [:admins-output :users-output :guests-output]
        n-peers (u/n-peers catalog c/workflow)])
  (with-test-env
    [test-env [n-peers env-config peer-config]]
    (u/bind-inputs! lifecycles {:read-segments input})
    (let [job {:workflow c/workflow
               :catalog catalog
               :lifecycles lifecycles
               :flow-conditions c/flow-conditions
               :task-scheduler :onyx.task-scheduler/balanced}]
      (onyx.api/submit-job peer-config job)
      (let [[admins users guests] (u/collect-outputs! lifecycles outputs)]
        (u/segments-equal? expected-admins admins)
        (u/segments-equal? expected-users users)
        (u/segments-equal? expected-guests guests)))))
