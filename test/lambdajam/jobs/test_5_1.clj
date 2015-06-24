(ns lambdajam.jobs.test-5-1
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-5-1 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]))

;; Try your hand at flow conditions by routing
;; user segments to different tasks based on their :role key
;; from the :identity task. Check the expected outputs to create
;; the correct flow conditions data structure and predicates.
;;
;; Try it with:
;;
;; `lein test lambdajam.jobs.test-5-1`
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
  (try
    (let [catalog (c/build-catalog)
          dev-cfg (-> "dev-peer-config.edn" resource slurp read-string)
          lifecycles (c/build-lifecycles)
          outputs [:admins-output :users-output :guests-output]]
      (user/go (u/n-peers catalog c/workflow))
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [peer-config (assoc dev-cfg :onyx/id (:onyx-id user/system))
            job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :flow-conditions c/flow-conditions
                 :task-scheduler :onyx.task-scheduler/balanced}]
        (onyx.api/submit-job peer-config job)
        (let [[admins users guests] (u/collect-outputs! lifecycles outputs)]
          (u/segments-equal? expected-admins admins)
          (u/segments-equal? expected-users users)
          (u/segments-equal? expected-guests guests))))
    (catch InterruptedException e
      (Thread/interrupted))
    (finally
     (user/stop))))
