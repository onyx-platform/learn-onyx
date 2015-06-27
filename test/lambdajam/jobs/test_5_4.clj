(ns lambdajam.jobs.test-5-4
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-5-4 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]))

;; The final feature of flow conditions that we'll cover are
;; key exclusions. Sometimes, you'll want to convey some information
;; from a function to a flow condition predicate that doesn't
;; need to be sent downstream. In the example below, we're going
;; to route users based on their age. Their age is only relevant
;; to the routing, and isn't required after that.
;;
;; Use :flow/exclude-keys and set its value to a vector of keywords
;; to remove from each segment if present.
;;
;; Try it with:
;;
;; `lein test lambdajam.jobs.test-5-4`
;;

(def input
  [{:username "Mike" :age 24}
   {:username "Linda" :age 35}
   {:username "Lucas" :age 32}
   {:username "Ron" :age 16}
   {:username "Kara" :age 14}])

(def expected-adult-output
  [{:username "Mike"}
   {:username "Linda"}
   {:username "Lucas"}])

(def expected-child-output
  [{:username "Ron" :age 16}
   {:username "Kara" :age 14}])

(deftest test-level-5-challenge-4
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
        (let [[children adults] (u/collect-outputs! lifecycles [:children :adults])]
          (u/segments-equal? expected-child-output children)
          (u/segments-equal? expected-adult-output adults))))
    (catch InterruptedException e
      (Thread/interrupted))
    (finally
     (user/stop))))
