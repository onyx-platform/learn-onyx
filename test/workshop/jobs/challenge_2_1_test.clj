(ns workshop.jobs.challenge-2-1-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [workshop.launcher.dev-system :refer [onyx-dev-env]]
            [workshop.challenge-2-1 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; This challenge requires you to write a few catalog entries
;; of your own for function tasks. Map the catalog entries
;; to their corresponding functions as defined by the workflow.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-2-1-test`
;;

(def input (map (fn [n] {:n n}) (range 10)))

(def expected-output (map (fn [n] {:n (+ 50 (* 3 n))}) (range 10)))

(deftest test-level-2-challenge-1
  (try
    (let [catalog (c/build-catalog)
          lifecycles (c/build-lifecycles)]
      (user/go (u/n-peers catalog c/workflow))
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [peer-config (u/load-peer-config (:onyx-id user/system))
            job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :task-scheduler :onyx.task-scheduler/balanced}]
        (onyx.api/submit-job peer-config job)
        (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
          (u/segments-equal? expected-output results))))
    (catch InterruptedException e
      (Thread/interrupted))
    (finally
     (user/stop))))
