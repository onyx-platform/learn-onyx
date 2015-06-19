(ns lambdajam.jobs.test-4-0
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-4-0 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]))

;; Our next topic to tackle will be lifecycles. Lifecycles are a
;; feature that allow you to control code that executes at particular
;; points during task execution on each peer. Lifecycles are data driven
;; and composable.
;;
;; Lifecycles are great for building up state, executing side effects like
;; logging, and talking to metrics servers. They also allow you to inject
;; non-serializable parameters, such as database connections, into functions.
;;
;; This challenge is an already-working example. Explore the lifecycles
;; and comments in the corresponding source file about the contracts
;; that Onyx lifecycles must abide by.
;;
;; Try it with:
;;
;; `lein test lambdajam.jobs.test-4-0`
;;

(def input (map (fn [n] {:n n}) (range 10)))

(def expected-output (map (fn [n] {:n (* 2 n)}) (range 10)))

(deftest test-level-3-challenge-0
  (try
    (let [catalog (c/build-catalog)
          dev-cfg (-> "dev-peer-config.edn" resource slurp read-string)
          lifecycles (c/build-lifecycles)]
      (user/go (u/n-peers catalog c/workflow))
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [peer-config (assoc dev-cfg :onyx/id (:onyx-id user/system))
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
