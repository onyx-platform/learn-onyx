(ns workshop.jobs.challenge-4-0-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-4-0 :as c]
            [workshop.workshop-utils :as u]
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
;; Onyx has an entire chapter in the user guide dedicated to lifecycles:
;; http://www.onyxplatform.org/docs/user-guide/latest/lifecycles.html
;;
;; This challenge is an already-working example. Explore the lifecycles
;; and comments in the corresponding source file about the contracts
;; that Onyx lifecycles must abide by.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-4-0-test`
;;

(def input (map (fn [n] {:n n}) (range 10)))

(def expected-output (map (fn [n] {:n (* 2 n)}) (range 10)))

(deftest test-level-4-challenge-0
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
        (feedback-exception! peer-config job-id)
        (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
          (u/segments-equal? expected-output results))))))
