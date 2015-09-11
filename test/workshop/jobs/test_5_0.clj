(ns workshop.jobs.test-5-0
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [workshop.launcher.dev-system :refer [onyx-dev-env]]
            [workshop.challenge-5-0 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; By now you're probably wondering about the limitations of
;; the workflow data structure. By default, all segments flow through
;; all branches of a workflow. What if you want to conditionally route
;; segments to one branch or another in a workflow? Flow conditions are
;; a construct that describes how to route segments around your workflow.
;;
;; Onyx has an entire chapter in its user guided dedicated to flow conditions:
;; http://onyx-platform.gitbooks.io/onyx/content/doc/user-guide/flow-conditions.html
;;
;; This challenge is an already-working example of flow conditions. Read
;; the source and get familiar with their basic usage.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.test-5-0`
;;

(def input (map (fn [n] {:n n}) (range 10)))

(def expected-output-squared-evens
  (map (fn [n] {:n (* n n)}) (filter even? (range 10))))

(def expected-output-squared-odds
  (map (fn [n] {:n (* n n)}) (filter odd? (range 10))))

(deftest test-level-5-challenge-0
  (try
    (let [catalog (c/build-catalog)
          lifecycles (c/build-lifecycles)
          outputs [:write-even-segments :write-odd-segments]]
      (user/go (u/n-peers catalog c/workflow))
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [peer-config (u/load-peer-config (:onyx-id user/system))
            job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :flow-conditions c/flow-conditions
                 :task-scheduler :onyx.task-scheduler/balanced}]
        (onyx.api/submit-job peer-config job)
        (let [[even-outputs odd-outputs] (u/collect-outputs! lifecycles outputs)]
          (u/segments-equal? expected-output-squared-evens even-outputs)
          (u/segments-equal? expected-output-squared-odds odd-outputs))))
    (catch InterruptedException e
      (Thread/interrupted))
    (finally
     (user/stop))))
