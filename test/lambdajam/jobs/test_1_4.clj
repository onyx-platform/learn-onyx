(ns lambdajam.jobs.test-1-4
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-1-4 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]))

;; Let's break things a bit to start getting used to Onyx's error messages.
;; Submit a workflow with a task name that doesn't exist in the catalog.
;; You should get an error when you try to submit the job via
;; onyx.api/submit-job. Observe onyx.log.
;;
;; Try it with:
;;
;; `lein test lambdajam.jobs.test-1-4`

(def input
  [{:sentence "Getting started with Onyx is easy"}
   {:sentence "This is a segment"}
   {:sentence "Segments are Clojure maps"}
   {:sentence "Sample inputs are easy to fabricate"}])

(deftest test-level-1-challenge-4
  (try
    (let [catalog (c/build-catalog)
          dev-cfg (-> "dev-peer-config.edn" resource slurp read-string)
          lifecycles (c/build-lifecycles)]
      (user/go 3)
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [peer-config (assoc dev-cfg :onyx/id (:onyx-id user/system))
            job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :task-scheduler :onyx.task-scheduler/balanced}]
        (onyx.api/submit-job peer-config job)
        (throw (Error. "The test failed, submit-job completed without an error!"))))
    (catch InterruptedException e
      (Thread/interrupted))
    (catch Exception e
      (is (= true true) "Job submission threw an exception."))
    (finally
     (user/stop))))
