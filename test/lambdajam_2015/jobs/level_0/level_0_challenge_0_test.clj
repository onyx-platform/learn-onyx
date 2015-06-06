(ns lambdajam-2015.jobs.level-0.level-0-challenge-0-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam-2015.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam-2015.workflows.level-0.workflow-0-0 :refer [workflow]]
            [lambdajam-2015.catalogs.level-0.catalog-0-0 :refer [catalog]]
            [lambdajam-2015.lifecycles.level-0.lifecycles-0-0 :refer [build-lifecycles]]
            [lambdajam-2015.functions.level-0.functions-0-0]
            [lambdajam-2015.workshop-utils :as u]
            [onyx.api]))

;; This is a basic, already-completed example to test that Onyx is working
;; okay on your machine. Test it with
;; `lein test lambdajam-2015.jobs.level-0.level-0-challenge-0-test` at the repl.

(def input
  [{:sentence "Getting started with Onyx is easy"}
   {:sentence "This is a segment"}
   {:sentence "Segments are Clojure maps"}
   {:sentence "Sample inputs are easy to fabricate"}])

(deftest test-challenge-0
  (let [dev-env (component/start (onyx-dev-env (u/n-peers catalog workflow)))]
    (try 
      (let [dev-cfg (-> "dev-peer-config.edn" resource slurp read-string)
            peer-config (assoc dev-cfg :onyx/id (:onyx-id dev-env))
            lifecycles (build-lifecycles)]
        (u/bind-inputs! lifecycles {:read-segments input})
        (let [job {:workflow workflow
                   :catalog catalog
                   :lifecycles lifecycles
                   :task-scheduler :onyx.task-scheduler/balanced}]
          (onyx.api/submit-job peer-config job)
          (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
            (clojure.pprint/pprint results)
            (u/segments-equal? input results))))
      (finally 
       (component/stop dev-env)))))
