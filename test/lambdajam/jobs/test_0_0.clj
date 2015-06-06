(ns lambdajam.jobs.test-0-0
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-0-0 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]))

;; This is a basic, already-completed example to test that Onyx is working
;; okay on your machine. Try it with:
;; `lein test lambdajam.jobs.level-0.test-0-0`

(def input
  [{:sentence "Getting started with Onyx is easy"}
   {:sentence "This is a segment"}
   {:sentence "Segments are Clojure maps"}
   {:sentence "Sample inputs are easy to fabricate"}])

(deftest test-level-0-challenge-0
  (let [dev-env (component/start (onyx-dev-env (u/n-peers c/catalog c/workflow)))]
    (try 
      (let [dev-cfg (-> "dev-peer-config.edn" resource slurp read-string)
            peer-config (assoc dev-cfg :onyx/id (:onyx-id dev-env))
            lifecycles (c/build-lifecycles)]
        (u/bind-inputs! lifecycles {:read-segments input})
        (let [job {:workflow c/workflow
                   :catalog c/catalog
                   :lifecycles lifecycles
                   :task-scheduler :onyx.task-scheduler/balanced}]
          (onyx.api/submit-job peer-config job)
          (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
            (println "==== Start job output ====")
            (clojure.pprint/pprint results)
            (println "==== End job output ====")
            (u/segments-equal? input results))))
      (finally
       (component/stop dev-env)))))
