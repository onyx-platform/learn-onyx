(ns workshop.jobs.challenge-0-0-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-0-0 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; This is a basic, already-complete example to test that Onyx is working
;; okay on your machine. Try it with:
;;
;; `lein test workshop.jobs.challenge-0-0-test`
;;
;; You should see some output printed to standard out.
;; If this is your first time using Onyx, look at the corresponding
;; source file to get a feel of the parts of an Onyx program.
;;

(def input
  [{:sentence "Getting started with Onyx is easy"}
   {:sentence "This is a segment"}
   {:sentence "Segments are Clojure maps"}
   {:sentence "Sample inputs are easy to fabricate"}])

(deftest test-level-0-challenge-0
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
          (println "==== Start job output ====")
          (clojure.pprint/pprint results)
          (println "==== End job output ====")
          (u/segments-equal? input results))))))
