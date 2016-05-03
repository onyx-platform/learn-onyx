(ns workshop.jobs.challenge-2-4-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-2-4 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; Let's try a non-functional adjustment to a task through the catalog.
;; Sometimes, you want to bound the maximum number of peers
;; that a task will receive. You can set :onyx/max-peers to an integer
;; that is >= 1. Try this on a task. Each peer will print out the
;; task name that it is executing. The test will pass when you bound
;; the :identity function to three peers.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-2-4-test`
;;

(def input (map (fn [n] {:n n}) (range 10)))

(def expected-output input)

(deftest test-level-2-challenge-4
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        n-peers 5]
    (let [stdout
          (with-out-str
            (with-test-env
              [test-env [n-peers env-config peer-config]]
              (u/bind-inputs! lifecycles {:read-segments input})
              (let [job {:workflow c/workflow
                         :catalog catalog
                         :lifecycles lifecycles
                         :task-scheduler :onyx.task-scheduler/balanced}
                    job-id (:job-id (onyx.api/submit-job peer-config job))]
                (assert job-id "Job was not successfully submitted")
                (feedback-exception! peer-config job-id)
                (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
                  (u/segments-equal? expected-output results)))))
          results (clojure.string/split stdout #"\n")]
      (is (= "Starting Onyx test environment" (first results)))
      (is (= "Stopping Onyx test environment" (last results)))
      (is (= ["Peer executing task :identity"
              "Peer executing task :identity"
              "Peer executing task :identity"
              "Peer executing task :read-segments"
              "Peer executing task :write-segments"]
             (sort (butlast (rest results))))))))
