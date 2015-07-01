(ns lambdajam.jobs.test-2-4
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-2-4 :as c]
            [lambdajam.workshop-utils :as u]
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
;; `lein test lambdajam.jobs.test-2-4`
;;

(def input (map (fn [n] {:n n}) (range 10)))

(def expected-output input)

(deftest test-level-2-challenge-4
  (let [stdout
        (with-out-str
          (try
            (let [catalog (c/build-catalog)
                  lifecycles (c/build-lifecycles)]
              (user/go 5)
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
        results (clojure.string/split stdout #"\n")]
    (is (= "Starting Onyx development environment" (first results)))
    (is (= "Stopping Onyx development environment" (last results)))
    (is (= ["Peer executing task :identity"
            "Peer executing task :identity"
            "Peer executing task :identity"
            "Peer executing task :read-segments"
            "Peer executing task :write-segments"]
           (sort (butlast (rest results)))))))
