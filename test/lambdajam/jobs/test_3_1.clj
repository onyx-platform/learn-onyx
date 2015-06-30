(ns lambdajam.jobs.test-3-1
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-3-1 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]))

;; Now that you know the rules of functions, let's implement some
;; functions for a workflow. This Onyx job will have 3 function tasks.
;; You'll need to implement the functions to pass the test. Read
;; the docstrings on the corresponding catalog entries for the
;; functions' purposes.
;;
;; Try it with:
;;
;; `lein test lambdajam.jobs.test-3-1`
;;

(def input
  [{:name "Mike"}
   {:name "Lucas"}
   {:name "Tim"}
   {:name "Aaron"}
   {:name "Lauren"}
   {:name "Bob"}
   {:name "Fred"}
   {:name "Lisa"}
   {:name "Tina"}])

(def expected-output
  [{:name "M | I | K | E"}
   {:name "L | U | C | A | S"}
   {:name "T | I | M"}
   {:name "A | A | R | O | N"}
   {:name "L | A | U | R | E | N"}
   {:name "B | O | B"}
   {:name "F | R | E | D"}
   {:name "L | I | S | A"}
   {:name "T | I | N | A"}])

(deftest test-level-3-challenge-0
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
