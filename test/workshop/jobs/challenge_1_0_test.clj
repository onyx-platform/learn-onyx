(ns workshop.jobs.challenge-1-0-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [workshop.launcher.dev-system :refer [onyx-dev-env]]
            [workshop.challenge-1-0 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; This level is all about workflows. Workflows define the graph of all
;; possible routes where data can flow through your job. You can think
;; of this as isolating the "structure" of your computation.
;;
;; Onyx's information model is documented in the user guide:
;; http://onyx-platform.gitbooks.io/onyx/content/doc/user-guide/information-model.html
;;
;; This challenge is an already-working example to get your started.
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-1-0-test`
;;
;; Here's a picture of the workflow that we're crafting. Data
;; starts at the top and flows downward in all directions. This
;; workflow happens to be flat, so data moves in a straight line.
;;
;;    read-segments
;;         |
;;         v
;;     increment-n
;;         |
;;         v
;;      square-n
;;         |
;;         v
;;    write-segments
;;
;; The representation of a workflow is a Clojure vector of vectors.
;; The inner vectors contain two elements: source, and destination, respectively.

(def input
  [{:n 0}
   {:n 1}
   {:n 2}
   {:n 3}
   {:n 4}
   {:n 5}
   {:n 6}
   {:n 7}
   {:n 8}
   {:n 9}])

(def expected-output
  [{:n 1}
   {:n 4}
   {:n 9}
   {:n 16}
   {:n 25}
   {:n 36}
   {:n 49}
   {:n 64}
   {:n 81}
   {:n 100}])

(deftest test-level-1-challenge-0
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
