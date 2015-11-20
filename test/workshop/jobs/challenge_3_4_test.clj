(ns workshop.jobs.challenge-3-4-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env]]
            [workshop.challenge-3-4 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; Sometimes it's desirable to have a function act on more than
;; one segment at a time for performance reasons, like writing to
;; an intermediate database during a workflow. Onyx supports
;; this with the catalog entry :onyx/bulk? set to a boolean variable.
;; When this is set to true, rather than supplying the function
;; with a single segment, the function is given the entire batch
;; of segments being processed. This number is also configurable
;; in the catalog by :onyx/batch-size. When :onyx/bulk? is set to
;; true, the output of the function is *ignored*. All segments
;; are passed through without modification to the downstream tasks.
;;
;; Try implementing a bulk function that writes all the segments
;; it sees to standard out. Use side effects directly in the task
;; function. Later we'll learn about a better way to do side effects,
;; but bulk functions are useful to know about when you need optimal
;; performance.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-3-4-test`
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

(def expected-output input)

(deftest test-level-3-challenge-4
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
                 :task-scheduler :onyx.task-scheduler/balanced}]
        (onyx.api/submit-job peer-config job)
        (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
          (u/segments-equal? expected-output results))))))
