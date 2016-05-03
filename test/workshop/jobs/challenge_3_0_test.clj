(ns workshop.jobs.challenge-3-0-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-3-0 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; In this level, we're going to briefly explore the idea of
;; functions in Onyx. Onyx functions are *literally* Clojure functions,
;; so we'll be quick with this section. We're going to check out
;; how to harness a few interesting patterns that relate to concepts
;; we learned earlier.
;;
;; Onyx has a chapter in its user guide dedicated to functions:
;; http://www.onyxplatform.org/docs/user-guide/latest/functions.html
;;
;; This challenge is an already-working example. Explore the functions
;; and comments in the corresponding source file about the contracts
;; that Onyx functions must abide by.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-3-0-test`
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
  [{:name "MiKe!"}
   {:name "LuCaS!"}
   {:name "TiM!"}
   {:name "AaRoN!"}
   {:name "LaUrEn!"}
   {:name "BoB!"}
   {:name "FrEd!"}
   {:name "LiSa!"}
   {:name "TiNa!"}])

(deftest test-level-3-challenge-0
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
        (assert job-id "Job was not successfully submitted")
        (feedback-exception! peer-config job-id)
        (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
          (u/segments-equal? expected-output results))))))
