(ns workshop.jobs.challenge-3-3-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env]]
            [workshop.challenge-3-3 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; In level 2, we explored how functions can take parameters by
;; changing the catalog entry. Now let's use what we learned to
;; write a catalog entry and parameterized function from scratch.
;; There are two sections of the source file for you to work on.
;;
;; The goal is to prepend a ~ and append a ? to every :name value
;; in your segment. By parameterizing the function, you can dynamically 
;; parameterize your functions at runtime.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-3-3-test`
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
  [{:name "~Mike?"}
   {:name "~Lucas?"}
   {:name "~Tim?"}
   {:name "~Aaron?"}
   {:name "~Lauren?"}
   {:name "~Bob?"}
   {:name "~Fred?"}
   {:name "~Lisa?"}
   {:name "~Tina?"}])

(deftest test-level-3-challenge-3
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
