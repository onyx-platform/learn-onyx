(ns workshop.jobs.challenge-4-3-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-4-3 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; Time to compose what we've learned about state and lifecycles
;; with function parameters. One way to add parameters to a function
;; is with :onyx/params. Another way is by using lifecycles. You
;; can add non-serializable parameters to a function by using the
;; :before-task-start and/or :before-batch lifecycles. Return a map
;; with key :onyx.core/params. The value of this key should be a vector
;; of values. This vector is applied to the *beginning* of the function
;; signature.
;;
;; In this challenge, you'll create an atom using lifecycles to sum
;; up the :n values of a segment. Maintain the state through the function,
;; and access the atom as a parameter of the sum! function.
;;
;; This technique composes with all the other ways to inject parameters.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-4-3-test`
;;

(def input (map (fn [n] {:n n}) (shuffle (range 100))))

(def expected-output input)

(deftest test-level-4-challenge-3
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        n-peers (u/n-peers catalog c/workflow)]
    (let [results
          (with-out-str
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
                  (u/segments-equal? expected-output results)))))
          lines (clojure.string/split results #"\n")]
      (is (contains? (set lines) "Summation was: 4950")))))
