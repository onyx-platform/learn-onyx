(ns workshop.jobs.test-4-3
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [workshop.launcher.dev-system :refer [onyx-dev-env]]
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
;; `lein test workshop.jobs.test-4-3`
;;

(def input (map (fn [n] {:n n}) (shuffle (range 100))))

(def expected-output input)

(deftest test-level-4-challenge-3
  (let [results
        (with-out-str
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
        lines (clojure.string/split results #"\n")]
    (is (= "Summation was: 4950" (last lines)))))
