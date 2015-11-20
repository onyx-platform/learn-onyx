(ns workshop.jobs.challenge-4-1-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env]]
            [workshop.challenge-4-1 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; In this challenge, we're going to log every segment that
;; we see after it's processed by the :times-three task with
;; the lifecycle hook :after-batch. Obtain the segments via
;; the key :onyx.core/batch in the event map. This key retrieves
;; the segments that enter the task - before processing.
;; Iterate over each segment with `doseq` and log its value.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-4-1-test`
;;

(def input (map (fn [n] {:n n}) (range 10)))

(def expected-output (map (fn [n] {:n (* 3 n)}) (range 10)))

(deftest test-level-4-challenge-1
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        n-peers (u/n-peers catalog c/workflow)]
    (let [output
          (with-out-str
            (with-test-env
              [test-env [n-peers env-config peer-config]]
              (u/bind-inputs! lifecycles {:read-segments input})
              (let [job {:workflow c/workflow
                         :catalog catalog
                         :lifecycles lifecycles
                         :task-scheduler :onyx.task-scheduler/balanced}]
                (onyx.api/submit-job peer-config job)
                (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
                  (u/segments-equal? expected-output results)))))
          results (clojure.string/split output #"\n")]
      (is (= "Starting Onyx development environment" (first results)))
      (is (= "Stopping Onyx development environment" (last results)))
      (is (= (into #{} (map (fn [n] (str {:n n})) (range 10)))
             (into #{} (butlast (rest results))))))))
