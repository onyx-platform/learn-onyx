(ns workshop.jobs.challenge-4-1-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-4-1 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; In this challenge, we're going to log every segment that
;; we see after it's processed by the :times-three task with
;; the lifecycle hook :after-batch. We can obtain a reference
;; to the segments through the event map. The event map contains
;; lots of useful values pertaining to the currently running task.
;; We can access the segments by first looking at the :onyx.core/batch
;; key. This value is itself a sequence of Clojure Records. The Records,
;; which can uniformly be treated as maps in Clojure, have a key :message
;; on each them. We use a Record to increase performance - but that's
;; mostly a side-tangent and is unimportant to accomplishing the exercise.
;; Iterate over each segment with `doseq` to print out the segment's value.
;; The segments under the :onyx.core/batch key are the segment values *before*
;; the :onyx/fn function transforms them.
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
                         :task-scheduler :onyx.task-scheduler/balanced}
                    job-id (:job-id (onyx.api/submit-job peer-config job))]
                (feedback-exception! peer-config job-id)
                (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
                  (u/segments-equal? expected-output results)))))
          results (clojure.string/split output #"\n")]
      (is (clojure.set/subset? 
           (into #{} (map (fn [n] (str {:n n})) (range 10)))
           (into #{}  results))))))
