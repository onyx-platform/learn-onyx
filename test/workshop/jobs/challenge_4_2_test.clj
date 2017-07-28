(ns workshop.jobs.challenge-4-2-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-4-2 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; Logging is great and all, but we've now opened to the door
;; to something much more interesting - maintaining state.
;; In this challenge, you're going to reference an atom defined
;; in the corresponding source file and use it in
;; the :before-task-start lifecycle hook.
;;
;; Remember that lifecycle hooks return maps, like the
;; `writer-lifecycle` hooks did on the previous examples. The
;; elements from the map you return will be merged with those being
;; passed on the event to other lifecycle handlers.
;;
;; After each batch is processed, find the maximum value
;; that we saw in the :identity task. After the task is finished,
;; we'll query the atom to find the maximum value.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-4-2-test`
;;

(def input (map (fn [n] {:n n}) (shuffle (range 100))))

(def expected-output (map (fn [n] {:n n}) (range 100)))

(deftest test-level-4-challenge-2
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        n-peers (u/n-peers catalog c/workflow)]
    (reset! c/state nil)
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
          (u/segments-equal? expected-output results)
          (is (= 99 @c/state)))))))
