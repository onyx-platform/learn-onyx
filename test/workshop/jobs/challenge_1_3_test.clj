(ns workshop.jobs.challenge-1-3-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [workshop.challenge-1-3 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; Function tasks aren't the only thing in workflows that can
;; branch. Inputs and outputs can branch, too. Let's try a more
;; complex workflow, shown below. We use single capital letters
;; for tasks this time for concision in the example. The graph
;; flows with inputs at the top and outputs at the bottom.
;;
;; A    B       C
;;  \  /        |
;;   D- >       E
;;   |  \     / | \
;;   F   \-> G  H  I
;;  / \       \ | /
;; J   K        L
;;
;; Notice how the scaffolding code in the test runner changes
;; slightly to deal with multiple inputs and outputs. Also
;; note that all non I/O tasks simply pass through the segment
;; unchanged.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-1-3-test`

(def a-input (map (fn [x] {:msg x}) [1 2 3 4 5]))

(def b-input (map (fn [x] {:msg x}) [:a :e :i :o :u]))

(def c-input (map (fn [x] {:msg x}) ["v" "w" "x" "y" "z"]))

(def j-expected-output (concat a-input b-input))

(def k-expected-output (concat a-input b-input))

;; Notice that all the elements from input C are expected three times.
;; This is because task C connects to E, which connects to G, H, and I.
;; All the segments from E are sent to G, H, and I, and ultimately merged
;; back together in L.
(def l-expected-output
  (concat a-input b-input c-input c-input c-input))

(deftest test-level-1-challenge-3
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        n-peers (u/n-peers catalog c/workflow)]
    (with-test-env
      [test-env [n-peers env-config peer-config]]
      (u/bind-inputs! lifecycles {:A a-input :B b-input :C c-input})
      (let [job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :task-scheduler :onyx.task-scheduler/balanced}
            job-id (:job-id (onyx.api/submit-job peer-config job))]
        (feedback-exception! peer-config job-id)
        (let [[j-actual k-actual l-actual] (u/collect-outputs! lifecycles [:J :K :L])]
          (u/segments-equal? j-expected-output j-actual)
          (u/segments-equal? k-expected-output k-actual)
          (u/segments-equal? l-expected-output l-actual))))))
