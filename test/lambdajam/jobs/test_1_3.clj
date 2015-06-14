(ns lambdajam.jobs.test-1-3
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-1-3 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]))

;; Function tasks aren't the only thing in workflows that can
;; branch. Inputs and outputs can branch, too. Let's try a more
;; complex workflow, shown below. We use single capital letters
;; for tasks this time for concision in the exmaple.
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
;; `lein test lambdajam.jobs.test-1-3`

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
  (try
    (let [catalog (c/build-catalog)
          dev-cfg (-> "dev-peer-config.edn" resource slurp read-string)
          lifecycles (c/build-lifecycles)]
      (user/go (u/n-peers catalog c/workflow))
      (u/bind-inputs! lifecycles {:A a-input :B b-input :C c-input})
      (let [peer-config (assoc dev-cfg :onyx/id (:onyx-id user/system))
            job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :task-scheduler :onyx.task-scheduler/balanced}]
        (onyx.api/submit-job peer-config job)
        (let [[j-actual k-actual l-actual] (u/collect-outputs! lifecycles [:J :K :L])]
          (u/segments-equal? j-expected-output j-actual)
          (u/segments-equal? k-expected-output k-actual)
          (u/segments-equal? l-expected-output l-actual))))
    (catch InterruptedException e
      (Thread/interrupted))
    (finally
     (user/stop))))
