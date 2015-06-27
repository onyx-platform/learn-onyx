(ns lambdajam.jobs.test-2-2
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-2-2 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]))

;; These functions and catalog entries that we're writing
;; are getting pretty specific. Can we write generic versions
;; of these catalog entries and supply parameters at runtime? Yes!
;;
;; Now we're going to build on the previous challenge and implement
;; the same job, except we'll use generic "plus" and "times" catalog entries.
;; There are a few ways that you can supply parameters to a function, but
;; the most direct for supplying Clojure values is a catalog parameter called
;; :onyx/params. :onyx/params takes a vector of keywords. These keywords are
;; looked up inside the same catalog entry and resolved to values. Parameters
;; are appended to the *front* of the function signature - the segment always
;; comes last.
;;
;; An example, partial catalog entry that takes parameters looks like:
;;
;; {:onyx/name :my-task-name
;;  :onyx/fn :my.ns/fn
;;  :onyx/type :function
;;  :my/param "this is the argument"
;;  :onyx/params [:my/param]}
;;
;; Try it with:
;;
;; `lein test lambdajam.jobs.test-2-2`
;;

(def input (map (fn [n] {:n n}) (range 10)))

(def expected-output (map (fn [n] {:n (+ 50 (* 3 n))}) (range 10)))

(deftest test-level-2-challenge-2
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
