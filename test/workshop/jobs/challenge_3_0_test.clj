(ns workshop.jobs.challenge-3-0-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [workshop.launcher.dev-system :refer [onyx-dev-env]]
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
;; http://onyx-platform.gitbooks.io/onyx/content/doc/user-guide/functions.html
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
