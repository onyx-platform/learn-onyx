(ns workshop.jobs.test-3-3
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [workshop.launcher.dev-system :refer [onyx-dev-env]]
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
;; `lein test workshop.jobs.test-3-3`
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
