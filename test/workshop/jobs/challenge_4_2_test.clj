(ns workshop.jobs.challenge-4-2-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [workshop.launcher.dev-system :refer [onyx-dev-env]]
            [workshop.challenge-4-2 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; Logging is great and all, but we've now opened to the door
;; to something much more interesting - maintaining state.
;; In this challenge, you're going to create an atom in
;; the :before-task-start lifecycle hook.
;; After each batch is processed, find the maximum value
;; that we saw in the :identity task. After the task is finished,
;; print the maximum value that we found to standard out.
;; Use the atom for maintain the latest maximum value.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-4-2-test`
;;

(def input (map (fn [n] {:n n}) (shuffle (range 100))))

(def expected-output (map (fn [n] {:n n}) (range 100)))

(deftest test-level-4-challenge-2
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
    (is (= "Maximum value was: 99" (last lines)))))
