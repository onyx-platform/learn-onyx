(ns workshop.jobs.test-4-1
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [workshop.launcher.dev-system :refer [onyx-dev-env]]
            [workshop.challenge-4-1 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; In this challenge, we're going to log every segment that
;; we see after it's processed by the :times-three task with
;; the lifecycle hook :after-batch. Obtain the segments via
;; the key :onyx.core/batch in the event map.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.test-4-1`
;;

(def input (map (fn [n] {:n n}) (range 10)))

(def expected-output (map (fn [n] {:n (* 3 n)}) (range 10)))

(deftest test-level-4-challenge-1
  (let [output
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
        results (clojure.string/split output #"\n")]
    (is (= "Starting Onyx development environment" (first results)))
    (is (= "Stopping Onyx development environment" (last results)))
    (is (= (into #{} (map (fn [n] (str {:n n})) (range 10)))
           (into #{} (butlast (rest results)))))))
