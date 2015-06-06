(ns lambdajam.jobs.test-1-1
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-1-1 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]))


;;    read-segments
;;         |
;;         v
;;       cube-n
;;         |
;;         v
;;      add-ten
;;         |
;;         v
;;    multiply-by-5
;;         |
;;         v
;;    write-segments

(def input (mapv (fn [n] {:n n}) (range 10)))

(def expected-output
  [{:n 50}
   {:n 55}
   {:n 90}
   {:n 185}
   {:n 370}
   {:n 675}
   {:n 1130}
   {:n 1765}
   {:n 2610}
   {:n 3695}])

(deftest test-level-1-challenge-0
  (let [dev-env (component/start (onyx-dev-env (u/n-peers c/catalog c/workflow)))]
    (try 
      (let [dev-cfg (-> "dev-peer-config.edn" resource slurp read-string)
            peer-config (assoc dev-cfg :onyx/id (:onyx-id dev-env))
            lifecycles (c/build-lifecycles)]
        (u/bind-inputs! lifecycles {:read-segments input})
        (let [job {:workflow c/workflow
                   :catalog c/catalog
                   :lifecycles lifecycles
                   :task-scheduler :onyx.task-scheduler/balanced}]
          (onyx.api/submit-job peer-config job)
          (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
            (u/segments-equal? expected-output results))))
      (finally
       (component/stop dev-env)))))
