(ns lambdajam.jobs.test-1-2
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-1-2 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]
            [user]))

;; Workflows are direct, acyclic graphs - meaning they can split
;; and merge. Try modeling the workflow below. The task "cube-n"
;; splits its output into two streams - into tasks "add-ten", and
;; "add-forty". Both tasks get *all* the segments produced by cube-n.
;; Their results are sent to "multiply-by-5".
;;
;;    read-segments
;;         |
;;         v
;;       cube-n
;;      /      \
;;     |       |
;;     v       v
;; add-ten   add-forty
;;     |       |
;;      \     /
;;         v
;;    multiply-by-5
;;         |
;;         v
;;    write-segments
;;
;;
;; Try it with:
;;
;; `lein test lambdajam.jobs.test-1-2`

(def input (mapv (fn [n] {:n n}) (range 10)))

(def expected-output
  [{:n 50}
   {:n 55}
   {:n 90}
   {:n 185}
   {:n 200}
   {:n 205}
   {:n 240}
   {:n 335}
   {:n 370}
   {:n 520}
   {:n 675}
   {:n 825}
   {:n 1130}
   {:n 1280}
   {:n 1765}
   {:n 1915}
   {:n 2610}
   {:n 2760}
   {:n 3695}
   {:n 3845}])

(deftest test-level-1-challenge-1
  (try
    (let [catalog (c/build-catalog)
          dev-cfg (-> "dev-peer-config.edn" resource slurp read-string)
          lifecycles (c/build-lifecycles)]
      (user/go (u/n-peers catalog c/workflow))
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [peer-config (assoc dev-cfg :onyx/id (:onyx-id user/system))
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
