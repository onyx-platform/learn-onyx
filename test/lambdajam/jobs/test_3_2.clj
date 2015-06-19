(ns lambdajam.jobs.test-3-2
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-3-2 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]))

;; Functions in Onyx can emit more than one segment. Rather than
;; returning a map, return a seq of maps. In this example, we
;; input sentences and retrieve each word as its own segment as
;; output. Pass the test.
;;
;; Try it with:
;;
;; `lein test lambdajam.jobs.test-3-2`
;;

(def input
  [{:sentence "Lorem ipsum dolor sit amet, consectetur adipiscing elit."}
   {:sentence "Vestibulum tincidunt leo lectus, ac vulputate quam aliquet a."}
   {:sentence "Ut molestie suscipit faucibus."}
   {:sentence "Curabitur hendrerit tortor quis sodales consequat."}])

(def expected-output
  [{:word "Lorem"}
   {:word "ipsum"}
   {:word "dolor"}
   {:word "sit"}
   {:word "amet,"}
   {:word "consectetur"}
   {:word "adipiscing"}
   {:word "elit."}
   {:word "Vestibulum"}
   {:word "tincidunt"}
   {:word "leo"}
   {:word "lectus,"}
   {:word "ac"}
   {:word "vulputate"}
   {:word "quam"}
   {:word "aliquet"}
   {:word "a."}
   {:word "Ut"}
   {:word "molestie"}
   {:word "suscipit"}
   {:word "faucibus."}
   {:word "Curabitur"}
   {:word "hendrerit"}
   {:word "tortor"}
   {:word "quis"}
   {:word "sodales"}
   {:word "consequat."}])

(deftest test-level-3-challenge-2
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
