(ns workshop.jobs.challenge-3-2-test
  (:require [clojure.test :refer [deftest is]]
            [onyx.test-helper :refer [with-test-env]]
            [workshop.challenge-3-2 :as c]
            [workshop.workshop-utils :as u]
            [onyx.api]))

;; Functions in Onyx can emit more than one segment. Rather than
;; returning a map, return a seq of maps. In this example, we
;; input sentences and retrieve each word as its own segment as
;; output. Pass the test.
;;
;; Try it with:
;;
;; `lein test workshop.jobs.challenge-3-2-test`
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
  (let [cluster-id (java.util.UUID/randomUUID)
        env-config (u/load-env-config cluster-id)
        peer-config (u/load-peer-config cluster-id)
        catalog (c/build-catalog)
        lifecycles (c/build-lifecycles)
        n-peers (u/n-peers catalog c/workflow)]
    (with-test-env
      [test-env [n-peers env-config peer-config]]
      (u/bind-inputs! lifecycles {:read-segments input})
      (let [job {:workflow c/workflow
                 :catalog catalog
                 :lifecycles lifecycles
                 :task-scheduler :onyx.task-scheduler/balanced}]
        (onyx.api/submit-job peer-config job)
        (let [[results] (u/collect-outputs! lifecycles [:write-segments])]
          (u/segments-equal? expected-output results))))))
