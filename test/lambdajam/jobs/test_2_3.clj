(ns lambdajam.jobs.test-2-3
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [lambdajam.launcher.dev-system :refer [onyx-dev-env]]
            [lambdajam.challenge-2-3 :as c]
            [lambdajam.workshop-utils :as u]
            [onyx.api]))

;; We're starting to get used to using the catalog as a point of
;; knob-tuning on functions. What else can we use the catalog for?
;; Here's we'll explore stateful grouping of functions driven by
;; catalog-entry parameters.
;;
;; In a stateful grouping, we apply a function to every segment. This
;; function returns some value. All segments that return that same value
;; will always be sent to the same peer. This gives you a point of leverage
;; to "see" all the segments that have a particular property on the same peer
;; This lets you do things like create local, in-memory aggregations.
;;
;; To turn a function into a grouped function, add :onyx/group-by-key to the
;; catalog entry and point it to a keyword that will be present in all segments.
;; Additionally, we need to indicate what "policy" to use if a peer leaves the
;; cluster during the job. We can either tell the entire job to fail, or ask it
;; to continue and have all segments that were sent to that peer be sent to a new one.
;; Set :onyx/flux-policy to :kill or :continue to get the respective behavior.
;; Finally, you need to set the minimum number of peers that this task needs to execute.
;; Set this to an integer with :onyx/min-peers to something like 2 for this example.
;;
;; This example will send some user events through. Group by :user-id and notice how
;; each peer prints to standard out all the segments it receives. Any event with
;; the same :user-id should always be seen by the same peer.
;;
;; Stateful grouping is perhaps the biggest topic that we're tackling so far,
;; so if you need a bit more of an explanation, you might want to consult
;; the documentation on this feature for hints:
;;
;; https://github.com/onyx-platform/onyx/blob/0.6.x/doc/user-guide/functions.md#grouping--aggregation
;;
;; Try it with:
;;
;; `lein test lambdajam.jobs.test-2-3`
;;

(def input
  [{:user-id 1 :event-text "abc"}
   {:user-id 2 :event-text "def"}
   {:user-id 3 :event-text "ghi"}
   {:user-id 1 :event-text "jkl"}
   {:user-id 2 :event-text "mno"}
   {:user-id 3 :event-text "pqr"}
   {:user-id 3 :event-text "stu"}
   {:user-id 2 :event-text "vwy"}
   {:user-id 1 :event-text "xya"}])

(def expected-output input)

(deftest test-level-2-challenge-3
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
