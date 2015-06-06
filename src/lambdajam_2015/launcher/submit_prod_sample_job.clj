(ns lambdajam-2015.launcher.submit-prod-sample-job
  (:require [clojure.java.io :refer [resource]]
            [lambdajam-2015.workflows.sample-workflow :refer [workflow]]
            [lambdajam-2015.catalogs.sample-catalog :refer [catalog]]
            [lambdajam-2015.lifecycles.sample-lifecycle :as sample-lifecycle]
            [lambdajam-2015.functions.sample-functions]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

(defn -main [onyx-id & args]
  (let [peer-config (assoc (-> "prod-peer-config.edn"
                               resource slurp read-string) :onyx/id onyx-id)
        ;; TODO: Transfer dev catalog entries and lifecycles into prod
        ;; IO streams.
        lifecycles (sample-lifecycle/build-lifecycles)]
    (let [job {:workflow workflow
               :catalog catalog
               :lifecycles lifecycles
               :task-scheduler :onyx.task-scheduler/balanced}]
      (onyx.api/submit-job peer-config job))))
