(ns lambdajam-2015.launcher.launch-prod-peers
  (:require [clojure.core.async :refer [chan <!!]]
            [clojure.java.io :refer [resource]]
            [lambdajam-2015.lifecycles.sample-lifecycle]
            [lambdajam-2015.functions.sample-functions]
            [onyx.plugin.core-async]
            [onyx.api]))

(defn -main [onyx-id n & args]
  (let [n-peers (Integer/parseInt n)
        peer-config (assoc (-> "prod-peer-config.edn"
                               resource slurp read-string) :onyx/id onyx-id)
        peer-group (onyx.api/start-peer-group peer-config)
        peers (onyx.api/start-peers n-peers peer-group)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread.
                       (fn []
                         (doseq [v-peer peers]
                           (onyx.api/shutdown-peer v-peer))
                         (onyx.api/shutdown-peer-group peer-group))))
    ;; Block forever.
    (<!! (chan))))
