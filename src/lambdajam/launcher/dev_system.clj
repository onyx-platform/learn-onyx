(ns lambdajam.launcher.dev-system
  (:require [clojure.core.async :refer [chan <!!]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [onyx.plugin.core-async]
            [onyx.api]))

(defrecord OnyxDevEnv [n-peers]
  component/Lifecycle

  (start [component]
    (println "Starting Onyx development environment")
    (let [onyx-id (java.util.UUID/randomUUID)
          env-config (assoc (-> "env-config.edn" resource slurp read-string)
                            :onyx/id onyx-id)
          peer-config (assoc (-> "dev-peer-config.edn"
                                 resource slurp read-string) :onyx/id onyx-id)
          env (onyx.api/start-env env-config)
          peer-group (onyx.api/start-peer-group peer-config)
          peers (onyx.api/start-peers n-peers peer-group)]
      (assoc component :env env :peer-group peer-group
             :peers peers :onyx-id onyx-id)))

  (stop [component]
    (println "Stopping Onyx development environment")

    (doseq [v-peer (:peers component)]
      (onyx.api/shutdown-peer v-peer))

    (onyx.api/shutdown-peer-group (:peer-group component))
    (onyx.api/shutdown-env (:env component))

    (assoc component :env nil :peer-group nil :peers nil)))

(defn onyx-dev-env [n-peers]
  (map->OnyxDevEnv {:n-peers n-peers}))
