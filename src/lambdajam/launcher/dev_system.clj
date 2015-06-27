(ns lambdajam.launcher.dev-system
  (:require [clojure.core.async :refer [chan <!!]]
            [clojure.java.io :refer [resource]]
            [com.stuartsierra.component :as component]
            [onyx.plugin.core-async]
            [onyx.api]))

(defn try-start-env [env-config]
  (try
    (onyx.api/start-env env-config)
    (catch Throwable e
      nil)))

(defn try-start-group [peer-config]
  (try
    (onyx.api/start-peer-group peer-config)
    (catch Throwable e
      nil)))

(defn try-start-peers [n-peers peer-group]
  (try
    (onyx.api/start-peers n-peers peer-group)
    (catch Throwable e
      nil)))

(def zk-address "127.0.0.1")

(def zk-port 2188)

(def zk-str (str zk-address ":" zk-port))

(defrecord OnyxDevEnv [n-peers]
  component/Lifecycle

  (start [component]
    (println "Starting Onyx development environment")
    (let [onyx-id (java.util.UUID/randomUUID)
          env-config (assoc (-> "env-config.edn" resource slurp read-string)
                       :onyx/id onyx-id
                       :zookeeper/address zk-str
                       :zookeeper.server/port zk-port)
          peer-config (assoc (-> "dev-peer-config.edn"
                                 resource slurp read-string)
                        :onyx/id onyx-id
                        :zookeeper/address zk-str)
          env (try-start-env env-config)
          peer-group (try-start-group peer-config)
          peers (try-start-peers n-peers peer-group)]
      (assoc component :env env :peer-group peer-group
             :peers peers :onyx-id onyx-id)))

  (stop [component]
    (println "Stopping Onyx development environment")

    (doseq [v-peer (:peers component)]
      (try
        (onyx.api/shutdown-peer v-peer)
        (catch InterruptedException e)))

    (when-let [pg (:peer-group component)]
      (try
        (onyx.api/shutdown-peer-group pg)
        (catch InterruptedException e)))

    (when-let [env (:env component)]
      (try
        (onyx.api/shutdown-env env)
        (catch InterruptedException e)))

    (assoc component :env nil :peer-group nil :peers nil)))

(defn onyx-dev-env [n-peers]
  (map->OnyxDevEnv {:n-peers n-peers}))
