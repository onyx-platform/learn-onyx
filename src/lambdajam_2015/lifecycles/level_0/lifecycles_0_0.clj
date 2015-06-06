(ns lambdajam-2015.lifecycles.level-0.lifecycles-0-0
  (:require [lambdajam-2015.workshop-utils :as u]))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan (u/get-input-channel (:core.async/id lifecycle))})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :lambdajam-2015.lifecycles.level-0.lifecycles-0-0/in-calls}
   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :core.async/id (java.util.UUID/randomUUID)}
   {:lifecycle/task :write-segments
    :lifecycle/calls :lambdajam-2015.lifecycles.level-0.lifecycles-0-0/out-calls}
   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :core.async/id (java.util.UUID/randomUUID)}])
