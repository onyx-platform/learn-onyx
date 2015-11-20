(ns user
  (:require [clojure.tools.namespace.repl :refer [refresh set-refresh-dirs]]))

(set-refresh-dirs "src" "test")

(defn init [])

(defn start [])

(defn stop [])

(defn go []
  (init)
  (start))

(defn reset []
  (stop)
  (refresh))
