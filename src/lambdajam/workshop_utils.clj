(ns lambdajam.workshop-utils
  (:require [clojure.test :refer [is]]
            [clojure.core.async :refer [chan sliding-buffer >!!]]
            [clojure.java.io :refer [resource]]
            [onyx.plugin.core-async :refer [take-segments!]]))

;;;; Test utils ;;;;

(def zk-address "127.0.0.1")

(def zk-port 2188)

(def zk-str (str zk-address ":" zk-port))

(defn only [coll]
  (assert (not (next coll)))
  (if-let [result (first coll)]
    result
    (assert false)))

(defn find-task [catalog task-name]
  (let [matches (filter #(= task-name (:onyx/name %)) catalog)]
    (when-not (seq matches)
      (throw (ex-info (format "Couldn't find task %s in catalog" task-name)
                      {:catalog catalog :task-name task-name})))
    (first matches)))

(defn n-peers
  "Takes a workflow and catalog, returns the minimum number of peers
   needed to execute this job."
  [catalog workflow]
  (let [task-set (into #{} (apply concat workflow))]
    (reduce
     (fn [sum t]
       (+ sum (or (:onyx/min-peers (find-task catalog t)) 1)))
     0 task-set)))

(defn segments-equal?
  "Onyx is a parallel, distributed system - so ordering isn't guaranteed.
   Does an unordered comparison of segments to check for equality."
  [expected actual]
  (is (= (into #{} expected) (into #{} (remove (partial = :done) actual))))
  (is (= :done (last actual)))
  (is (= (dec (count actual)) (count expected))))

(defn load-peer-config [onyx-id]
  (assoc (-> "dev-peer-config.edn" resource slurp read-string)
    :onyx/id onyx-id
    :zookeeper/address zk-str))

(defn load-env-config [onyx-id]
  (assoc (-> "env-config.edn" resource slurp read-string)
    :onyx/id onyx-id
    :zookeeper/address zk-str
    :zookeeper.server/port zk-port))

;;;; Lifecycles utils ;;;;

(def input-channel-capacity 10000)

(def output-channel-capacity (inc input-channel-capacity))

(def get-input-channel
  (memoize
   (fn [id] (chan input-channel-capacity))))

(def get-output-channel
  (memoize
   (fn [id] (chan (sliding-buffer output-channel-capacity)))))

(defn channel-id-for [lifecycles task-name]
  (->> lifecycles
       (filter #(= task-name (:lifecycle/task %)))
       (map :core.async/id)
       (remove nil?)
       (first)))

(defn bind-inputs! [lifecycles mapping]
  (doseq [[task segments] mapping]
    (let [in-ch (get-input-channel (channel-id-for lifecycles task))]
      (doseq [segment segments]
        (>!! in-ch segment))
      (>!! in-ch :done))))

(defn collect-outputs! [lifecycles output-tasks]
  (->> output-tasks
       (map #(get-output-channel (channel-id-for lifecycles %)))
       (map #(take-segments! %))))
