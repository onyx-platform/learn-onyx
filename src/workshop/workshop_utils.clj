(ns workshop.workshop-utils
  (:require [clojure.test :refer [is]]
            [clojure.set :refer [join]]
            [clojure.core.async :refer [chan sliding-buffer >!! close!]]
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
  (is (= (into #{} expected) (into #{} actual))))

(defn load-peer-config [onyx-id]
  (assoc (-> "dev-peer-config.edn" resource slurp read-string)
    :onyx/tenancy-id onyx-id
    :zookeeper/address zk-str))

(defn load-env-config [onyx-id]
  (assoc (-> "env-config.edn" resource slurp read-string)
    :onyx/tenancy-id onyx-id
    :zookeeper/address zk-str
    :zookeeper.server/port zk-port))

;;;; Lifecycles utils ;;;;

(def default-channel-size 1000)

(def output-channel-capacity (inc default-channel-size))

(defonce channels (atom {}))
(defonce buffers (atom {}))

(defn get-channel
  ([id] (get-channel id default-channel-size))
  ([id size]
   (if-let [id (get @channels id)]
     id
     (do (swap! channels assoc id (chan (or size default-channel-size)))
         (get-channel id)))))

(defn get-buffer
  [id]
   (if-let [id (get @buffers id)]
     id
     (do (swap! buffers assoc id (atom {}))
         (get-buffer id))))

(defn get-input-channel [id]
  (get-channel id default-channel-size))

(defn get-output-channel [id]
  (get-channel id output-channel-capacity))

(defn inject-in-ch
  [_ lifecycle]
  {:core.async/buffer (get-buffer (:core.async/id lifecycle))
   :core.async/chan (get-channel (:core.async/id lifecycle)
                                 (or (:core.async/size lifecycle)
                                     default-channel-size))})

(defn inject-out-ch
  [_ lifecycle]
  {:core.async/chan (get-channel (:core.async/id lifecycle)
                                 (or (:core.async/size lifecycle)
                                     default-channel-size))})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(defn get-core-async-channels
  [{:keys [catalog lifecycles]}]
  (let [lifecycle-catalog-join (join catalog lifecycles {:onyx/name :lifecycle/task})]
    (reduce (fn [acc item]
              (assoc acc
                     (:onyx/name item)
                     (get-channel (:core.async/id item)
                                  (:core.async/size item)))) 
            {} 
            (filter :core.async/id lifecycle-catalog-join))))

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
      (close! in-ch))))

(defn collect-outputs! [lifecycles output-tasks]
  (->> output-tasks
       (map #(get-output-channel (channel-id-for lifecycles %)))
       (map #(take-segments! % 50))))
