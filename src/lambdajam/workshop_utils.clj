(ns lambdajam.workshop-utils
  (:require [clojure.test :refer [is]]
            [clojure.core.async :refer [chan sliding-buffer >!!]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.static.planning :refer [find-task]]))

;;;; Test utils ;;;;

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
  "Onyx is a parallel, distributed system - so ordering isn't gaurunteed.
   Does an unordered comparison of segments to check for equality."
  [expected actual]
  (is (= (into #{} expected) (into #{} (remove (partial = :done) actual))))
  (is (= :done (last actual)))
  (is (= (dec (count actual)) (count expected))))

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
  (:core.async/id
   (->> lifecycles
        (filter #(= task-name (:lifecycle/task %)))
        (first))))

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
