(ns workshop.challenge-1-2
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

;;; <<< BEGIN FILL ME IN >>>

(def workflow
  [[:read-segments :cube-n]
   [:cube-n :add-ten]
   [:cube-n :add-forty]
   [:add-ten :multiply-by-5]
   [:add-forty :multiply-by-5]
   [:multiply-by-5 :write-segments]])

;;; <<< END FILL ME IN >>>

;;; Catalogs ;;;

(defn build-catalog
  ([] (build-catalog 5 50))
  ([batch-size batch-timeout]
     [{:onyx/name :read-segments
       :onyx/plugin :onyx.plugin.core-async/input
       :onyx/type :input
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Reads segments from a core.async channel"}

      {:onyx/name :cube-n
       :onyx/fn :workshop.challenge-1-2/cube-n
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Mutiply :n by itself twice"}

      {:onyx/name :add-ten
       :onyx/fn :workshop.challenge-1-2/add-ten
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Add 10 to :n"}

      {:onyx/name :add-forty
       :onyx/fn :workshop.challenge-1-2/add-forty
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Add 40 to :n"}

      {:onyx/name :multiply-by-5
       :onyx/fn :workshop.challenge-1-2/multiply-by-5
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Multiply :n by 5"}

      {:onyx/name :write-segments
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;

(defn cube-n [segment]
  (update-in segment [:n] (partial * (:n segment) (:n segment))))

(defn add-ten [segment]
  (update-in segment [:n] (partial + 10)))

(defn add-forty [segment]
  (update-in segment [:n] (partial + 40)))

(defn multiply-by-5 [segment]
  (update-in segment [:n] (partial * 5)))

;;; Lifecycles ;;;

(defn inject-reader-ch [event lifecycle]
  {:core.async/chan (u/get-input-channel (:core.async/id lifecycle))})

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

(def reader-lifecycle
  {:lifecycle/before-task-start inject-reader-ch})

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.challenge-1-2/reader-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :workshop.challenge-1-2/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])