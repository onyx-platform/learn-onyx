(ns lambdajam.challenge-5-1
  (:require [lambdajam.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :identity]
   [:identity :admins-output]
   [:identity :users-output]
   [:identity :guests-output]])

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

      {:onyx/name :identity
       :onyx/fn :clojure.core/identity
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "Returns the segment"}

      {:onyx/name :admins-output
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}

      {:onyx/name :users-output
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}

      {:onyx/name :guests-output
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

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
    :lifecycle/calls :lambdajam.challenge-5-1/reader-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :admins-output
    :lifecycle/calls :lambdajam.challenge-5-1/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :admins-output
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :users-output
    :lifecycle/calls :lambdajam.challenge-5-1/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :users-output
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :guests-output
    :lifecycle/calls :lambdajam.challenge-5-1/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :guests-output
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}])

;;; Flow conditions ;;;

;; <<< BEGIN FILL ME IN >>>

(defn admin? [event old-segment new-segment all-new-segments]
  (= (:status new-segment) :admin))

(defn user? [event old-segment new-segment all-new-segments]
  (= (:status new-segment) :user))

(defn guest? [event old-segment new-segment all-new-segments]
  (= (:status new-segment) :guest))

(def flow-conditions
  [{:flow/from :identity
    :flow/to [:admins-output]
    :flow/predicate :lambdajam.challenge-5-1/admin?}

   {:flow/from :identity
    :flow/to [:users-output]
    :flow/predicate :lambdajam.challenge-5-1/user?}

   {:flow/from :identity
    :flow/to [:guests-output]
    :flow/predicate :lambdajam.challenge-5-1/guest?}])

;; <<< END FILL ME IN >>>