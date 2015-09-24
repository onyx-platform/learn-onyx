(ns workshop.cheat-sheet)
;;;;;;;; CHEAT SHEET ;;;;;;;;

;;;;;;;;;;;;;;;;;
;; Workflow example

; :A is an input task
; :C is an output task
; :B is a function task
; The flow is:
; :A -> :B -> :C
(def workflow
  [[:A :B]
   [:B :C]])

;;;;;;;;;;;;;;;;;
;; Catalog Entries
;; See information model for allowed keys:
;; https://onyx-platform.gitbooks.io/onyx/content/doc/user-guide/information-model.html

[;; Input task
 {:onyx/name :your-input-task-name
  :onyx/plugin :path-to-plugin-builder/function
  :onyx/type :input
  :onyx/medium :plugin-medium
  ;; task will read up to batch-size segments at a time
  :onyx/batch-size 20 
  ;; task will wait no longer than batch-timeout to read segments
  :onyx/batch-timeout 1000 
  ;; enable to restrict the number of peers working on this task
  ; :onyx/max-peers 1 
  ;; enable to force a minumum number of peers onto this task
  ; :onyx/min-peers 1 
  :onyx/doc "Documentation for this catalog entry"}

 ;; Function task
 ;; https://onyx-platform.gitbooks.io/onyx/content/doc/user-guide/functions.html
 {:onyx/name :your-task-name
  :onyx/fn :your-ns/your-fn ; ::your-fn also works if you're in the generating ns
  :onyx/type :function
  ;; Add a parameter to your-fn from catalog entry e.g. (fn [v segment])
  ; :onyx/params [:some/key]
  ; :some/key 3
  :onyx/batch-size 20 
  :onyx/batch-timeout 1000 
  :onyx/doc "Returns the segment"}

 ;; Group by Key task
 {:onyx/name :group-by-key-task-name
  :onyx/fn ::your-fn
  :onyx/type :function
  :onyx/group-by-key :name
  :onyx/min-peers 3
  :onyx/batch-size 20 
  :onyx/batch-timeout 1000 
  :onyx/flux-policy :continue}

 ;; Group by Function task
 {:onyx/name :group-by-fn-task-name
  :onyx/fn ::your-fn
  :onyx/type :function
  :onyx/group-by-fn :your-ns/your-grouping-function
  :onyx/min-peers 3
  :onyx/flux-policy :continue
  :onyx/batch-size 20 
  :onyx/batch-timeout 1000}

 ;; Output task
 {:onyx/name :your-output-task-name
  :onyx/plugin :path-to-plugin-builder/function
  :onyx/type :output
  :onyx/medium :plugin-medium
  :onyx/batch-size 20 
  :onyx/batch-timeout 1000
  :onyx/doc "Output task doc"}]

;;;;;;;;;;;;;;;;;
;; https://onyx-platform.gitbooks.io/onyx/content/doc/user-guide/functions.html
;; Functions (for use on onyx/fn task)
(defn your-fn [segment]
  ;; do something with the segment and return a segment
  (update segment :n inc))

;;;;;;;;;;;;;;;;;
;; Lifecycles
;; https://onyx-platform.gitbooks.io/onyx/content/doc/user-guide/lifecycles.html

;; Lifecycle function examples for each lifecycle type
;; Notice that all lifecycle functions return maps except `start-task?`.
(defn start-task? [event lifecycle]
  (println "Executing once before the task starts.")
  true)

(defn before-task-start [event lifecycle]
  (println "Executing once before the task starts.")
  {})

(defn after-task-stop [event lifecycle]
  (println "Executing once after the task is over.")
  {})

(defn before-batch [event lifecycle]
  (println "Executing once before each batch.")
  {})

(defn after-batch [event lifecycle]
  (println "Executing once after each batch.")
  {})

(defn after-ack-segment [event message-id rets lifecycle]
  (println "Message " message-id " is fully acked"))

(defn after-retry-segment [event message-id rets lifecycle]
  (println "Retrying message " message-id))

;; Lifecycle calls example.
;; Lifecycle calls map the lifecycle name to a function to call
(def calls
  {:lifecycle/start-task? start-task?
   :lifecycle/before-task-start before-task-start
   :lifecycle/before-batch before-batch
   :lifecycle/after-batch after-batch
   :lifecycle/after-task-stop after-task-stop
   :lifecycle/after-ack-segment after-ack-segment
   :lifecycle/after-retry-segment after-retry-segment})

;; Lifecycle entries map lifecycle calls to the tasks they should be run on.
;; The entries are submitted as part of the job data
(def lifecycles
  [{:lifecycle/task :all ;; task-name or :all to use with all tasks
    :lifecycle/calls ::calls
    :lifecycle/doc "Lifecycle calls doc"}])

;;;;;;;;;;;;;;;;;
;; Flow Conditions
;; https://onyx-platform.gitbooks.io/onyx/content/doc/user-guide/flow-conditions.html

(def flow-conditions
  [{:flow/from :from-task
    :flow/to [:to-task1 :to-task2] ;; :all will direct to all tasks
    :my/parameter 17
    :flow/predicate [:my.ns/my-predicate-fn? :my/parameter]
    :flow/doc "Emits segment if my-predicate-fn? is true for the segment 
              (in this case (:some-key segment) = 17)"}])

(defn my-predicate-fn? [event old-segment new-segment all-new-segments parameter]
  (= (:some-key new-segment) parameter))
