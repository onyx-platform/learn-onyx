(defproject lambdajam "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.onyxplatform/onyx "0.7.0-SNAPSHOT"]
                 [com.stuartsierra/component "0.2.3"]
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.slf4j/slf4j-nop "1.7.12"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.10"]
                                  [pjstadig/humane-test-output "0.7.0"]]
                   :source-paths ["env/dev" "src"]
                   :injections [(require 'pjstadig.humane-test-output)
                                (pjstadig.humane-test-output/activate!)]}})
