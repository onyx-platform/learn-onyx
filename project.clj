(defproject workshop "0.10.0-beta14"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.10.0-20170504_205431-g9bf1741"]
                 [org.slf4j/slf4j-api "1.7.12"]
                 [org.slf4j/slf4j-nop "1.7.12"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.namespace "0.2.10"]
                                  [pjstadig/humane-test-output "0.7.0"]]
                   :jvm-opts ^:replace ["-server" "-Xmx2400M" "-XX:+UseG1GC"]
                   :plugins [[lein-update-dependency "0.1.2"]
                             [lein-set-version "0.4.1"]]
                   :source-paths ["env/dev" "src"]
                   :injections [(require 'pjstadig.humane-test-output)
                                (pjstadig.humane-test-output/activate!)]}})
