(ns lambdajam-2015.catalogs.level-0.catalog-0-0)

(def batch-size 20)

(def catalog
  [{:onyx/name :read-segments
    :onyx/ident :core.async/read-from-chan
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :no-transform
    :onyx/fn :lambdajam-2015.functions.level-0.functions-0-0/no-transform
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/doc "Returns segments unchanged."}

   {:onyx/name :write-segments
    :onyx/ident :core.async/write-to-chan
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])
