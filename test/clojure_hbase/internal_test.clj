(ns clojure-hbase.internal-test
  (:use clojure.test
        clojure-hbase.internal))

(deftest arity-test
  (are [f n] (= n (arity f))
       cons 2
       + 2
       inc 1
       (fn [a b c] 2) 3
       (fn [a b c & r] r) 4))
