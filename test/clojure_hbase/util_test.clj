(ns clojure-hbase.util-test
  (:refer-clojure :rename {get map-get})
  (:use clojure.test
        clojure-hbase.core
        clojure-hbase.util)
  (:import org.apache.hadoop.hbase.util.Bytes))

(deftest check-simple-converters
  (is (= "test" (as-str (Bytes/toBytes "test"))))
  (is (= :test (as-kw (Bytes/toBytes "test"))))
  (is (= 'test (as-sym (Bytes/toBytes "test")))))

(deftest check-obj-converters
  (is (= '(1 2 3) (as-obj (to-bytes '(1 2 3)))))
  (is (= [1 2 3]) (as-obj (to-bytes [1 2 3])))
  (is (= {:test '(1 2 3)} (as-obj (to-bytes {:test '(1 2 3)})))))