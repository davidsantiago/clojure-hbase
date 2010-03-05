(ns clojure-hbase-util-test
  (:refer-clojure :rename {get map-get})
  (:use clojure.test
	com.davidsantiago.clojure-hbase.util)
  (:import org.apache.hadoop.hbase.util.Bytes))

(deftest check-simple-converters
  (is (= "test" (as-str (Bytes/toBytes "test"))))
  (is (= :test (as-kw (Bytes/toBytes "test"))))
  (is (= 'test (as-sym (Bytes/toBytes "test")))))