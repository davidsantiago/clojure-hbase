(ns clojure-hbase.admin-test
  (:refer-clojure :rename {get map-get})
  (:use clojure.test
        clojure.stacktrace
        [clojure-hbase.admin :exclude [flush]])
  (:import [org.apache.hadoop.hbase.util Bytes]
           [java.util UUID]))

;; No tests here for right now, but this should ensure that the admin module at
;; least gets read and compiled when tests are run.