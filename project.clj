(defproject clojure-hbase "1.0.0-SNAPSHOT"
  :description "A convenient Clojure interface to HBase."
  :dependencies [[org.clojure/clojure "1.1.0"]
		 [org.clojure/clojure-contrib "1.1.0-master-SNAPSHOT"]
		 [net.newsince/hbase "0.20.2"]
		 [net.newsince/hadoop-core "0.20.1"]
		 [net.newsince/zookeeper "3.2.1"]
		 [log4j/log4j "1.2.15"]
		 [commons-logging/commons-logging "1.0.4"]]
  :dev-dependencies [[swank-clojure/swank-clojure "1.1.0"]])

(ns leiningen.docs)
;(defn docs...)