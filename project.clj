(defproject clojure-hbase "0.90.4"
  :description "A convenient Clojure interface to HBase."
  :dependencies [[org.clojure/clojure "1.2.0"]
                 [org.apache.hadoop/hadoop-core "0.20.2-dev"]
                 [org.apache.hbase/hbase "0.90.4"]
                 [org.apache.zookeeper/zookeeper "3.3.3"]
                 [log4j/log4j "1.2.15" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [commons-logging/commons-logging "1.0.4"]])