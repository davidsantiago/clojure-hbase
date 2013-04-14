(defproject clojure-hbase "0.92.2"
  :description "A convenient Clojure interface to HBase."
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.hbase/hbase "0.92.2"]
                 [log4j/log4j "1.2.16" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.apache.hbase/hbase "0.92.2" :classifier "tests" :scope "test"]
                 [org.apache.hadoop/hadoop-test "1.1.0" :scope "test"]]
  :profiles {:clojure1.2 {:dependencies [[org.clojure/clojure "1.2.0"]]}
             :clojure1.3 {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :clojure1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :clojure1.5 {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :hbase92 {:dependencies [[org.apache.hbase/hbase "0.92.2"]
                                      [org.apache.hbase/hbase "0.92.2" :classifier "tests" :scope "test"]
                                      [org.apache.hadoop/hadoop-test "1.1.0" :scope "test"]]}
             :hbase94 {:dependencies [[org.apache.hbase/hbase "0.94.1"]
                                      [org.apache.hbase/hbase "0.94.1" :classifier "tests" :scope "test"]
                                      [org.apache.hadoop/hadoop-test "1.1.0" :scope "test"]]}
             :cdh4 {:dependencies [[org.apache.hbase/hbase "0.92.1-cdh4.1.2"]
                                   [org.apache.hadoop/hadoop-common "2.0.0-cdh4.1.2" :scope "test"]
                                   [org.apache.hadoop/hadoop-hdfs "2.0.0-cdh4.1.2" :scope "test"]
                                   [org.apache.hbase/hbase "0.92.1-cdh4.1.2" :classifier "tests" :scope "test"]
                                   [org.apache.hadoop/hadoop-common "2.0.0-cdh4.1.2" :classifier "tests" :scope "test"]
                                   [org.apache.hadoop/hadoop-hdfs "2.0.0-cdh4.1.2" :classifier "tests" :scope "test"]
                                   [org.apache.hadoop/hadoop-test "2.0.0-mr1-cdh4.1.2" :scope "test"]]
                    :repositories [["cloudera" "https://repository.cloudera.com/content/groups/public/"]]}})
