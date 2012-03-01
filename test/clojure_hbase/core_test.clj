(ns clojure-hbase.core-test
  (:refer-clojure :rename {get map-get})
  (:use clojure.test
        clojure.stacktrace
        [clojure-hbase.core]
        [clojure-hbase.admin :exclude [flush]])
  (:import [org.apache.hadoop.hbase.util Bytes]
           [java.util UUID]))

;; This file creates a table to do all its work in, and requires an already-
;; configured running instance of HBase. Obviously, make sure this is not a
;; production version of HBase you're testing on.

(def test-tbl-name (str "clojure-hbase-test-db" (UUID/randomUUID)))
(defn setup-tbl [] (create-table (table-descriptor test-tbl-name)))
(defn remove-tbl []
  (disable-table test-tbl-name)
  (delete-table test-tbl-name))

(defmacro as-test [& body]
  `(do
     (try
       (setup-tbl)
       ~@body
       (catch Throwable t# (print-cause-trace t#))
       (finally
        (remove-tbl)))))

(deftest create-delete-table
  (as-test
   (is (.contains (map #(Bytes/toString (.getName %)) (list-tables))
                  test-tbl-name)
       "The table was created at the beginning of the as-test."))
  (is (not (.contains (map #(Bytes/toString (.getName %)) (list-tables))
                      test-tbl-name))
      "Now that we are out of the as-test, the table doesn't exist."))

(deftest add-delete-CF
  (let [cf-name "test-cf-name"]
    (as-test
     (disable-table test-tbl-name)
     (add-column-family test-tbl-name (column-descriptor cf-name))
     (is (= (.getNameAsString (.getFamily
                               (get-table-descriptor test-tbl-name)
                               (to-bytes cf-name)))
            cf-name)
         "Created a new column family and retrieved its column descriptor.")
     (delete-column-family test-tbl-name cf-name)
     (is (= nil (.getFamily (get-table-descriptor test-tbl-name)
                            (to-bytes cf-name)))
         "Deleted the column family successfully."))))

(deftest get-put-delete
  (let [cf-name "test-cf-name"
        row     "testrow"
        value   "testval"]
    (as-test
     (disable-table test-tbl-name)
     (add-column-family test-tbl-name (column-descriptor cf-name))
     (enable-table test-tbl-name)
     (with-table [test-tbl (table test-tbl-name)]
       (put test-tbl row :value [cf-name :testqual value])
       (is (= value (Bytes/toString (last (first
                                           (as-vector
                                            (get test-tbl row :column
                                                 [cf-name :testqual]))))))
           "Successfully executed Put and Get.")
       (delete test-tbl row :column [cf-name :testqual])
       (is (= '() (as-vector (get test-tbl row :column
                                  [cf-name :testqual])))
           "Successfully executed Delete of the Put.")))))

(def scan-row-values (sort-by #(first %)
                              (for [k (range 10000)]
                                [(str (UUID/randomUUID))
                                 (str (UUID/randomUUID))])))

(deftest scan-check
  (let [cf-name "test-cf-name"]
    (as-test
     (disable-table test-tbl-name)
     (add-column-family test-tbl-name (column-descriptor cf-name))
     (enable-table test-tbl-name)
     (with-table [test-tbl (table test-tbl-name)]
       (doseq [[key value] scan-row-values]
         (put test-tbl key :value [cf-name :value value]))
       (is (= true
              (reduce #(and %1 %2)
                      (with-scanner [scan-results (scan test-tbl)]
                        (map #(= (first %1)
                                 (Bytes/toString (.getRow %2)))
                             scan-row-values (seq scan-results))))))))))

(deftest as-map-test
  (let [cf-name "test-cf-name"
        qual    "testqual"
        row     "testrow"
        value   "testval"]
    (as-test
     (disable-table test-tbl-name)
     (add-column-family test-tbl-name (column-descriptor cf-name))
     (enable-table test-tbl-name)
     (with-table [test-tbl (table test-tbl-name)]
       (put test-tbl row :value [cf-name qual value])
       (is (= value
              (first (vals (get-in (as-map (get test-tbl row)
                                           :map-family    #(Bytes/toString %)
                                           :map-qualifier #(Bytes/toString %)
                                           :map-timestamp str
                                           :map-value     #(Bytes/toString %))
                                   [cf-name qual]))))
           "as-map works.")
       (is (= {cf-name {qual value}}
              (latest-as-map (get test-tbl row)
                             :map-family    #(Bytes/toString %)
                             :map-qualifier #(Bytes/toString %)
                             :map-value     #(Bytes/toString %)))
           "latest-as-map works.")))))

(deftest test-set-config
  (as-test
   (is
    (try (set-config
          (make-config "hbase.zookeeper.quorum" "asdsa")) ;<- not valid
         (table test-tbl-name) ;<- should raise exception
         false #_"<- fail if we got here, it should have thrown"
         (catch Exception e
           true)))
   (is
    (do
      (set-config
       (make-config "hbase.zookeeper.quorum" "127.0.0.1")) ;<- valid
      (table test-tbl-name)))))
