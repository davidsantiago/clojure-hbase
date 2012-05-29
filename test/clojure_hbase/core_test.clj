(ns clojure-hbase.core-test
  (:refer-clojure :rename {get map-get})
  (:use clojure.test
        clojure.stacktrace
        [clojure-hbase.core]
        [clojure-hbase.admin :exclude [flush]])
  (:import [org.apache.hadoop.hbase.util Bytes]
           [java.util UUID]))

;; Testing Utilities

(defn keywordize [x] (keyword (Bytes/toString x)))

(defn test-vector
  [result]
  (as-vector result :map-family keywordize :map-qualifier keywordize
             :map-timestamp (fn [x] nil) :map-value keywordize))

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
        rowvalue   [[:test-cf-name :testqual nil :testval]]]
    (as-test
     (disable-table test-tbl-name)
     (add-column-family test-tbl-name (column-descriptor cf-name))
     (enable-table test-tbl-name)
     (with-table [test-tbl (table test-tbl-name)]
       (put test-tbl row :value [cf-name :testqual :testval])
       (is (= rowvalue
              (test-vector
               (get test-tbl row :column
                    [cf-name :testqual])))
           "Successfully executed Put :value and Get :column.")
       (is (= rowvalue
              (test-vector
               (get test-tbl row :family :test-cf-name)))
           "Successfully executed Get :family.")
       (let [timestamp (-> (get test-tbl row :column
                                [cf-name :testqual])
                           (as-vector) (first) (nth 2))]
         (is (= rowvalue (test-vector (get test-tbl row
                                           :time-stamp timestamp)))
             "Sucessfully executed Get :time-stamp")
         (is (= rowvalue (test-vector
                          (get test-tbl row
                               :time-range [(dec timestamp) (inc timestamp)])))
             "Successfully executed Get :time-range"))
       ;; Delete the row
       (delete test-tbl row :column [cf-name :testqual])
       (is (= '() (as-vector (get test-tbl row :column
                                  [cf-name :testqual])))
           "Successfully executed Delete of the Put.")))))

(deftest multicol-get-put-delete
  (let [row "testrow"
        rowvalue [[:test-cf-name1 :test1qual1 nil :testval1]
                  [:test-cf-name1 :test1qual2 nil :testval2]
                  [:test-cf-name2 :test2qual1 nil :testval3]
                  [:test-cf-name2 :test2qual2 nil :testval4]]
        subvalue [[:test-cf-name1 :test1qual1 nil :testval1]
                  [:test-cf-name1 :test1qual2 nil :testval2]]]
    (as-test
     (disable-table test-tbl-name)
     (add-column-family test-tbl-name (column-descriptor "test-cf-name1"))
     (add-column-family test-tbl-name (column-descriptor "test-cf-name2"))
     (enable-table test-tbl-name)
     (with-table [test-tbl (table test-tbl-name)]
       (put test-tbl row :values [:test-cf-name1 [:test1qual1 "testval1"
                                                  :test1qual2 "testval2"]
                                  :test-cf-name2 [:test2qual1 "testval3"
                                                  :test2qual2 "testval4"]])
       (is (= rowvalue (test-vector (get test-tbl row)))
           "Verified all columns were Put with an unqualified row Get.")
       (is (= subvalue
              (test-vector (get test-tbl row :columns
                                [:test-cf-name1 [:test1qual1 :test1qual2]])))
           "Successfully did a Get on subset of columns in row using :columns.")
       (is (= subvalue (test-vector (get test-tbl row
                                         :families [:test-cf-name1])))
           "Successfully executed Get on subset of columns by :families.")
       (delete test-tbl row :columns [:test-cf-name1 [:test1qual1 :test1qual2]
                                      :test-cf-name2 [:test2qual1 :test2qual2]])
       (is (= '() (test-vector (get test-tbl row :columns
                                  [:test-cf-name1 [:test1qual1 :test1qual2]
                                   :test-cf-name2 [:test2qual1 :test2qual2]])))
           "Successfully executed Delete of multiple cols using :columns.")))))

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

(deftest scan-limited-columns
  (let [cf-name "test-cf-name"]
    (as-test
     (disable-table test-tbl-name)
     (add-column-family test-tbl-name (column-descriptor cf-name))
     (enable-table test-tbl-name)
     (with-table [test-tbl (table test-tbl-name)]
       (put test-tbl 1 :values [cf-name [:a "1" :b "2" :c "3" :d "4"]])
       (put test-tbl 2 :values [cf-name [:a "5" :b "6" :c "7" :d "8"]])
       (testing "a smaller set of columns returned"
         (is (= [{:test-cf-name {:a "1"
                                 :b "2"}}
                 {:test-cf-name {:a "5"
                                 :b "6"}}]
                (with-scanner [scan-results (scan test-tbl :columns [:test-cf-name [:a :b]])]
                  (doall
                   (map (fn [x] (latest-as-map x
                                              :map-family    (comp keyword #(Bytes/toString %))
                                              :map-qualifier (comp keyword #(Bytes/toString %))
                                              :map-value     #(Bytes/toString %)))
                        
                        (-> scan-results .iterator iterator-seq))))))
         (is (empty?
              (with-scanner [scan-results (scan test-tbl :columns [:test-cf-name [:y :z]])]
                  (doall
                   (map (fn [x] (latest-as-map x
                                              :map-family    (comp keyword #(Bytes/toString %))
                                              :map-qualifier (comp keyword #(Bytes/toString %))
                                              :map-value     #(Bytes/toString %)))
                        
                        (-> scan-results .iterator iterator-seq)))))))))))

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
  (try
    (as-test
     (is
      (try (set-config {"hbase.zookeeper.quorum" "asdsa"}) ;<- not valid
           (table test-tbl-name)        ;<- should raise exception
           false #_"<- fail if we got here, it should have thrown"
           (catch Exception e
             true)))
     (is
      (do
        (set-config {"hbase.zookeeper.quorum" "127.0.0.1"}) ;<- valid
        (table test-tbl-name))))
    (finally
     (set-config (default-config)))))
