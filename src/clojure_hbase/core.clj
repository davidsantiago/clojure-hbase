(ns clojure-hbase.core
  (:refer-clojure :rename {get map-get})
  (:use clojure-hbase.internal)
  (:import [org.apache.hadoop.hbase HBaseConfiguration HConstants KeyValue]
           [org.apache.hadoop.hbase.client HTable
            HTablePool Get Put Delete Scan Result RowLock]
           [org.apache.hadoop.hbase.util Bytes]))

(def ^{:private true} put-class
  (Class/forName "org.apache.hadoop.hbase.client.Put"))
(def ^{:private true} get-class
  (Class/forName "org.apache.hadoop.hbase.client.Get"))
(def ^{:private true} delete-class
  (Class/forName "org.apache.hadoop.hbase.client.Delete"))
(def ^{:private true} scan-class
  (Class/forName "org.apache.hadoop.hbase.client.Scan"))

(def #^HTablePool ^:dynamic ^{:private true} *db*
  "This holds the HTablePool reference for all users. Users never have to see
   this, and the HBase API does not appear to me to allow configuration in code
   nor the use of multiple databases simultaneously (configuration is driven by
   the XML config files). So we just hide this detail from the user."
   (atom nil))

(defn- ^HTablePool htable-pool
  []
  (if-let [pool @*db*]
    pool
    (swap! *db* (fn [_] (HTablePool.)))))

(defmulti to-bytes-impl
  "Converts its argument into an array of bytes. By default, uses HBase's
   Bytes/toBytes and does nothing to byte arrays. Since it is a multimethod
   you can redefine it to create your own serialization routines for new types."
  class)
(defmethod to-bytes-impl (Class/forName "[B")
  [arg]
  arg)
(defmethod to-bytes-impl clojure.lang.Keyword
  [arg]
  (Bytes/toBytes ^String (name arg)))
(defmethod to-bytes-impl clojure.lang.Symbol
  [arg]
  (Bytes/toBytes ^String (name arg)))
(defmethod to-bytes-impl clojure.lang.IPersistentList
  [arg]
  (let [list-as-str (binding [*print-dup* false] (pr-str arg))]
    (Bytes/toBytes ^String list-as-str)))
(defmethod to-bytes-impl clojure.lang.IPersistentVector
  [arg]
  (let [vec-as-str (binding [*print-dup* false] (pr-str arg))]
    (Bytes/toBytes ^String vec-as-str)))
(defmethod to-bytes-impl clojure.lang.IPersistentMap
  [arg]
  (let [map-as-str (binding [*print-dup* false] (pr-str arg))]
    (Bytes/toBytes ^String map-as-str)))
(defmethod to-bytes-impl :default
  [arg]
  (Bytes/toBytes arg))

(defn to-bytes
  "Converts its argument to an array of bytes using the to-bytes-impl
   multimethod. We can't type hint a multimethod, so we type hint this
   shell function and calls all over this module don't need reflection."
  {:tag (Class/forName "[B")}
  [arg]
  (to-bytes-impl arg))

(defn as-map
  "Extracts the contents of the Result object and sticks them into a 3-level
   map, indexed by family, qualifier, and then timestamp.

   Functions can be passed in with arguments :map-family, :map-qualifier,
   :map-timestamp, and :map-value. You can also use :map-default to pick a
   default function, which will be overriden by the more specific directives."
  [#^Result result & args]
  (let [options      (into {} (map vec (partition 2 args)))
        default-fn   (map-get options :map-default identity)
        family-fn    (map-get options :map-family default-fn)
        qualifier-fn (map-get options :map-qualifier default-fn)
        timestamp-fn (map-get options :map-timestamp default-fn)
        value-fn     (map-get options :map-value default-fn)]
    (loop [remaining-kvs (seq (.raw result))
           kv-map {}]
      (if-let [^KeyValue kv (first remaining-kvs)]
        (let [family    (family-fn (.getFamily kv))
              qualifier (qualifier-fn (.getQualifier kv))
              timestamp (timestamp-fn (.getTimestamp kv))
              value     (value-fn (.getValue kv))]
          (recur (next remaining-kvs)
                 (assoc-in kv-map [family qualifier timestamp] value)))
        kv-map))))

(defn latest-as-map
  "Extracts the contents of the Result object and sticks them into a 2-level
   map, indexed by family and qualifier. The latest timestamp is used.

   Functions can be passed in with arguments :map-family, :map-qualifier,
   and :map-value. You can also use :map-default to pick a default function,
   which will be overriden by the more specific directives."
  [#^Result result & args]
  (let [options      (into {} (map vec (partition 2 args)))
        default-fn   (map-get options :map-default identity)
        family-fn    (map-get options :map-family default-fn)
        qualifier-fn (map-get options :map-qualifier default-fn)
        value-fn     (map-get options :map-value default-fn)]
    (loop [remaining-kvs (seq (.raw result))
           keys #{}]
      (if-let [^KeyValue kv (first remaining-kvs)]
        (let [family    (.getFamily kv)
              qualifier (.getQualifier kv)]
          (recur (next remaining-kvs)
                 (conj keys [family qualifier])))
        ;; At this point, we have a duplicate-less list of [f q] keys in keys.
        ;; Go back through, pulling the latest values for these keys.
        (loop [remaining-keys keys
               kv-map {}]
          (if-let [[family qualifier] (first remaining-keys)]
            (recur (next remaining-keys)
                   (assoc-in kv-map [(family-fn family)
                                     (qualifier-fn qualifier)]
                             (value-fn (.getValue result family qualifier))))
            kv-map))))))

(defn as-vector
  "Extracts the contents of the Result object and sticks them into a
   vector tuple of [family qualifier timestamp value]; returns a sequence
   of such vectors.

   Functions can be passed in with arguments :map-family, :map-qualifier,
   :map-timestamp, and :map-value. You can also use :map-default to pick a
   default function, which will be overriden by the more specific directives."
  [#^Result result & args]
  (let [options      (into {} (map vec (partition 2 args)))
        default-fn   (map-get options :map-default identity)
        family-fn    (map-get options :map-family default-fn)
        qualifier-fn (map-get options :map-qualifier default-fn)
        timestamp-fn (map-get options :map-timestamp default-fn)
        value-fn     (map-get options :map-value default-fn)]
    (loop [remaining-kvs (seq (.raw result))
           kv-vec (transient [])]
      (if-let [^KeyValue kv (first remaining-kvs)]
        (let [family    (family-fn (.getFamily kv))
              qualifier (qualifier-fn (.getQualifier kv))
              timestamp (timestamp-fn (.getTimestamp kv))
              value     (value-fn (.getValue kv))]
          (recur (next remaining-kvs)
                 (conj! kv-vec [family qualifier timestamp value])))
        (persistent! kv-vec)))))

(defn scanner
  "Creates a Scanner on the given table using the given Scan."
  [#^HTable table #^Scan scan]
  (io!
   (.getScanner table scan)))

(defn table
  "Gets an HTable from the open HTablePool by name."
  [table-name]
  (io!
   (.getTable (htable-pool) (to-bytes table-name))))

(defn release-table
  "Puts an HTable back into the open HTablePool."
  [#^HTable table]
  (io!
   (.putTable (htable-pool) table)))

;; with-table and with-scanner are basically the same function, but I couldn't
;; figure out a way to generate them both with the same macro.
(defmacro with-table
  "Executes body, after creating the tables given in bindings. Any table
   created in this way (use the function table) will automatically be returned
   to the HTablePool when the body finishes."
  [bindings & body]
  {:pre [(vector? bindings)
         (even? (count bindings))]}
  (cond
   (= (count bindings) 0) `(do ~@body)
   (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                             (try
                               (with-table ~(subvec bindings 2) ~@body)
                               (finally
                                (release-table ~(bindings 0)))))
   :else (throw (IllegalArgumentException.
                 "Bindings must be symbols."))))

(defmacro with-scanner
  "Executes body, after creating the scanners given in the bindings. Any scanner
   created in this way (use the function scanner or scan!) will automatically
   be closed when the body finishes."
  [bindings & body]
  {:pre [(vector? bindings)
         (even? (count bindings))]}
  (cond
   (= (count bindings) 0) `(do ~@body)
   (symbol? (bindings 0)) `(let ~(subvec bindings 0 2)
                             (try
                               (with-scanner ~(subvec bindings 2) ~@body)
                               (finally
                                (.close ~(bindings 0)))))
   :else (throw (IllegalArgumentException. "Bindings must be symbols."))))

(defn row-lock
  "Returns a RowLock on the given row of the given table."
  [#^HTable table row]
  (io!
   (.lockRow table (to-bytes row))))

(defn row-unlock
  "Unlocks the row locked by the given RowLock."
  [#^HTable table row-lock]
  (io!
   (.unlockRow table row-lock)))

(defn query
  "Performs the given query actions (Get/Scan) on the given HTable. The return
   value will be a sequence of equal length, with each slot containing the
   results of the query in the corresponding position."
  [#^HTable table & ops]
  (io!
   (map (fn [op]
          (condp instance? op
            get-class   (.get table #^Get op)
            scan-class  (scanner table op)
            (throw (IllegalArgumentException.
                    "Arguments must be Get or Scan objects."))))
        ops)))

(defn modify
  "Performs the given modifying actions (Put/Delete) on the given HTable."
  [#^HTable table & ops]
  (io!
   (map (fn [op]
          (condp instance? op
            put-class     (.put table #^Put op)
            delete-class  (.delete table #^Delete op)
            (throw (IllegalArgumentException.
                    "Arguments must be Put or Delete objects."))))
        ops)))

;;
;;  GET
;;

(defn- make-get
  "Makes a Get object, taking into account user directives, such as using
   an existing Get, or passing a pre-existing RowLock."
  [row options]
  (let [row (to-bytes row)
        directives #{:row-lock :use-existing}
        cons-opts (apply hash-map (flatten (filter
                                            #(contains? directives
                                                        (first %)) options)))]
    (condp contains? cons-opts
      :use-existing (io! (:use-existing cons-opts))
      :row-lock     (new Get row (:row-lock cons-opts))
      (new Get row))))

(def ^{:private true} get-argnums
  "This maps each get command to its number of arguments, for helping us
   partition the command sequence."
  {:column       1    ;; :column [:family-name :qualifier]
   :columns      1    ;; :columns [:family-name [:qual1 :qual2...]...]
   :family       1    ;; :family :family-name
   :families     1    ;; :families [:family1 :family2 ...]
   :filter       1    ;; :filter <a filter you've made>
   :all-versions 0    ;; :all-versions
   :max-versions 1    ;; :max-versions <int>
   :time-range   1    ;; :time-range [start end]
   :time-stamp   1    ;; :time-stamp time
   :row-lock     1    ;; :row-lock <a row lock you've got>
   :use-existing 1}   ;; :use-existing <some Get you've made>
  )

(defn- handle-get-columns
  "Handles the case where a get operation has requested columns with the
   :columns specifier (ie, :columns [:family [:col1 :col2 :col3]]). Returns
   the get operation, but remember, it is mutable. Note that this function
   is also used by the bindings for Scan, since they have the same functions."
  [^Get get-op columns]
  (doseq [column (partition 2 columns)] ;; :family [:cols...] pairs.
    (let [[family qualifiers] column]
      (doseq [q qualifiers]
        (.addColumn get-op (to-bytes family) (to-bytes q)))))
  get-op)

(defn get*
  "Returns a Get object suitable for performing a get on an HTable. To make
   modifications to an existing Get object, pass it as the argument to
   :use-existing; to use an existing RowLock, pass it as the argument to
   :row-lock."
  [row & args]
  (let [specs (partition-query args get-argnums)
        #^Get get-op (make-get row specs)]
    (doseq [spec specs]
      (condp = (first spec)
          :column       (apply #(.addColumn get-op
                                            (to-bytes %1) (to-bytes %2))
                               (second spec))
          :columns      (handle-get-columns get-op (second spec))
          :family       (.addFamily get-op (to-bytes (second spec)))
          :families     (for [f (second spec)]
                          (.addFamily get-op (to-bytes f)))
          :filter       (.setFilter get-op (second spec))
          :all-versions (.setMaxVersions get-op)
          :max-versions (.setMaxVersions get-op (second spec))
          :time-range   (apply #(.setTimeRange get-op %1 %2) (second spec))
          :time-stamp   (.setTimeStamp get-op (second spec))))
    get-op))

(defn get
  "Creates and executes a Get object against the given table. Options are
   the same as for get."
  [#^HTable table row & args]
  (let [g #^Get (apply get* row args)]
    (io!
     (.get table g))))

;;
;;  PUT
;;

(def ^{:private true} put-argnums
  "This maps each put command to its number of arguments, for helping us
   partition the command sequence."
  {:value        1    ;; :value [:family :column <value>]
   :values       1    ;; :values [:family [:column1 value1 ...] ...]
   :write-to-WAL 1    ;; :write-to-WAL true/false
   :row-lock     1    ;; :row-lock <a row lock you've got>
   :use-existing 1}   ;; :use-existing <a Put you've made>
  )

(defn- make-put
  "Makes a Put object, taking into account user directives, such as using
   an existing Put, or passing a pre-existing RowLock."
  [row options]
  (let [row (to-bytes row)
        directives #{:row-lock :use-existing}
        cons-opts (apply hash-map (flatten (filter
                                            #(contains? directives
                                                        (first %)) options)))]
    (condp contains? cons-opts
      :use-existing (io! (:use-existing cons-opts))
      :row-lock     (new Put row ^RowLock (:row-lock cons-opts))
      (new Put row))))

(defn- put-add
  [#^Put put-op family qualifier value]
  (.add put-op (to-bytes family) (to-bytes qualifier) (to-bytes value)))

(defn- handle-put-values
  [#^Put put-op values]
  (doseq [value (partition 2 values)]
    (let [[family qv-pairs] value]
      (doseq [[q v] (partition 2 qv-pairs)]
        (put-add put-op family q v))))
  put-op)

(defn put*
  "Returns a Put object suitable for performing a put on an HTable. To make
   modifications to an existing Put object, pass it as the argument to
   :use-existing; to use an existing RowLock, pass it as the argument to
   :row-lock."
  [row & args]
  (let [specs  (partition-query args put-argnums)
        #^Put put-op (make-put row specs)]
    (doseq [spec specs]
      (condp = (first spec)
          :value          (apply put-add put-op (second spec))
          :values         (handle-put-values put-op (second spec))
          :write-to-WAL   (.setWriteToWAL put-op (second spec))))
    put-op))

(defn put
  "Creates and executes a Put object against the given table. Options are
   the same as for put."
  [#^HTable table row & args]
  (let [p #^Put (apply put* row args)]
    (io!
     (.put table p))))

(defn check-and-put
  "Atomically checks that the row-family-qualifier-value match the values we
   give, and if so, executes the Put."
  ([#^HTable table row family qualifier value #^Put put]
     (.checkAndPut table (to-bytes row) (to-bytes family) (to-bytes qualifier)
                   (to-bytes value) put))
  ([#^HTable table [row family qualifier value] #^Put put]
     (check-and-put table row family qualifier value put)))

(defn insert
  "If the family and qualifier are non-existent, the Put will be committed.
   The row is taken from the Put object, but the family and qualifier cannot
   be determined from a Put object, so they must be specified."
  [#^HTable table family qualifier ^Put put]
  (check-and-put table (.getRow put) family qualifier
                 (byte-array 0) put))

;;
;; DELETE
;;

(def ^{:private true} delete-argnums
  "This maps each delete command to its number of arguments, for helping us
   partition the command sequence."
  {:column                1    ;; :column [:family-name :qualifier]
   :columns               1    ;; :columns [:family-name [:q1 :q2...]...]
   :family                1    ;; :family :family-name
   :families              1    ;; :families [:family1 :family2 ...]
   :with-timestamp        2    ;; :with-timestamp <long> [:column [...]
   :with-timestamp-before 2    ;; :with-timestamp-before <long> [:column ...]
   :row-lock              1    ;; :row-lock <a row lock you've got>
   :use-existing          1}   ;; :use-existing <a Put you've made>
  )

(defn- make-delete
  "Makes a Delete object, taking into account user directives, such as using
   an existing Delete, or passing a pre-existing RowLock."
  [row options]
  (let [row (to-bytes row)
        directives #{:row-lock :use-existing}
        cons-opts (apply hash-map (flatten (filter
                                            #(contains? directives (first %))
                                            options)))]
    (condp contains? cons-opts
      :use-existing (io! (:use-existing cons-opts))
      :row-lock     (new Delete row HConstants/LATEST_TIMESTAMP
                         (:row-lock cons-opts))
      (new Delete row))))

(defn- delete-column
  [#^Delete delete-op family qualifier]
  (.deleteColumn delete-op (to-bytes family) (to-bytes qualifier)))

(defn- delete-column-with-timestamp
  [#^Delete delete-op family qualifier timestamp]
  (.deleteColumn delete-op (to-bytes family) (to-bytes qualifier) timestamp))

(defn- delete-column-before-timestamp
  [#^Delete delete-op family qualifier timestamp]
  (.deleteColumns delete-op (to-bytes family) (to-bytes qualifier) timestamp))

(defn- delete-family
  [#^Delete delete-op family]
  (.deleteFamily delete-op (to-bytes family)))

(defn- delete-family-timestamp
  [#^Delete delete-op family timestamp]
  (.deleteFamily delete-op (to-bytes family) timestamp))

(defn- handle-delete-ts
  [#^Delete delete-op ts-specs]
  (doseq [[ts-op timestamp specs] (partition 3 ts-specs)
          spec specs]
    (condp = ts-op
        :with-timestamp
      (condp = (first spec)
          :column
        (apply #(delete-column-with-timestamp delete-op %1 %2 timestamp)
               (rest spec))
        :columns (let [[family quals] (rest spec)]
                   (doseq [q quals]
                     (delete-column-with-timestamp
                       delete-op family q timestamp))))
      :with-timestamp-before
      (condp = (first spec)
          :column
        (apply #(delete-column-before-timestamp delete-op %1 %2 timestamp)
               (rest spec))
        :columns (let [[family quals] (rest spec)]
                   (doseq [q quals]
                     (delete-column-before-timestamp
                      delete-op family q timestamp)))
        :family (delete-family-timestamp delete-op (second spec) timestamp)
        :families (doseq [f (rest spec)]
                    (delete-family-timestamp delete-op f timestamp))))))

(defn delete*
  "Returns a Delete object suitable for performing a delete on an HTable. To
   make modifications to an existing Delete object, pass it as the argument to
   :use-existing; to use an existing RowLock, pass it as the argument to
   :row-lock."
  [row & args]
  (let [specs     (partition-query args delete-argnums)
        delete-op (make-delete row specs)]
    (doseq [spec specs]
      (condp = (first spec)
          :with-timestamp        (handle-delete-ts delete-op spec)
          :with-timestamp-before (handle-delete-ts delete-op spec)
          :column                (apply #(delete-column delete-op %1 %2)
                                        (second spec))
          :columns               (let [[family quals] (rest spec)]
                                   (doseq [q quals]
                                     (delete-column delete-op family q)))
          :family                (delete-family delete-op (second spec))
          :families              (doseq [f (rest spec)]
                                   (delete-family delete-op f))))
    delete-op))

(defn delete
  "Creates and executes a Delete object against the given table. Options are
   the same as for delete."
  [#^HTable table row & args]
  (let [d #^Delete (apply delete* row args)]
    (io!
     (.delete table d))))

;;
;; SCAN
;;

(def ^{:private true} scan-argnums
  "This maps each scan command to its number of arguments, for helping us
   partition the command sequence."
  {:column       1    ;; :column [:family-name :qualifier]
   :columns      1    ;; :columns [:family-name [:qual1 :qual2...]...]
   :family       1    ;; :family :family-name
   :families     1    ;; :families [:family1 :family2 ...]
   :filter       1    ;; :filter <a filter you've made>
   :all-versions 0    ;; :all-versions
   :max-versions 1    ;; :max-versions <int>
   :time-range   1    ;; :time-range [start end]
   :time-stamp   1    ;; :time-stamp time
   :start-row    1    ;; :start-row row
   :stop-row     1    ;; :stop-row row
   :use-existing 1}   ;; :use-existing <some Get you've made>
  )

(defn- make-scan
  [options]
  (let [directives #{:use-existing}
        cons-opts (apply hash-map (flatten (filter
                                            #(contains? directives (first %))
                                            options)))]
    (condp contains? cons-opts
      :use-existing (io! (:use-existing cons-opts))
      (Scan.))))

(defn scan*
  "Returns a Scan object suitable for performing a scan on an HTable. To make
   modifications to an existing Scan object, pass it as the argument to
   :use-existing."
  [& args]
  (let [specs   (partition-query args scan-argnums)
        scan-op #^Scan (make-scan specs)]
    (doseq [spec specs]
      (condp = (first spec)
          :column       (apply #(.addColumn scan-op
                                            (to-bytes %1) (to-bytes %2))
                               (second spec))
          :columns      (handle-get-columns scan-op (second spec))
          :family       (.addFamily scan-op (to-bytes (second spec)))
          :families     (for [f (second spec)]
                          (.addFamily scan-op (to-bytes f)))
          :filter       (.setFilter scan-op (second spec))
          :all-versions (.setMaxVersions scan-op)
          :max-versions (.setMaxVersions scan-op (second spec))
          :time-range   (apply #(.setTimeRange scan-op %1 %2) (second spec))
          :time-stamp   (.setTimeStamp scan-op (second spec))
          :start-row    (.setStartRow scan-op (to-bytes (second spec)))
          :stop-row     (.setStopRow scan-op (to-bytes (second spec)))))
    scan-op))

(defn scan
  "Creates and runs a Scan object. All arguments are the same as scan.
   ResultScanner implements Iterable, so you should be able to just use it
   directly, but don't forget to .close it! Better yet, use with-scanner."
  [#^HTable table & args]
  (let [s (apply scan* args)]
    (io!
     (scanner table s))))