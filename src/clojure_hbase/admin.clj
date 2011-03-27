(ns clojure-hbase.admin
  (:refer-clojure :rename {get map-get} :exclude [flush])
  (:use clojure.contrib.def
        clojure-hbase.core
        clojure-hbase.internal)
  (:import [org.apache.hadoop.hbase HBaseConfiguration HConstants
            HTableDescriptor HColumnDescriptor]
           [org.apache.hadoop.hbase.client HBaseAdmin]
           [org.apache.hadoop.hbase.util Bytes]
           [org.apache.hadoop.hbase.io.hfile Compression]))

(defvar- ^HBaseAdmin *admin* (HBaseAdmin. (HBaseConfiguration/create)))

;;
;; HColumnDescriptor
;;

(defvar- column-desc-argnums
  {:block-cache-enabled      1  ;; :block-cache-enabled <boolean>
   :block-size               1  ;; :block-size <int>
   :bloom-filter-type        1  ;; :bloom-filter <StoreFile.BloomType>
   :compression-type         1  ;; :compression-type <Compression.Algorithm>
   :in-memory                1  ;; :in-memory <boolean>
   :max-versions             1  ;; :max-versions <int>
   :time-to-live             1} ;; :time-to-live <int>
  "This maps each get command to its number of arguments, for helping us
   partition the command sequence.")

(defn column-descriptor
  [family-name & args]
  (let [specs (partition-query args column-desc-argnums)
        ^HColumnDescriptor cd (HColumnDescriptor. (to-bytes family-name))]
    (doseq [spec specs]
      (condp = (first spec)
          :block-cache-enabled      (.setBlockCacheEnabled cd (second spec))
          :block-size               (.setBlocksize cd (second spec))
          :bloom-filter-type        (.setBloomFilterType cd (second spec))
          :compression-type         (.setCompressionType cd (second spec))
          :in-memory                (.setInMemory cd (second spec))
          :max-versions             (.setMaxVersions cd (second spec))
          :time-to-live             (.setTimeToLive cd (second spec))))
    cd))

;;
;; HTableDescriptor
;;
(defvar- table-desc-argnums
  {:max-file-size         1  ;; :max-file-size <long>
   :mem-store-flush-size  1  ;; :mem-store-flush-size <long>
   :read-only             1  ;; :read-only <boolean>
   :family                1} ;; :family <HColumnDescriptor>
  "This maps each get command to its number of arguments, for helping us
   partition the command sequence.")

(defn table-descriptor
  [table-name & args]
  (let [specs (partition-query args table-desc-argnums)
        td (HTableDescriptor. (to-bytes table-name))]
    (doseq [spec specs]
      (condp = (first spec)
          :max-file-size         (.setMaxFileSize td (second spec))
          :mem-store-flush-size  (.setMemStoreFlushSize td (second spec))
          :read-only             (.setReadOnly td (second spec))
          :family                (.addFamily td (second spec))))
    td))


;;
;; HBaseAdmin
;;

(defn add-column-family
  [table-name ^HColumnDescriptor column-descriptor]
  (.addColumn *admin* (to-bytes table-name) column-descriptor))

(defn hbase-available?
  []
  (HBaseAdmin/checkHBaseAvailable (HBaseConfiguration.)))

(defn compact
  [table-or-region-name]
  (.compact *admin* (to-bytes table-or-region-name)))

(defn create-table
  [table-descriptor]
  (.createTable *admin* table-descriptor))

(defn create-table-async
  [^HTableDescriptor table-descriptor split-keys]
  (.createTableAsync *admin* table-descriptor split-keys))

(defn delete-column-family
  [table-name column-name]
  (.deleteColumn *admin* (to-bytes table-name) (to-bytes column-name)))

(defn delete-table
  [table-name]
  (.deleteTable *admin* (to-bytes table-name)))

(defn disable-table
  [table-name]
  (.disableTable *admin* (to-bytes table-name)))

(defn enable-table
  [table-name]
  (.enableTable *admin* (to-bytes table-name)))

(defn flush
  [table-or-region-name]
  (.flush *admin* (to-bytes table-or-region-name)))

(defn cluster-status
  []
  (.getClusterStatus *admin*))

(defn get-connection
  []
  (.getConnection *admin*))

(defn get-master
  []
  (.getMaster *admin*))

(defn get-table-descriptor
  [table-name]
  (.getTableDescriptor *admin* (to-bytes table-name)))

(defn master-running?
  []
  (.isMasterRunning *admin*))

(defn table-available?
  [table-name]
  (.isTableAvailable *admin* (to-bytes table-name)))

(defn table-disabled?
  [table-name]
  (.isTableDisabled *admin* (to-bytes table-name)))

(defn table-enabled?
  [table-name]
  (.isTableEnabled *admin* (to-bytes table-name)))

(defn list-tables
  []
  (seq (.listTables *admin*)))

(defn major-compact
  [table-or-region-name]
  (.majorCompact *admin* (to-bytes table-or-region-name)))

(defn modify-column-family
  [table-name column-name ^HColumnDescriptor column-descriptor]
  (.modifyColumn *admin* (to-bytes table-name) (to-bytes column-name)
                 column-descriptor))

(defn modify-table
  [table-name table-descriptor]
  (.modifyTable *admin* (to-bytes table-name) table-descriptor))

(defn shutdown
  []
  (.shutdown *admin*))

(defn split
  [table-or-region-name]
  (.split *admin* (to-bytes table-or-region-name)))

(defn table-exists?
  [table-name]
  (.tableExists *admin* (to-bytes table-name)))
