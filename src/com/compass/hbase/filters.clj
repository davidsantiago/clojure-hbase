(ns com.compass.hbase.filters
  (:refer-clojure :exclude [filter])
  (:use clojure.contrib.def
	com.compass.hbase.schema)
  (:require [clj-time.core :as time])
  (:import
   [org.apache.hadoop.hbase.client Get Scan]
   [org.apache.hadoop.hbase.filter
    ;; Base classes
    Filter 
    CompareFilter
    CompareFilter$CompareOp
    ;; Comparators
    BinaryComparator
    BinaryPrefixComparator
    RegexStringComparator
    SubstringComparator
    ;; Filters
    ColumnCountGetFilter
    ColumnPaginationFilter
    ColumnPrefixFilter
    DependentColumnFilter
    FamilyFilter
    FilterList
    FilterList$Operator
    FirstKeyOnlyFilter
    InclusiveStopFilter
    KeyOnlyFilter
    PageFilter
    PrefixFilter
    QualifierFilter
    RowFilter
    SingleColumnValueExcludeFilter
    SingleColumnValueFilter
    SkipFilter
    TimestampsFilter
    ValueFilter
    WhileMatchFilter]))

;;
;; Provide a generic framework for defining Get, Scan and Map-Reduce
;; constraints over HBase data.
;;
;; Intended to capture server side constraints in a single filter object
;;
	     
(defprotocol ConstraintSet
  "Manipulate a set of HBase constraints for a Get,Scan or M-R interface"
  (filter [this type compare value-spec] [this type arg]
	  "Filter out rows or columns based on contents")
  (page [this size]
	"Returns a set of results of size 'size'")
  (project [this type values]
	  "Restrict the nature of a given row result"))

(def *project-specifiers*
     #{:families :row-range :columns :timestamp :timerange :max-versions})

(defrecord HBaseConstraints [projections filters page all-versions]
  ConstraintSet
  (project [#^HBaseConstraints this specifier values]
	  (if (= specifier :all-versions)
	    (assoc this :all-versions true)
	    (do (assert (contains? *project-specifiers* specifier))
		(assoc-in this [:projections specifier] values))))
  (filter [#^HBaseConstraints this type compare value-spec] 
	  (assert (not (get-in this [:filters type])))
	  (assoc-in this [:filters type] [compare value-spec]))
  (filter [#^HBaseConstraints this type arg]
	  (assoc-in this [:filters type] arg))
  (page [#^HBaseConstraints this size]
	(assoc this :page size)))

(defmethod print-method HBaseConstraints [this writer]
	   (.write writer (format "#<Constraints %d %d>"
				  (count (:projections this))
				  (count (:filters this)))))

(defn constraints []
  (HBaseConstraints. nil nil nil nil))

;;
;; Apply constraints
;;

(defn timestamp-now []
  (.getMillis (time/now)))

(defn timestamp-ago 
  "Return a Unix timestamp from a reference time object
   minus a number of :hours, :minutes or :days"
  ([reference type amount]
     (assert (keyword? type))
     (.getMillis
      (cond (= type :hours)
	    (time/minus reference (time/hours amount))
	    (= type :minutes)
	    (time/minus reference (time/minutes amount))
	    (= type :days)
	    (time/minus reference (time/days amount))
	    true
	    (throw (java.lang.Error. "Unrecognized argument to timestamp-ago")))))
  ([type amount]
     (timestamp-ago (time/now) type amount)))

(defn timestamp [reference]
  (cond (number? reference) (long reference)
	(= reference :now) (timestamp-now)
	(sequential? reference) (apply timestamp-ago reference)))

;; Selecting result subsets

(defmulti apply-project (fn [op schema [select values]] select))

(defmethod apply-project :families
  [op schema [select values]]
  (doseq [family values]
    (.addFamily op (encode-family schema family))))

(defmethod apply-project :columns
  [op schema [select values]]
  (doseq [spec values]
    (if (sequential? spec)
      (.addColumn op
		  (encode-family schema (first spec))
		  (encode-column schema (first spec) (second spec)))
      (.addColumn op
		  (encode-column schema nil spec)))))

(defmethod apply-project :row-range
  [op schema [select [min-value max-value]]]
  (doto op
    (.setStartRow (encode-row schema min-value))
    (.setStopRow (encode-row schema max-value))))

(defmethod apply-project :timestamp
  [op schema [select value]]
  (.setTimeStamp op (timestamp value)))

(defmethod apply-project :timerange
  [op schema [select [start end]]]
  (.setTimeRange op (timestamp start) (timestamp end)))

(defmethod apply-project :max-versions
  [op schema [select values]]
  (.setMaxVersions op (int values)))

;;
;; Create filters and comparators to filter
;; rows based on columns, values, etc.
;;
;;
;; FILTERS:     [ELEMENT COMPARITOR VALUE-SPEC] | [ELEMENT VALUE-SPEC]
;; ELEMENT:     :row | :qualifier | :column | :cell
;; COMPARITOR:  [COMP-TYPE COMPARISON] | Comparison
;; COMPARISON:  =,>,<,<=,>=,not= 
;; COMP-TYPE:    :binary | :prefix | :regex | :substr
;; VALUE-SPEC:  <value> | [<family> <value>] | [<family> <qualifier> <value>]

(comment
  ;; Get All rows with ID > 2 with :text qualifiers and message usernames
  ;; starting in IE, return only :message and :test families where :message
  ;; families have :text2 or :text fields present
  (-> (constraints)
      (filter :row [:binary >] 2)
      (filter :qualifier := [:messages :content])
      (filter :column [:prefix :=] [:message :username "ie"])
      (project :families [:message :test])
      (project :columns [[:message :text2] [:message :text]])))

(defn lookup-compare [sym]
  (let [sym (if (vector? sym) (second sym) sym)]
    (case sym
	  (:= = '=) CompareFilter$CompareOp/EQUAL
	  (:> > '>) CompareFilter$CompareOp/GREATER
	  (:>= >= '>=) CompareFilter$CompareOp/GREATER_OR_EQUAL
	  (:<= <= '<=) CompareFilter$CompareOp/LESS_OR_EQUAL
	  (:< < '<) CompareFilter$CompareOp/LESS
	  (:not= not= 'not=) CompareFilter$CompareOp/NOT_EQUAL)))

(defn as-comparator [compare value]
  (if (vector? compare)
    (case (first compare)
	  :binary (BinaryComparator. value)
	  :prefix (BinaryPrefixComparator. value)
	  :regex (RegexStringComparator. value)
	  :substr (SubstringComparator. value))
    (BinaryComparator. value)))

(defmulti make-filter (fn [schema [type & args]] type))

(defmethod make-filter :default
  [schema [type & rest]]
  (println "Unrecognized filter option " type)
  (throw (clojure.contrib.condition.Condition. "Bad option")))

(defmethod make-filter :row
  [schema [type [compare row-value]]]
  (RowFilter. (lookup-compare compare)
	      (as-comparator compare (encode-row schema row-value))))

(defmethod make-filter :row-range
  [schema [type [min-value max-value]]]
  (FilterList. FilterList$Operator/MUST_PASS_ALL
	       [(RowFilter. (lookup-compare :>)
			    (as-comparator :> (encode-row schema min-value)))
		(RowFilter. (lookup-compare :<)
			    (as-comparator :< (encode-row schema max-value)))]))

(defmethod make-filter :qualifier
  [schema [type [compare [family qual-value]]]]
  (QualifierFilter. (lookup-compare compare)
		    (as-comparator compare (encode-column schema family qual-value))))
	   
(defmethod make-filter :column
  [schema [type [compare [family qualifier value]]]]
  (doto (SingleColumnValueFilter.
	 (encode-family schema family)
	 (encode-column schema family qualifier)
	 (lookup-compare compare)
	 (as-comparator compare (encode-cell schema family qualifier value)))
    (.setFilterIfMissing true)))
			     
(defmethod make-filter :cell
  [schema [type [compare [value encoding]]]]
  (ValueFilter. (lookup-compare compare)
		(as-comparator compare value))) ;;(encode-value value encoding))))

(defmethod make-filter :keys-only
  [schema [type [rest]]]
  (KeyOnlyFilter.))

(defmethod make-filter :first-kv-only
  [schema [type empty]]
  (FirstKeyOnlyFilter.))

(defmethod make-filter :limit
  [schema [type size]]
  (PageFilter. (long size)))

;;
;; Turn constraint specs into HBase objects
;;

(defn filter-list [list]
  (FilterList. FilterList$Operator/MUST_PASS_ALL list))

(defn constrain-op [op schema #^HBaseConstraints c]
  (let [flist (map (partial make-filter schema)
		   (:filters c))]
    (assert (every? #(not (nil? %)) flist))
    (.setFilter op (filter-list flist))
    (doall
     (map (partial apply-project op schema)
	  (:projections c)))
    op))

 
 