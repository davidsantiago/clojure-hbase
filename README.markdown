# clojure-hbase

Clojure-HBase is a simple library for accessing HBase conveniently from Clojure. 

## Alternate Client API 

Compass labs has added an alternative client library interface based
heavily on the original implementation.  HBase administration
functions can still use the original clojure_hbase.admin.

Two main facilities were introduced, schemas and constraints.  Schemas
are a simple template for data encoding/decoding when processing HBase
operations.  Constraints result in method calls on Gets and Scans as
well as passing appropriate sets of filter objects to the Get/Scan
operation.  

    (require '[com.compass.hbase.client :as client])
    (require '[com.compass.hbase.filters :as f])

### Schemas

Define a schema for a table called users with two column families,
userinfo and friends. The first seq after the table name is metadata.
:default determines the default data type for qualifiers and values in
any column family not already defined in the schema.  The :row-type is
also defined.  The remainder of the definition consists of qualifier
and value types for each column family.

    (define-schema :users [:defaults [:string :json-key] 
    	       	           :row-type :long]
       :userinfo [:keyword :json-key] 
       :friends [:long :bool])

### Client API

Put then Get all values for row ID 100.  The Get procedure looks up a
schema in a global registry (configured by define-schema) for the
table named :users.  Gets and scans return a "family map" for each row
that consists of a dictionary of family names to maps where each
map consists of the keys and values for that family.

    (client/put :users 100 {:userinfo {:name "Test User" :id "21412"}})
    (client/get :users 100) => {:userinfo {:name "Test User" :id "21412"}}

Additional commands are straightforward

    (client/del :users 100) => fmap
    (client/get-multi :users [100 101 102]) => [fmap fmap fmap]
    (client/put-multi :users [[100 fmap] [200 fmap]]) 
    (client/scan (fn [id fmap] fmap) :users) => [fmap, fmap, ...]
    (client/do-scan (fn [id fmap] fmap) :users) => [fmap, fmap, ...]
    (client/raw-scan (fn [id fmap] fmap) :users) => [ResultSet, ...]

### Constraints 

The Get and Scan commands above all accept constraint objects which
are used to restrict the rows, families, qualifiers, values and
timestamps returned from a query.  The new API provides a relatively
primitive, but nicely composable mini-language for expressing these
constraints in terms of filters and return value restrictions.
Moreover, it is fairly easy to use constraints in map-reduce job
configuration also.  If you're interested in this in the context of
map-reduce, check out the discussion of [steps and flows in clojure-hadoop](http://ianeslick.com/higher-level-composition-in-clojure-hadoop-st).

Constraints are simply a clojure record that documents the various
constraints you've composed together.  When a Get or Scan command is
executed, the constraints are converted into the specific method calls
or filter objects necessary to satisfy them.

(f/constraints) will create an empty constraint object.

The Constraint protocol supports three methods: 

* (project type data)
* (filter type comparison value)
* (page size)

For example, to get users restricted to the :userinfo family

    (client/get :users &lt;id> (-> (f/constraints) 
                                (f/project :families [:userinfo])))

To return the userinfo data for all users with a name starting with
"a", the constraint expression is.

    (client/scan (fn [a b] b) :users
   	         (-> (f/constraints)
	             (f/project :families [:userinfo])
	             (f/filter :qualifier [:prefix :<] [:userinfo :name "b"]))

Similar to ClojureQL, constraints can be made and are not materialized until
the get or scan command is actually started, meaning we can store
constraints in vars or have functions that define a set of constraints
and then compose them later.  There are also two convenience functions for
composing these higher order constraint expressions.

    (make-constraints expr1 expr2 ...) and 
    (add-constraints constraints expr1 expr2 ...)

So we can now easily define appropriate variables and functions

    (def userinfo (make-constraints
                    (f/project :families [:userinfo])))

    (defn filter-user-name-prefix [c comp prefix]
      (add-constraints c (f/filter :qualifier [:prefix comp] [:userinfo :name prefix])))

And then apply them interactively or programmatically to perform scans.

    (client/scan (fn [a b] b) :users (filter-user-name-prefix userinfo :< "b"))

The currently support projection types include:

* :families - Restrict results to one or more families
* :columns - Restrict row results to a matching family + qualifier
* :row-range - Restrict scan to a range of row values (f/project :row-range [low high])
* :timestamp - Only return values for the given long timestamp
* :timerange - Return values for the given low / high timestamps
* :max-versions - The maximum number of versions of any qualifier+value to return

It is fairly trivial to add new projections or filters; please feel
free to send patches.

Two utility functions make dealing with time ranges easier, (timestamp
ref), (timestamp-now) and (timestamp-ago reference type amount).
timestamp-ago takes a reference timestamp and returns a long value
according to type {:minutes | :hours | :days} and a number.  Arguments
to timestamp and timerange use the timestamp function to interpret
arguments.  This makes it easy then to say things like:

Scan from two days ago until now:

     (f/project constraints :timerange [[:days 2] :now])

Or from 1 month before ref, a long-valued reference timestamp.

     (f/project constraints :timerange [[ref :months 1] ref])

Filter expressions all include a comparison expression.  Typically
you'll use :=, but you can use a variety of comparison types {:binary
| :prefix | :substr | :regex } and the usual boolean comparitors.

Beware that filters don't limit the scan row, so a row filter will
test every row and only return those that pass the test, but if you're
doing a scan operation, this will touch every row in the table which
can take quite a bit of time.

Filter types include:

* (f/filter :row &lt;compare> &lt;value>) - Filter rows by value comparison
* (f/filter :qualifier &lt;compare> [&lt;family> &lt;name>]) - Passes all qualifier names in the given family where (&lt;compare> qualifier &lt;name>) is true
* (f/filter :column &lt;compare> [&lt;family> &lt;qualifier> &lt;value>]) - Pass all columns where the value comparison is true
* (f/filter :cell &lt;compare> [&lt;value> &lt;type>]) - Pass all qualifier-value pairs where the value matches &lt;value>.
* (f/filter :keys-only &lt;ignored>) - Only return the qualifiers, no values
* (f/filter :first-kv-only &lt;ignored> - Only return the first qualifier-value pair (good for getting matching rows without returning much data
* (f/filter :limit &lt;size>) - Only return &lt;size> rows using PageFilter.

There are some compositional semantics missing, such as ignoring rows
where certain columns don't match, rather than filtering just
key-value pairs.  This will be addressed in a later revision.

## Original API Usage

HBase supports four main operations: Get, Put, Delete, and Scan. The API is 
based around creating objects of the same name, and then submitting those to 
the HTable representing a given table in the database. Clojure-HBase is 
intended to provide a convenient API for creating these objects and then 
submitting them for you. Here's an example session: 

      (require ['com.davidsantiago.clojure-hbase :as 'hb])
 
      (hb/with-table [users (hb/table "test-users")]
		     (hb/put users "testrow" :values [:account [:c1 "test" :c2 "test2"]]))
      nil

      (hb/with-table [users (hb/table "test-users")]
		     (hb/get users "testrow" :columns [:account [:c1 :c2]]))
      #<Result keyvalues={testrow/account:c1/1265871284243/Put/vlen=4, testrow/account:c2/1265871284243/Put/vlen=5}>

      (hb/with-table [users (hb/table "test-users")]
		     (hb/delete users "testrow" :columns [:account [:c1 :c2]]))
      nil

      (hb/with-table [users (hb/table "test-users")]
		     (hb/get users "testrow" :columns [:account [:c1 :c2]]))
      #<Result keyvalues=NONE>

Creating an HTable object is potentially an expensive operation in HBase, 
so HBase provides the class HTablePool, which keeps track of already created
HTables and lets them be reused. Clojure-HBase transparently uses an 
HTablePool to manage tables for you. It's not strictly necessary, but 
surrounding your calls with the with-table statement will ensure that any 
tables requested in the bindings are returned to the HTablePool at the end of
the code. This can be manually managed with the table and release-table 
functions. Perhaps a better way to write the above code would have been:

      (hb/with-table [users (hb/table "test-users")]
         (hb/put users "testrow" :values [:account [:c1 "test" :c2 "test2"]])
         (hb/get users "testrow" :columns [:account [:c1 :c2]])
         (hb/delete users "testrow" :columns [:account [:c1 :c2]])
         (hb/get users "testrow" :columns [:account [:c1 :c2]]))
      #<Result keyvalues=NONE>

The get, put, and delete functions will take any number of arguments after
the table and row arguments. Options to the function are keywords followed by
0 or more arguments, depending on the function. The arguments can be pretty 
much anything that can be turned into a byte array (see below). For now, see 
the source code for the list of options.

HBase identifies data by rows, which have column-families, which contain 
columns. In general, when specifying a column, you need to give a row, a 
column-family, and a column. When operating on multiple columns within a
family, you can specify the column-family and then any number of columns 
in a vector, as above. 

It may sometimes be useful to have access to the raw HBase Get/Put/Delete
objects, perhaps for interoperability with another library. The functions
get\*, put\*, scan\* and delete\* will return those objects without submitting 
them:

      (hb/get* "testrow" :column [:account :c1]))
			#<Get row=testrow, maxVersions=1, timeRange=[0,9223372036854775807), families={(family=account, columns={c1}}>

Note that the table argument is not necessary here. Such objects can be 
submitted for processing to an HTable using the functions query (for Get and
Scan objects) or modify (for Put and Delete objects):

      (hb/with-table [users (hb/table "test-users")]
        (hb/modify users 
          (hb/put* "testrow" :value [:account :c1 "test"])))
      nil

Alternatively, you may have already-created versions of these objects from
existing code that you would then like to augment from Clojure. The functions
support a :use-existing option that lets you pass in an existing version of
the expected object and performs all its operations on that instead of 
creating a new one.

HBase only thinks of byte arrays; this includes column-family, columns, and
values. This means that any object you can serialize to a byte array can be
used for any of these. You don't have to do any of this manually (though you
can if you want, byte-arrays are perfectly acceptable arguments). In all the
code above, keywords and strings are used interchangeably, and several other
types can be used as well. You can also allow more types to be used for this 
purpose by adding your own method for the to-bytes-impl multimethod. Remember, 
though, HBase is basically untyped. We can make it easy to put stuff in, but
you have to remember what everything was and convert it back yourself.

Scan objects are a bit different from the other 3. They are created similarly,
but they will return a ResultScanner that lets you iterate through the scan
results. Since ResultScanner implements Iterable, you should able to use it
places where Clojure expects one (ie, seq). ResultScanners should be 
.close()'d when they are no longer needed; by using the with-scanner macro
you can ensure that this is done automatically.

The Result objects that come out of get and scan requests are not always the
most convenient to work with. If you'd prefer to deal with the result as a
set of hierarchical maps, you can use the as-map function to create a map out
of the result. For example: 

      (hb/with-table [users (hb/table "test-users")]
						     (hb/get users "testrow" :column [:account :c1]))
			#<Result keyvalues={testrow/account:c1/1266054048251/Put/vlen=4}>
			
can become:

      (hb/with-table [users (hb/table "test-users")]
						     (hb/as-map (hb/get users "testrow" :column [:account :c1])))
			{#<byte[] [B@54231c3> {#<byte[] [B@3cd0fbe7> {1266054048251 #<byte[] [B@3c4a19e2>}}}
			
The Clojure function get-in can be very useful for pulling what you want out
of this structure. We can do even better, using the options to as-map, which 
let you specify a function to map onto each family, qualifier, timestamp, or 
value as you wish.

      (hb/with-table [users (hb/table "test-users")]
						     (hb/as-map (hb/get users "testrow" :column [:account :c1]) :map-family #(keyword (Bytes/toString %)) :map-qualifier #(keyword (Bytes/toString %)) :map-timestamp #(java.util.Date. %) :map-value #(Bytes/toString %) str))
			{:account {:c1 {#<Date Sat Feb 13 03:40:48 CST 2010> "test"}}}

Depending on your use case, you may prefer to have all of the values in the 
Result as a series of [family qualifier timestamp value] vectors. The function
as-vector accepts the same arguments and returns a vector, each value of which
is a vector of the form just mentioned: 

      (hb/with-table [users (hb/table "test-users")]
			           (hb/as-vector (hb/get users "testrow" :column [:account :c1]) :map-family #(keyword (Bytes/toString %)) :map-qualifier #(keyword (Bytes/toString %)) :map-timestamp #(java.util.Date. %) :map-value #(Bytes/toString %) str))
      [[:account :c1 #<Date Sat Feb 13 03:40:48 CST 2010> "test"]]

## Status

Basic unit tests passing. No known bugs. Bug reports and input welcome.

## Lately...

- Added some utility functions for converting hbase output back into usable objects.
- Added multimethods to to-bytes so that lists, maps, and vector can be easily inserted
  into and converted back from hbase.
- Added the :map-default keyword option for as-map, latest-as-map, and as-vector. Makes it
  easier to give those options; the more specific keywords override the default.
- Added unit tests for new utility functions.

Added basic unit tests.
Added a first cut at most of the Admin functions.

## License

Eclipse Public License