# clojure-hbase

Clojure-HBase is a simple library for accessing HBase conveniently from Clojure. 

## Usage

HBase supports four main operations: Get, Put, Delete, and Scan. The API is 
based around creating objects of the same name, and then submitting those to 
the HTable representing a given table in the database. Clojure-HBase is 
intended to provide a convenient API for creating these objects and then 
submitting them for you. Here's an example session: 

      (require ['com.davidsantiago.clojure-hbase :as 'hb])
 
      (hb/with-table [users (hb/table "test-users")]
		     (hb/put! users "testrow" :values [:account [:c1 "test" :c2 "test2"]]))
      nil

      (hb/with-table [users (hb/table "test-users")]
		     (hb/get! users "testrow" :columns [:account [:c1 :c2]]))
      #<Result keyvalues={testrow/account:c1/1265871284243/Put/vlen=4, testrow/account:c2/1265871284243/Put/vlen=5}>

      (hb/with-table [users (hb/table "test-users")]
		     (hb/delete! users "testrow" :columns [:account [:c1 :c2]]))
      nil

      (hb/with-table [users (hb/table "test-users")]
		     (hb/get! users "testrow" :columns [:account [:c1 :c2]]))
      #<Result keyvalues=NONE>

Creating an HTable object is potentially an expensive operation in HBase, 
so HBase provides the class HTablePool, which keeps track of already created
HTables and lets them be reused. Clojure-HBase transparently uses an 
HTablePool to manage tables for you. It's not strictly necessary, but 
surrounding your calls with the with-table statement will ensure that any 
tables requested in the bindings are returned to the HTablePool at the end of
the code. This can be manually managed with the table and release-table 
functions. Of course, a better way to write the above code would have been:

      (hb/with-table [users (hb/table "test-users")]
         (hb/put! users "testrow" :values [:account [:c1 "test" :c2 "test2"]])
         (hb/get! users "testrow" :columns [:account [:c1 :c2]])
         (hb/delete! users "testrow" :columns [:account [:c1 :c2]])
         (hb/get! users "testrow" :columns [:account [:c1 :c2]]))
      #<Result keyvalues=NONE>

The get!, put!, and delete! functions will take any number of arguments after
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
get, put, scan and delete will return those objects without submitting them:

      (hb/get "testrow" :column [:account :c1]))
			#<Get row=testrow, maxVersions=1, timeRange=[0,9223372036854775807), families={(family=account, columns={c1}}>

Note that the table argument is not necessary here. Such objects can be 
submitted for processing to an HTable using the functions query (for Get and
Scan objects) or modify (for Put and Delete objects):

      (hb/with-table [users (hb/table "test-users")]
        (hb/modify users 
          (hb/put "testrow" :value [:account :c1 "test"])))
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
places where Clojure expects a seq (not yet tested).

## Status

Very immature, barely tested. Bug reports and input welcome.

## Upcoming

* Nice wrapper for Result objects, so they can be treated like maps.
* Some sort of unit test situation.

## License

Eclipse Public License