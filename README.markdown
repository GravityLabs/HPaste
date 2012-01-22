# Welcome to HPaste!

HPaste unlocks the rich functionality of HBase for a Scala audience. In so doing, it attempts to achieve the following goals:

* Provide a strong, clear syntax for querying and filtration
* Perform as fast as possible -- the abstractions should not show up in a profiler, just in your code!
* Re-articulate HBase's data structures rather than force it into an ORM-style atmosphere.
* A rich set of base classes for writing MapReduce jobs in hadoop against HBase tables.
* Provide a maximum amount of code re-use between general Hbase client usage, and operation from within a MapReduce job.
* Use Scala's type system to its advantage--the compiler should verify the integrity of the schema.
* Be a verbose DSL--minimize boilerplate code, but be human readable!

Here's some quick code examples to give you a sense of what you're getting into:

#### Creating a WebTable
The classic case for HBase and BigTable is crawling and storing web pages.  You need to define a WebTable for your crawling.  The below defines a table called WebTable, with a String key.

* It has a column family called "meta", that holds columns with String keys and Any value-type.
* It then specifies that the "meta" family holds a "title" column, a "lastCrawled" column, and a "url" column.
* A second column family holds content and has the compressed flag to true.
* It has a family "content" for storing content.  The "article" column is for storing the main page content.
* The Attributes column is a map where you can atomically store values keyed by a string.

```scala
object ExampleSchema extends Schema {
	
  implicit val conf = ClusterTest.htest.getConfiguration //Replace with your own configuration instance


  class WebTable extends HbaseTable[WebTable, String, WebPageRow](tableName="pages",rowKeyClass=classOf[PageUrl]) {
    def rowBuilder(result:DeserializedResult) = new WebPageRow(this,result)

    val meta = family[String, String, Any]("meta")
    val title = column(meta,"title",classOf[String])
    val lastCrawled = column(meta,"lastCrawled",classOf[DateTime])
    val url = column(meta,"url",classOf[String])

    val content = family[String,String,Any]("text",compressed=true)
    val article = column(content,"article",classOf[String])
    val attributes = column(content,"attrs",classOf[Map[String,String]])


  }
  class WebPageRow(table:WebTable,result:DeserializedResult) extends HRow[WebTable,String](result,table)
  val WebTable = table(new WebTable)
}

```

#### Putting values into the WebTable

Now, suppose you're crawling a website.  The below will create a row with the values specified.  When you call value(), the first argument is a function that points to the column you specified in the above WebTable schema.  This dips into DSL-land.

```scala
ExampleSchema.WebTable
            .put("http://mycrawledsite.com/crawledpage.html")
            .value(_.title, "My Crawled Page Title")
            .value(_.lastCrawled, new DateTime())
            .value(_.article, "Jonsie went to the store.  She didn't notice the spinning of the Earth, nor did the Earth notice the expansion of the Universe.")
            .value(_.attributes, Map("foo" -> "bar", "custom" -> "data"))
            .execute()

```

#### Querying values out of the WebTable

Let's get the above page out of the WebTable.  Let's say we just want the title of the page and when it was last crawled.  The withColumns() call tells HBase to only fetch those columns.  It takes a series of functions that return the column values you specified in the WebTable, so you get compile-time checking on that.  

```scala
ExampleSchema.WebTable.query2.withKey("http://mycrawledsite.com/crawledpage.html")
            .withColumns(_.title, _.lastCrawled).singleOption() match {
      case Some(pageRow) => {
        val title = pageRow.column(_.title).getOrElse("No Title")
        val crawledDate = pageRow.column(_.lastCrawled).getOrElse(new DateTime())
        //Do something with title and crawled date...
      }
      case None => {
        println("Row not found")
      }
    }
```

The result you get back is an instance of the row class you specified against the WebTable: WebPageRow.  When you get a WebPageRow back from a query, a scan, or a map reduce job, you can fetch the columns out via the column() call.  


# Building HPaste
This project is put together with Maven.  In theory you should be able to build and run the project's tests via:

```
mvn test
```

Perhaps the best way to get started is to look at the unit tests in the SchemaTest.scala file.  These unit tests show how to create a schema, extend serialization with your own objects, and utilize the schema.  They will, when run, fire up a mini instance of HBase.  It's best to at least look at those tests before you go through the below examples, because the tests show everything in context.

## Defining a Schema

Following the HBase structure, first you define a Schema, then Tables, then Column Families, then (optionally) Columns.  Below is an example schema that contains a single Table definition.  The table will be called "schema_example" in HBase.  It will expect its row keys to be Strings (the second type parameter to HbaseTable).

```scala
  object ExampleSchema extends Schema {

    //There should only be one HBaseConfiguration object per process.  You'll probably want to manage that
    //instance yourself, so this library expects a reference to that instance.  It's implicitly injected into
    //the code, so the most convenient place to put it is right after you declare your Schema.
    implicit val conf = htest.getConfiguration

    //A table definition, where the row keys are Strings
    class ExampleTable extends HbaseTable[ExampleTable,String](tableName = "schema_example")
    {
      //Column family definition
      val meta = family[String, String, Any]("meta")
      //Inside meta, assume a column called title whose value is a string
      val title = column(meta, "title", classOf[String])
      //Inside meta, assume a column called url whose value is a string
      val url = column(meta, "url", classOf[String])
      //Inside meta, assume a column called views whose value is a string
      val views = column(meta, "views", classOf[Long])
      //A column called date whose value is a Joda DateTime
      val creationDate = column(meta, "date", classOf[DateTime])

      //A column called viewsArr whose value is a sequence of strings
      val viewsArr = column(meta,"viewsArr", classOf[Seq[String]])
      //A column called viewsMap whose value is a map of String to Long
      val viewsMap = column(meta,"viewsMap", classOf[mutable.Map[String,Long]])

      //A column family called views whose column names are Strings and values are Longs.  Can be treated as a Map
      val viewCounts = family[String, String, Long]("views")

      //A column family called views whose column names are YearDay instances and whose values are Longs
      val viewCountsByDay = family[String, YearDay, Long]("viewsByDay")

      //A column family called kittens whose column values are the custom Kitten type
      val kittens = family[String,String,Kitten]("kittens")
    }

    //Register the table (DON'T FORGET TO DO THIS :) )
    val ExampleTable = table(new ExampleTable)

  }
```

The above import statement is very important: it brings in all of HPaste's default serializers, which transparently convert types to byte arrays.

The above table has a column family called meta, and several strongly-typed columns in the "meta" family.  It then creates several column families that do not have column definitions under them ("views" and "viewsByDay").  The reason for this is that in HBase you tend to create two different types of column families: in one scenario, you create a column family that contains a set of columns that resemble columns in an RDBMS: each column is "typed" and has a unique identity.  In the second scenario, you create a column family that resembles a Map: you dynamically add key-value pairs to this family.  In this second scenario, you don't know what the columns are ahead of time--you'll be adding and removing columns on the fly.  HPaste supports both models.

## Lifecycle Management

Because HBase prefers to have a single instance of the Configuration object for connection management purposes, gravity-hbase-schema does not manage any Configuration lifecycle.  When you specify your schema, you need to have an implicit instance of Configuration in scope.

## Table Creation
If you have an existing table in HBase with the same name and families, you can get started.  If you don't, you can now call:
```scala
    val create = ExampleSchema.ExampleTable.createScript()
    println(create)
```
Paste the results into your hbase shell to create the table.

## Data Manipulation
Now let's add some data to the table.  HPaste supports GETS, PUTS, and INCREMENTS.  You can batch multiple operations together, or do them serially.

### Column Valued Operations
Column valued operations are ops that work against a particular column.  Rather like a RDBMS.

The following example chains a series of operations together.  We will put a title against Chris, a title against Joe, and increment Chris' "views" column by 10.  If the columns do not exist, they will be lazily created as per HBase convention.

```scala
    ExampleSchema.ExampleTable
            .put("Chris").value(_.title, "My Life, My Times")
            .put("Joe").value(_.title, "Joe's Life and Times")
            .increment("Chris").value(_.views, 10l)
            .execute()
```

When you call execute(), the operations you chained together will be executed in the following order: DELETES, then PUTS, then INCREMENTS.  We decided to order the operations in that way because that is how they will generally be ordered in normal use cases, and also because HBase cannot guarantee ordering in batch-operations in which increments are mixed in with deletes and puts.

If you need to enforce your own ordering, you should break your statements apart by calling execute() when you want to flush operations to HBase.

### Column Family Operations

Often times you will define a column family where the columns themselves hold data (as columns are dynamically created in HBase).

Here's the "views" column we specified in the example schema.  The type parameters tell the system that we expect this column family to have a string-valued family name, a string-valued column name, and a Long value.

```scala
      val viewCounts = family[String, String, Long]("views")
```

Because Column Families treated in this fashion resemble the Map construct most closely, that's what we support. The following will put two columns titled "Today" and "Yesterday" into the Example Table under the key 1346:

```scala
    ExampleSchema.ExampleTable.put(1346l).valueMap(_.viewCounts, Map("Today" -> 61l, "Yesterday" -> 86l)).execute
```

### Serialization: Creating your own types

For serialization, HPaste uses Scala's implicit parameter system, which is very handy for creating an extensible conversion library.  All of HPaste's built-in serializers are contained in a package object called "schema".  Here's the serializer for Strings:

```scala
  implicit object StringConverter extends ByteConverter[String] {
    override def toBytes(t: String) = Bytes.toBytes(t)
    override def fromBytes(bytes: Array[Byte]) = Bytes.toString(bytes)
  }
```

To create your own serializer, follow the above pattern.  Implement a ByteConverter of the type you want, and declare it as an implicit object.  Any time you use a type that requires that serializer, make sure the namespace is in scope (for example, HPaste wants you to have "import com.gravity.hbase.schema._" at the top of your client code, so that all the standard serializers get imported).

#### Complex Type Serialization : Strings (slow, big, but readable)
If you want to create your own custom type, the easiest way to do so is to make it a string value under the covers.  For example, there's a built in type in HPaste called YearDay:

```scala
case class YearDay(year: Int, day: Int)
```

The serializer for it is located in the "schema" package object.  All it does is convert the YearDay object to a string, then serialize out the String as bytes using the Hbase Bytes helper object.

```scala
  implicit object YearDayConverter extends ByteConverter[YearDay] {
    val SPLITTER = "_".r
    override def toBytes(t:YearDay) = Bytes.toBytes(t.year.toString + "_" + t.day.toString)
    override def fromBytes(bytes:Array[Byte]) = {
      val strRep = Bytes.toString(bytes)
      val strRepSpl = SPLITTER.split(strRep)
      val year = strRepSpl(0).toInt
      val day = strRepSpl(1).toInt
      YearDay(year,day)
    }
  }
```
This means that YearDay objects will be human readable when you look at them in HBase, and also it means it's mentally easier to reason about the layout of the object when you're extending it.

That having been said, string-based serialization is the lowest common denominator when it comes to speed and size of data.  The next section describes custom byte serialization.

#### Complex Type Serialization : Byte Arrays (fast, small, unreadable)
Because ByteConverter (and by proxy HBase) just needs byte arrays, you can extend the class with any serializer framework you want (Thrift, Avro, etc.).

HPaste does have some extra support for the simplest, most performant, and most dangerous Java serialization framework: DataInputStream and DataOutputStream.  To use this, you can inherit from ComplexByteConverter.  This will provide a DataInputStream when bytes need to be serialized to your type, and a DataOutputStream when it's time for your object to become a byte array.

Let's say you have a class Kitten:

```scala
case class Kitten(name:String, age:Int, height:Double)
```

The following defines a serializer for Kitten:

```scala
  implicit object KittenConverter extends ComplexByteConverter[Kitten] {
    override def write(kitten:Kitten, output:DataOutputStream)  {
      output.writeUTF(kitten.name)
      output.writeInt(kitten.age)
      output.writeDouble(kitten.height)
    }

    override def read(input:DataInputStream) = {
      Kitten(input.readUTF(), input.readInt(), input.readDouble())
    }
  }
```

WARNING: There are a lot of production issues with using raw serialization this way.  They mainly revolve around versioning.  What if you want to change the layout of Kitten later? The above method of serializing uses no metadata. The bytestream is just a bunch of undifferentiated primitives. If you, for example, add a new Int value before the Name, the deserializer will, when it encounters older objects, read the first bytes of the Name as the Int. In that situation you'll need to write a tool that massages the old Kitten byte-layout into the new Kitten byte-layout.  Better serialization frameworks do a better job of handling this situation.  HPaste will soon support Avro out of the box.  That having been said, as long as you're mindful about what you're doing, DataInput and DataOutput are blazingly fast and leave a tiny data footprint--if you confine yourself to relative immutable type definitions, you can go far with this model.

### Sequences, Maps, and Sets

Once you have a serializer for a type, you can (fairly) easily define a serializer that lets you define a Set, Map, or Seq of that object.  The below assumes you already have a ByteConverter for a Kitten object, and will now support column values that are sequences.

```
  implicit object KittenSeqConverter extends SeqConverter[Kitten,Seq[Kitten]]
```

NOTE: The above way of defining a sequence is clumsy, agreed.  Need to refactor.


### Mixing Modification Operations

You can mix column-valued and column family operations into the same call.

```scala
    ExampleSchema.ExampleTable
            .put("Chris").value(_.title, "My Life, My Times")
            .put("Joe").value(_.title, "Joe's Life and Times")
            .put("Fred").value(_.viewsArr,Seq("Chris","Bissell"))
            .increment("Chris").value(_.views, 10l)
            .put("Chris").valueMap(_.viewCountsByDay, Map(YearDay(2011,16)->60l, YearDay(2011,17)->50l))
            .execute()
```

# Data Retrieval


1. Creating a specification for your Get operation, in which you decide what rows to get back, what columns and column families to get back.

2. When you issue the retrieval operation and get the results back, extracting the results into useful data.

Assuming the examples under the DATA MANIPULATION section, the following will retrieve a Map of String to Long out of the Example Table:

```scala
    val dayViewsRes = ExampleSchema.ExampleTable.query.withKey(key).withColumnFamily(_.viewCountsByDay).single()
    val dayViewsMap = dayViewsRes.family(_.viewCountsByDay)
```


# MapReduce Operation Support

## Jobs vs Tasks

A job is represented by an HJob.  A job is composed of multiple tasks, which are represented as HTasks.  HTasks can have dependencies, which are woven together by the HJob they're a part of.

## Row Serialization

HRows can be serialized between Mappers and Reducers, using the writeRow and readRow methods of PrimitiveOutputStream and PrimitiveInputStream.
This is often the least performant way of passing data between mappers and reducers, but also involves the least amount of code.


# Developers

This library was developed by [Chris Bissell](https://github.com/Lemmsjid "Lemmsjid"), with many contributions, both philosophical and code, by [Robbie Coleman](https://github.com/erraggy "erraggy").