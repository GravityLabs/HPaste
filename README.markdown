# Welcome to HPaste!

### What is HPaste?

HPaste unlocks the rich functionality of HBase for a Scala audience. In so doing, it attempts to achieve the following goals:

* Provide a strong, clear syntax for querying and filtration
* Perform as fast as possible while maintaining idiomatic Scala client code -- the abstractions should not show up in a profiler!
* Re-articulate HBase's data structures rather than force it into an ORM-style atmosphere.
* A rich set of base classes for writing MapReduce jobs in hadoop against HBase tables.
* Provide a maximum amount of code re-use between general Hbase client usage, and operation from within a MapReduce job.
* Use Scala's type system to its advantage--the compiler should verify the integrity of the schema.
* Be a verbose DSL--minimize boilerplate code, but be human readable!

### What isn't HPaste?

You'll notice that HPaste has a lot of convenience classes for MapReduce jobs.  This is to make it painless to use your domain objects and tables in the context of MR jobs. HPaste has no aspirations to replace tuple-based frameworks like Pig or Cascading (both of which we use for complex log parsing).  HPaste is intended to hug the Hadoop MapReduce API very closely, building convenience functions where necessary, without abstracting too far away from the base concept.  

The goal of HPaste's MapReduce support is to allow you to build rich functionality into your Table and Row objects, and make it be painless to have those tables and rows participate in MapReduce jobs.  Oftentimes in HBase you have a combination of OLTP style operations (client gets data, client serves data), and OLAP style operations (pivot one table around a particular piece of data, and output to another table).  That is where HPaste comes in handy, because there is often an impedance in Pig and/or Cascading between HBase-friendly binary data serialized objects and the tuple framework that makes those libraries so awesome to use for ad-hoc log-style data.

(It is a mini-goal of HPaste to integrate into Cascading's tuple framework.)

## Project Status
This project is currently actively developed and maintained.  It is used in a large production codebase in high-throughput, memory-intensive scenarios, and has many months of bug fixes under its belt.  Because it already has a great deal of code utilizing it, there will not be many breaking changes to the API.  Instead what we usually do is provide an upgraded API that sits next to the old API, then deprecate the old one.  

## Installation

This project uses [Maven](http://maven.apache.org/ "Apache Maven"). To use HPaste in your own maven project, simply add it as a dependency:

```xml
<dependency>
  <groupId>com.gravity</groupId>
  <artifactId>gravity-hpaste</artifactId>
  <version>0.1.5</version>
</dependency>
```

## Quickstart

Here's some quick code examples to give you a sense of what you're getting into.  All of the examples in the sections below come from the HPaste unit tests.  Specifically the file [WebCrawlSchemaTest.scala](https://github.com/GravityLabs/HPaste/blob/master/src/test/scala/com/gravity/hbase/schema/WebCrawlSchemaTest.scala).  If you go to that file and follow along with the explanations below, things will make more sense.

#### Creating a WebTable
The classic case for HBase and BigTable is crawling and storing web pages.  You need to define a WebTable for your crawling.  The below defines a table called WebTable, with a String key.

* It has a column family called "meta", that holds columns with String keys and Any value-type.
* It then specifies that the "meta" family holds a "title" column, a "lastCrawled" column, and a "url" column.
* A second column family holds content and has the compressed flag to true.
* It has a family "content" for storing content.  The "article" column is for storing the main page content.
* The Attributes column is a map where you can atomically store values keyed by a string.
* SearchMetrics is a column family that contains searches your users have made that have sent them to that page, organized by day.

```scala
class WebTable extends HbaseTable[WebTable, String, WebPageRow](tableName = "pages", rowKeyClass = classOf[String]) {
    def rowBuilder(result: DeserializedResult) = new WebPageRow(this, result)

    val meta = family[String, String, Any]("meta")
    val title = column(meta, "title", classOf[String])
    val lastCrawled = column(meta, "lastCrawled", classOf[DateTime])

    val content = family[String, String, Any]("text", compressed = true)
    val article = column(content, "article", classOf[String])
    val attributes = column(content, "attrs", classOf[Map[String, String]])

    val searchMetrics = family[String, DateMidnight, Long]("searchesByDay")


  }

  class WebPageRow(table: WebTable, result: DeserializedResult) extends HRow[WebTable, String](result, table)

  val WebTable = table(new WebTable)
}
```

#### Putting values into the WebTable

Now, suppose you're crawling a website.  The below will create a row with the values specified.  When you call value(), the first argument is a function that points to the column you specified in the above WebTable schema.  This dips into DSL-land.

```scala
WebCrawlingSchema.WebTable
            .put("http://mycrawledsite.com/crawledpage.html")
            .value(_.title, "My Crawled Page Title")
            .value(_.lastCrawled, new DateTime())
            .value(_.article, "Jonsie went to the store.  She didn't notice the spinning of the Earth, nor did the Earth notice the expansion of the Universe.")
            .value(_.attributes, Map("foo" -> "bar", "custom" -> "data"))
            .valueMap(_.searchMetrics, Map(new DateMidnight(2011, 6, 5) -> 3l, new DateMidnight(2011, 6, 4) -> 34l))
            .execute()
```

#### Querying values out of the WebTable

Let's get the above page out of the WebTable.  Let's say we just want the title of the page and when it was last crawled.  The withColumns() call tells HBase to only fetch those columns.  It takes a series of functions that return the column values you specified in the WebTable, so you get compile-time checking on that.  

```scala
WebCrawlingSchema.WebTable.query2.withKey("http://mycrawledsite.com/crawledpage.html")
            .withColumns(_.title, _.lastCrawled)
            .withFamilies(_.searchMetrics)
            .singleOption() match {
      case Some(pageRow) => {
        println("Title: " + pageRow.column(_.title).getOrElse("No Title"))
        println("Crawled on: " + pageRow.column(_.lastCrawled).getOrElse(new DateTime()))

        pageRow.family(_.searchMetrics).foreach {
          case (date: DateMidnight, views: Long) =>
            println("Got " + views + " views on date " + date.toString("MM-dd-yyyy"))
        }
        //Do something with title and crawled date...
      }
      case None => {
        println("Row not found")
      }
    }
```

The result you get back is an instance of the row class you specified against the WebTable: WebPageRow.  When you get a WebPageRow back from a query, a scan, or a map reduce job, you can fetch the columns out via the column() call.  If you asked for a column family that can be treated as a Map (a Column Family that does not have columns specified in it), then you can retrieve the map via the family() call.

#### Aggregating values via MapReduce jobs

HPaste contains support for low-level MapReduce operations.  What we mean by low-level is that there are not many layers of abstraction on top of a basic MR job--instead, we focus on making it easy to create and manage table inputs and outputs and serialize binary data between them.

Let's say we have a lot of pages crawled, and we, as a search engine, have searches people have performed for those pages.  We now want to roll those into per-site totals.  Let's create a table next to our WebTable called the SiteMetricsTable:

```scala
class SiteMetricsTable extends HbaseTable[SiteMetricsTable, String, SiteMetricsRow](tableName = "site-metrics", rowKeyClass = classOf[String]) {
    def rowBuilder(result: DeserializedResult) = new SiteMetricsRow(this, result)

    val meta = family[String, String, Any]("meta")
    val name = column(meta, "name", classOf[String])

    val searchMetrics = family[String, DateMidnight, Long]("searchesByDay")
}

class SiteMetricsRow(table: SiteMetricsTable, result: DeserializedResult) extends HRow[SiteMetricsTable, String](result, table)

val Sites = table(new SiteMetricsTable)

```

Now we'll make a MapReduce job that scans the WebPages table, aggregates its metrics, and writes them to the SiteMetricsTable:

```scala
class WebSearchAggregationJob extends HJob[NoSettings]("Aggregate web searches by site",
  HMapReduceTask(
    HTaskID("Aggregation task"),
    HTaskConfigs(),
    HIO(
      HTableInput(WebCrawlingSchema.WebTable),
      HTableOutput(WebCrawlingSchema.Sites)
    ),
    new FromTableBinaryMapperFx(WebCrawlingSchema.WebTable) {
      val webPage = row
      val domain = new URL(webPage.rowid).getAuthority
      ctr("Sites for domain" + domain)

      val dates = webPage.family(_.searchMetrics)

      for((dateOfSearches,searchCount) <- dates) {
        val keyOutput = makeWritable{keyWriter=>
          keyWriter.writeUTF(domain)
          keyWriter.writeObj(dateOfSearches)
        }
        val valueOutput = makeWritable{valueWriter=>
          valueWriter.writeLong(searchCount)
        }
        ctr("Dated metrics written for domain " + domain)
        write(keyOutput, valueOutput)
      }
    },
    new ToTableBinaryReducerFx(WebCrawlingSchema.Sites) {
      val (domain, dateOfSearches) = readKey{keyInput=>
        (keyInput.readUTF(), keyInput.readObj[DateMidnight])
      }

      var totalCounts = 0l

      perValue{valueInput=>
        totalCounts += valueInput.readLong
      }


      write(
        WebCrawlingSchema.Sites.put(domain).valueMap(_.searchMetrics,Map(dateOfSearches->totalCounts))
      )
    }
  )
)

```
The above is a self-contained MapReduce job that is ready to go.  We can execute the above job via (where the Configuration object is the one relevant to your clsuter):

```scala
new WebSearchAggregationJob().run(Settings.None, LocalCluster.getTestConfiguration)
```

All of the above examples are part of the HPaste unit tests, so it should be easy to use them to set up your own system.  

## Features not covered in the Quickstart
There are many features not included in the Quickstart that exist in the codebase.  

* Complex type serialization
* Chaining map reduce jobs
* Scanners and the filtration DSL
* Settings classes that can be injected into MapReduce jobs

# Building and Testing HPaste
This project is put together with Maven.  In theory you should be able to build and run the project's tests via:

``
mvn test
``

The tests will create a temporary hbase cluster, create temporary tables, and run map reduce jobs against those tables.  The unit tests are the best way to encounter HPaste, because they are constantly added to and perform live operations against a real cluster, so there's no smoke-and-mirrors.

# More In Depth

## Defining a Schema

Following the HBase structure, first you define a Schema, then Tables, then Column Families, then (optionally) Columns.  Below is an example schema that contains a single Table definition.  The table will be called "schema_example" in HBase.  It will expect its row keys to be Strings.

```scala
object ExampleSchema extends Schema {


  //There should only be one HBaseConfiguration object per process.  You'll probably want to manage that
  //instance yourself, so this library expects a reference to that instance.  It's implicitly injected into
  //the code, so the most convenient place to put it is right after you declare your Schema.
  implicit val conf = LocalCluster.getTestConfiguration

  //A table definition, where the row keys are Strings
  class ExampleTable extends HbaseTable[ExampleTable,String, ExampleTableRow](tableName = "schema_example",rowKeyClass=classOf[String])
  {
    def rowBuilder(result:DeserializedResult) = new ExampleTableRow(this,result)

    val meta = family[String, String, Any]("meta")
    //Column family definition
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
    val viewsMap = column(meta,"viewsMap", classOf[Map[String,Long]])

    //A column family called views whose column names are Strings and values are Longs.  Can be treated as a Map
    val viewCounts = family[String, String, Long]("views")

    //A column family called views whose column names are YearDay instances and whose values are Longs
    val viewCountsByDay = family[String, YearDay, Long]("viewsByDay")

    //A column family called kittens whose column values are the custom Kitten type
    val kittens = family[String,String,Kitten]("kittens")
  }

  class ExampleTableRow(table:ExampleTable,result:DeserializedResult) extends HRow[ExampleTable,String](result,table)

  //Register the table (DON'T FORGET TO DO THIS :) )
  val ExampleTable = table(new ExampleTable)

}

```

The above table has a column family called meta, and several strongly-typed columns in the "meta" family.  It then creates several column families that do not have column definitions under them ("views" and "viewsByDay").  The reason for this is that in HBase you tend to create two different types of column families: in one scenario, you create a column family that contains a set of columns that resemble columns in an RDBMS: each column is "typed" and has a unique identity.  In the second scenario, you create a column family that resembles a Map: you dynamically add key-value pairs to this family.  In this second scenario, you don't know what the columns are ahead of time--you'll be adding and removing columns on the fly.  HPaste supports both models.

## Lifecycle Management

Because HBase prefers to have a single instance of the Configuration object for connection management purposes, HPaste does not manage any Configuration lifecycle.  When you specify your schema, you need to have an implicit instance of Configuration in scope.

## Table Creation Scripts
If you have an existing table in HBase with the same name and families, you can get started.  If you don't, you can now call:
```scala
    val create = ExampleSchema.ExampleTable.createScript()
    println(create)
```
Paste the results into your hbase shell to create the table.

## Data Manipulation
HPaste supports GETS, PUTS, and INCREMENTS.  You can batch multiple operations together, or do them serially.  

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

```scala
implicit object KittenSeqConverter extends SeqConverter[Kitten]
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

# More on MapReduce support

## Row Serialization between Mappers and Reducers

HRows can be serialized between Mappers and Reducers, using the writeRow and readRow methods of PrimitiveOutputStream and PrimitiveInputStream.

This is often the least performant way of passing data between mappers and reducers (depending on how big your rows are), but also involves the least amount of code.

Below is a job that assumes the same WebCrawling schema in the Quickstart.  Its goal is to write output files that show page by the site they're contained in.  We've added a convenience function to the WebPageRow to extract the domain:

```scala
  class WebPageRow(table: WebTable, result: DeserializedResult) extends HRow[WebTable, String](result, table) {
    def domain = new URL(rowid).getAuthority
  }
```

Here is the job.  

```scala
class WebTablePagesBySiteJob extends HJob[NoSettings]("Get articles by site",
  HMapReduceTask(
    HTaskID("Articles by Site"),
    HTaskConfigs(),
    HIO(
      HTableInput(WebCrawlingSchema.WebTable),
      HPathOutput("/reports/wordcount")
    ),
    new FromTableBinaryMapperFx(WebCrawlingSchema.WebTable) {
      val webPage : WebPageRow = row //For illustrative purposes we're specifying the type here, no need to
      val domain = row.domain //We've added a convenience method to WebPageRow to extract the domain for us

      write(
      {keyOutput=>keyOutput.writeUTF(domain)}, //This writes out the domain as the key
      {valueOutput=>valueOutput.writeRow(WebCrawlingSchema.WebTable,webPage)} //This writes the entire value of the row out
      )
    },
    new BinaryToTextReducerFx {
      val domain = readKey(_.readUTF()) //This allows you to read out the key

      perValue{valueInput=>
        val webPage : WebPageRow = valueInput.readRow(WebCrawlingSchema.WebTable) //Now you can read out the entire WebPageRow object from the value stream
        ctr("Pages for domain " + domain)
        writeln(domain + "\t" + webPage.column(_.title).getOrElse("No Title")) //This is a convenience function that writes a line to the text output
      }
    }
  )
)

```
This job is part of HPaste's unit tests, so you can see it in context at [WebCrawlSchemaTest.scala](https://github.com/GravityLabs/HPaste/blob/master/src/test/scala/com/gravity/hbase/schema/WebCrawlSchemaTest.scala).

# Developers

This library was developed by [Chris Bissell](https://github.com/Lemmsjid "Lemmsjid"), with many contributions, both philosophical and code, by [Robbie Coleman](https://github.com/erraggy "erraggy").