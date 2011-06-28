# Introduction

HBase provides a very rich low-level interface, but due to a plethora of byte arrays it can be difficult to program against.

The gravity-hbase-schema library aims to be a nice Scala interface on top of HBase.  While it may resemble an ORM from the outside, it is not--instead it is a schema-based query tool that follows the conventions of HBase querying, rather than trying to abstract into a domain model.

# Schema Definition

Following the HBase structure, first you define a Schema, then Tables, then Column Families, then (optionally) Columns.  Below is an example schema that contains a single Table definition.  The table will be called "schema_example" in HBase.  It will expect its row keys to be Strings (the second type parameter to HbaseTable).

```scala
  object ExampleSchema extends Schema {
    implicit val conf = SchemaTest.htest.getConfiguration

    class ExampleTable extends HbaseTable[ExampleTable,String](tableName = "schema_example") {

      val meta = family[String, String, Any]("meta")
      val title = column(meta, "title", classOf[String])
      val url = column(meta, "url", classOf[String])
      val views = column(meta, "views", classOf[Long])

      val viewCounts = family[String, String, Long]("views")
      val viewCountsByDay = family[String, YearDay, Long]("viewsByDay")
    }

    val ExampleTable = new ExampleTable
  }
```

# Data Manipulation

## Column Valued Operations

Given the above example schema, the following example chains a series of operations together.  We will put a title against Chris, a title against Joe, and increment Chris' "views" column by 10.  If the columns do not exist, they will be lazily created as per HBase convention.

```scala
    ExampleSchema.ExampleTable
            .put("Chris").value(_.title, "My Life, My Times")
            .put("Joe").value(_.title, "Joe's Life and Times")
            .increment("Chris").value(_.views, 10l)
            .execute()
```

When you call execute(), the operations you chained together will be executed in the following order: DELETES, then PUTS, then INCREMENTS.  We decided to order the operations in that way because that is how they will generally be ordered in normal use cases, and also because HBase cannot guarantee ordering in batch-operations in which increments are mixed in with deletes and puts.

### Enforcing your own ordering

If you need to enforce your own ordering, you should break your statements apart by calling execute() when you want to flush operations to HBase.

## Column Family Operations

Often times you will define a column family where the columns themselves hold data (as columns are dynamically created in HBase).

Here's the "views" column we specified in the example schema.  The type parameters tell the system that we expect this column family to have a string-valued family name, a string-valued column name, and a Long value.

```scala
      val viewCounts = family[String, String, Long]("views")
```

Because Column Families treated in this fashion resemble the Map construct most closely, that's what we support. The following will put two columns titled "Today" and "Yesterday" into the Example Table under the key 1346:

```scala
    ExampleSchema.ExampleTable.put(1346l).valueMap(_.viewCounts, Map("Today" -> 61l, "Yesterday" -> 86l)).execute
```

# Data Retrieval

Data retrieval in HBase involves two steps:

1. Creating a specification for your Get operation, in which you decide what rows to get back, what columns and column families to get back.

2. When you issue the retrieval operation and get the results back, extracting the results into useful data.

Assuming the examples under the DATA MANIPULATION section, the following will retrieve a Map of String to Long out of the Example Table:

```scala
    val dayViewsRes = ExampleSchema.ExampleTable.query.withKey(key).withColumnFamily(_.viewCountsByDay).single()
    val dayViewsMap = dayViewsRes.family(_.viewCountsByDay)
```

# Lifecycle Management

Because HBase prefers to have a single instance of the Configuration object for connection management purposes, gravity-hbase-schema does not manage any Configuration lifecycle.  When you specify your schema, you need to have an implicit instance of Configuration in scope.

