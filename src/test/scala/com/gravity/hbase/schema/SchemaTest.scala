package com.gravity.hbase.schema

import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.Test

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object ExampleSchema extends Schema {
  implicit val conf = HBaseConfiguration.create

  object ExampleTable extends HbaseTable(tableName="schema_example") {

    val meta = family[String,String,Any]("meta")
    val title = column(meta,"title", classOf[String])
    val url = column(meta,"url", classOf[String])
    val views = column(meta,"views", classOf[Long])

    val viewCounts = family[String,String,Long]("views")

    val viewCountsByDay = family[String,YearDay,Long]("viewsByDay")
  }

}

class SchemaTest  {

  @Test def createAndDelete() {
    val create = ExampleSchema.ExampleTable.createScript()
    println(create)
  }

  def dumpViewMap(key:Long) {
    val dayViewsRes = ExampleSchema.ExampleTable.query.withKey(key).withColumnFamily(ExampleSchema.ExampleTable.viewCountsByDay).single
    val dayViewsMap = dayViewsRes.family(ExampleSchema.ExampleTable.viewCountsByDay)

    for((yearDay, views) <- dayViewsMap) {
      println("Got yearday " + yearDay + " with views " + views)
    }
  }

  @Test def testPut() {
    ExampleSchema.ExampleTable
      .put("Chris").value(ExampleSchema.ExampleTable.title,"My Life, My Times")
      .put("Joe").value(ExampleSchema.ExampleTable.title,"Joe's Life and Times")
      .increment("Chris").value(ExampleSchema.ExampleTable.views,10l)
      .execute()
    
    ExampleSchema.ExampleTable.put(1346l).value(ExampleSchema.ExampleTable.title,"My kittens").execute

    ExampleSchema.ExampleTable.put(1346l).valueMap(ExampleSchema.ExampleTable.viewCounts, Map("Today" -> 61l, "Yesterday" -> 86l)).execute

    val dayMap = Map(
      YearDay(2011,63) -> 64l,
      YearDay(2011,64) -> 66l,
      YearDay(2011,65) -> 67l
    )

    val id = 1346l

    ExampleSchema.ExampleTable.put(id).valueMap(ExampleSchema.ExampleTable.viewCountsByDay, dayMap).execute

    println("Dumping after map insert")
    dumpViewMap(id)

    ExampleSchema.ExampleTable.increment(id).valueMap(ExampleSchema.ExampleTable.viewCountsByDay, dayMap).execute

    println("Dumping after increment")
    dumpViewMap(id)

    ExampleSchema.ExampleTable.delete(id).family(ExampleSchema.ExampleTable.viewCountsByDay).execute
    println("Dumping after delete")
    dumpViewMap(id)

    val views = ExampleSchema.ExampleTable.query.withKey("Chris").withColumn(ExampleSchema.ExampleTable.views).single().column(ExampleSchema.ExampleTable.views)

    println("Views: " + views.get)
  }
}