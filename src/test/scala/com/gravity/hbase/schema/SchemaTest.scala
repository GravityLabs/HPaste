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
  }

}

class SchemaTest  {

  @Test def createAndDelete() {
    val create = ExampleSchema.ExampleTable.createScript()
    println(create)
  }

  @Test def testPut() {
    ExampleSchema.ExampleTable
      .put("Chris").value(ExampleSchema.ExampleTable.title,"My Life, My Times")
      .put("Joe").value(ExampleSchema.ExampleTable.title,"Joe's Life and Times")
      .increment("Chris").value(ExampleSchema.ExampleTable.views,10l)
      .execute()
    
    ExampleSchema.ExampleTable.put(1346l).value(ExampleSchema.ExampleTable.title,"My kittens").execute

    val views = ExampleSchema.ExampleTable.query.withKey("Chris").withColumn(ExampleSchema.ExampleTable.views).single().column(ExampleSchema.ExampleTable.views)

    println("Views: " + views)
  }
}