package com.gravity.hbase.schema

import org.apache.hadoop.hbase.HBaseConfiguration
import org.junit.Test

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object ExampleSchema {
  implicit val conf = HBaseConfiguration.create

  object ExampleTable extends HbaseTable(tableName="schema_example") {
    val meta = family[String,String,Any]("meta")
    val title = column[String,String,String](meta,"title")
    val url = column[String,String,String](meta,"url")


  }

}

class SchemaTest  {

  @Test def createAndDelete() {
    val create = ExampleSchema.ExampleTable.createScript()
    println(create)
  }
}