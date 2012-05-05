/** Licensed to Gravity.com under one
  * or more contributor license agreements. See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership. Gravity.com licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package com.gravity.hbase.schema

import org.junit.Assert._
import scala.collection._
import org.junit._
import org.joda.time.DateTime
import CustomTypes._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * This test is intended to simultaneously test the library and show how you put together your own schema.
 */

/**
 * CUSTOM TYPES
 */
case class Kitten(name:String, age:Int, height:Double)

case class PageUrl(url:String)

/**
 * CUSTOM SERIALIZERS
 * These are serializers for custom types.  When you create your own serializers, which is common, it's useful to put them
 * in their own object definition.  Then, when you need the serializers in client code, make sure you import the object.  For
 * the below, you'd do
 * import com.gravity.hbase.schema.CustomTypes._
 */
object CustomTypes {

  implicit object PageUrlConverter extends ComplexByteConverter[PageUrl] {
    override def write(url:PageUrl, output:PrimitiveOutputStream) {
      output.writeUTF(url.url)
    }
    override def read(input:PrimitiveInputStream) = {
      PageUrl(input.readUTF())
    }
  }

  implicit object KittenConverter extends ComplexByteConverter[Kitten] {
    override def write(kitten:Kitten, output:PrimitiveOutputStream)  {
      output.writeUTF(kitten.name)
      output.writeInt(kitten.age)
      output.writeDouble(kitten.height)
    }

    override def read(input:PrimitiveInputStream) = {
      Kitten(input.readUTF(), input.readInt(), input.readDouble())
    }
  }

  implicit object KittenSeqConverter extends SeqConverter[Kitten]
}


object ExampleSchema extends Schema {


  //There should only be one HBaseConfiguration object per process.  You'll probably want to manage that
  //instance yourself, so this library expects a reference to that instance.  It's implicitly injected into
  //the code, so the most convenient place to put it is right after you declare your Schema.
  implicit val conf = LocalCluster.getTestConfiguration

  //A table definition, where the row keys are Strings
  class ExampleTable extends HbaseTable[ExampleTable,String, ExampleTableRow](tableName = "schema_example",rowKeyClass=classOf[String], tableConfig = HbaseTableConfig(maxFileSizeInBytes=1073741824))
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




/**
 * This test is intended to simultaneously test the library and show how you put together your own schema.
 */
class ExampleSchemaTest extends HPasteTestCase(ExampleSchema) {

  /**
   * Test that a complex custom type can be added and retrieved from a table as a Map
   */
  @Test def testComplexCustomType() {
    val kittens = Map("Suki" -> Kitten("Suki",9,8.6), "Efrem" -> Kitten("Efrem",8,6.8), "Rory" -> Kitten("Rory",9,9.6),"Scout"->Kitten("Scout",8,12.3))

    ExampleSchema.ExampleTable.put("Chris").valueMap(_.kittens,kittens).execute()

    val result = ExampleSchema.ExampleTable.query.withKey("Chris").withColumnFamily(_.kittens).single()
    val kittens2 = result.family(_.kittens)

    Assert.assertEquals(kittens,kittens2)
  }

  /**
   * Test that the create script looks right
   */
  @Test def testCreateScript() {
    val createScript = """create 'schema_example', {NAME => 'meta', VERSIONS => 1},{NAME => 'views', VERSIONS => 1},{NAME => 'viewsByDay', VERSIONS => 1},{NAME => 'kittens', VERSIONS => 1}
alter 'schema_example', {METHOD => 'table_att', MAX_FILESIZE => '1073741824'}"""
    
    val create = ExampleSchema.ExampleTable.createScript()
    println(create)
    Assert.assertEquals(createScript,create)
  }

  /**
   * Test the creation of a multi column alter script
   */
  @Test def testAlterScript() {
    val expected = """flush 'schema_example'
disable 'schema_example'
alter 'schema_example', {NAME => 'kittens', VERSIONS => 1},{NAME => 'views', VERSIONS => 1}
alter 'schema_example', {METHOD => 'table_att', MAX_FILESIZE => '1073741824'}
enable 'schema_example'"""

    val alter = ExampleSchema.ExampleTable.alterScript(families=ExampleSchema.ExampleTable.kittens :: ExampleSchema.ExampleTable.viewCounts :: Nil)

    Assert.assertEquals(expected,alter)
  }

  /**
   * Helper method
   */
  def dumpViewMap(key: String) {
    val dayViewsRes = ExampleSchema.ExampleTable.query.withKey(key).withColumnFamily(_.viewCountsByDay).withColumn(_.views).withColumn(_.title).single()

    val dayViewsMap = dayViewsRes.family(_.viewCountsByDay)

    for ((yearDay, views) <- dayViewsMap) {
      println("Got yearday " + yearDay + " with views " + views)
    }
  }

  @Test def testMaps() {
    val viewMap = Map("Chris"->50l, "Fred" -> 100l)
    ExampleSchema.ExampleTable
      .put("MapTest").value(_.viewsMap,viewMap)
      .execute()

    val res = ExampleSchema.ExampleTable.query.withKey("MapTest").withColumn(_.viewsMap).single()
    val returnedMap = res.column(_.viewsMap).get

    Assert.assertEquals(returnedMap,viewMap)
  }

  @Test def testSeqs() {
    ExampleSchema.ExampleTable
      .put("SeqTest").value(_.viewsArr, Seq("Chris","Fred","Bill"))
      .execute()

    val res = ExampleSchema.ExampleTable.query.withKey("SeqTest").withColumn(_.viewsArr).single()

    val resSeq = res.column(_.viewsArr).get
    Assert.assertEquals(resSeq(0),"Chris")
    Assert.assertEquals(resSeq(1),"Fred")
    Assert.assertEquals(resSeq(2),"Bill")
  }

  /**
   * This test does a bunch of operations without asserts, it's here to play around with the data.
   */
  @Test def testPut() {
    ExampleSchema.ExampleTable
            .put("Chris").value(_.title, "My Life, My Times")
            .put("Joe").value(_.title, "Joe's Life and Times")
            .put("Fred").value(_.viewsArr,Seq("Chris","Bissell"))
            .increment("Chris").value(_.views, 10l)
            .put("Chris").valueMap(_.viewCountsByDay, Map(YearDay(2011,16)->60l, YearDay(2011,17)->50l))
            .put("Fred").value(_.viewsMap, mutable.Map("Chris"->50l,"Bissell"->100l))
            .execute()

    val arrRes = ExampleSchema.ExampleTable.query.withKey("Fred").withColumn(_.viewsArr).withColumn(_.viewsMap).single()

    val arr = arrRes.column(_.viewsArr)

    arr.get.foreach(println)

    val arrMap = arrRes.column(_.viewsMap)
    arrMap.get.foreach((tuple: (String, Long)) => println(tuple._1 + " views " + tuple._2))

    val id = "Bill"

    ExampleSchema.ExampleTable.put(id).value(_.title, "My kittens").execute()

    ExampleSchema.ExampleTable.put(id).valueMap(_.viewCounts, Map("Today" -> 61l, "Yesterday" -> 86l)).execute()

    val dayMap = Map(
      YearDay(2011, 63) -> 64l,
      YearDay(2011, 64) -> 66l,
      YearDay(2011, 65) -> 67l
    )


    ExampleSchema.ExampleTable.put(id).valueMap(_.viewCountsByDay, dayMap).execute()

    println("Dumping after map insert")
    dumpViewMap(id)

    ExampleSchema.ExampleTable.increment(id).valueMap(_.viewCountsByDay, dayMap).execute()

    println("Dumping after increment")
    dumpViewMap(id)

    ExampleSchema.ExampleTable.delete(id).family(_.viewCountsByDay).execute()
    println("Dumping after delete")
    dumpViewMap(id)

    val views = ExampleSchema.ExampleTable.query.withKey("Chris").withColumn(_.views).single().column(_.views)

    val myviewqueryresult = ExampleSchema.ExampleTable.query.withKey("Chris").withColumn(_.views).single()


    println("Views: " + views.get)
  }

  @Test def testWithKeys() {
    ExampleSchema.ExampleTable.put("Robbie").value(_.title, "My Bros, My Probs")
            .put("Ronnie").value(_.title, "My Weights, My Muskellz").execute()

    val bros = ExampleSchema.ExampleTable.query.withKeys(Set("Robbie", "Ronnie")).withColumnFamily(_.meta).execute()

    if (bros.isEmpty) fail("Failed to retrieve the data we just put!")

    for (bro <- bros) {
      bro.column(_.title) match {
        case Some(title) => println("%nBro: %s; title: %s".format(bro.rowid, title))
        case None => fail("FAILED TO GET TITLE!")

      }
    }

  }



}

