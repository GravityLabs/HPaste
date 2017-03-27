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
  import CustomTypes._


  //There should only be one HBaseConfiguration object per process.  You'll probably want to manage that
  //instance yourself, so this library expects a reference to that instance.  It's implicitly injected into
  //the code, so the most convenient place to put it is right after you declare your Schema.
  implicit val conf = LocalCluster.getTestConfiguration

  //A table definition, where the row keys are Strings
  class ExampleTable extends HbaseTable[ExampleTable,String, ExampleTableRow](tableName = "schema_example",rowKeyClass=classOf[String], tableConfig = HbaseTableConfig(maxFileSizeInBytes=1073741824), cache = new TestCache())
  {
    def rowBuilder(result:DeserializedResult) = new ExampleTableRow(this,result)

    val meta = family[String, Any]("meta")
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
    val viewsMap: Col[Map[String, Long]] = column(meta,"viewsMap", classOf[Map[String,Long]])

    //A column family called views whose column names are Strings and values are Longs.  Can be treated as a Map
    val viewCounts = family[String, Long]("views")

    //A column family called views whose column names are YearDay instances and whose values are Longs
    val viewCountsByDay = family[YearDay, Long]("viewsByDay")

    //A column family called kittens whose column values are the custom Kitten type
    val kittens = family[String,Kitten]("kittens")

    val misc = family[String, Any]("misc")

    val misc1 = column(misc, "misc1", classOf[String])
    val misc2 = column(misc, "misc2", classOf[String])
    val misc3 = column(misc, "misc3", classOf[String])
  }

  class ExampleTableRow(table:ExampleTable,result:DeserializedResult) extends HRow[ExampleTable,String](result,table)

  //Register the table (DON'T FORGET TO DO THIS :) )
  val ExampleTable = table(new ExampleTable)

}




/**
 * This test is intended to simultaneously test the library and show how you put together your own schema.
 */
class ExampleSchemaTest extends HPasteTestCase(ExampleSchema) {
  implicit val conf = LocalCluster.getTestConfiguration

  /**
   * Test that a complex custom type can be added and retrieved from a table as a Map
   */
  @Test def testComplexCustomType() {
    val kittens = Map("Suki" -> Kitten("Suki",9,8.6), "Efrem" -> Kitten("Efrem",8,6.8), "Rory" -> Kitten("Rory",9,9.6),"Scout"->Kitten("Scout",8,12.3))

    ExampleSchema.ExampleTable.put("Chris").valueMap(_.kittens,kittens).execute()

    val result = ExampleSchema.ExampleTable.query2.withKey("Chris").withFamilies(_.kittens).single(skipCache = true)

    val kittens2 = result.family(_.kittens)

    Assert.assertEquals(kittens,kittens2)
  }

  /**
   * Test that a complex custom type can be added and retrieved from a table as a Map
   */
  @Test def testDuplicateMappings() {

    ExampleSchema.ExampleTable.put("Chris").value(_.misc1, "value1").value(_.title, "some title").value(_.url, "http://example.com").execute()

    val result = ExampleSchema.ExampleTable.query2.withKey("Chris").withFamilies(_.meta).withColumns(_.title, _.misc1).single()

    Assert.assertEquals(Some("some title"), result.column(_.title))
    Assert.assertEquals(Some("http://example.com"), result.column(_.url))
    Assert.assertEquals(Some("value1"), result.column(_.misc1))
  }

  /**
   * Test that the create script looks right
   */
  @Test def testCreateScript() {
    val createScript = """create 'schema_example', {NAME => 'meta', VERSIONS => 1},{NAME => 'views', VERSIONS => 1},{NAME => 'viewsByDay', VERSIONS => 1},{NAME => 'kittens', VERSIONS => 1},{NAME => 'misc', VERSIONS => 1}
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
    val dayViewsRes = ExampleSchema.ExampleTable.query2.withKey(key).withFamilies(_.viewCountsByDay).withColumn(_.views).withColumn(_.title).single(skipCache = false)

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

    val res = ExampleSchema.ExampleTable.query2.withKey("MapTest").withColumn(_.viewsMap).single(skipCache = false)
    val returnedMap = res.column(_.viewsMap).get

    Assert.assertEquals(returnedMap,viewMap)
  }

  @Test def testSeqs() {
    ExampleSchema.ExampleTable
      .put("SeqTest").value(_.viewsArr, Seq("Chris","Fred","Bill"))
      .execute()

    val res = ExampleSchema.ExampleTable.query2.withKey("SeqTest").withColumn(_.viewsArr).single(skipCache = false)

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

    val arrRes = ExampleSchema.ExampleTable.query2.withKey("Fred").withColumn(_.viewsArr).withColumn(_.viewsMap).single(skipCache = false)

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

    val views = ExampleSchema.ExampleTable.query2.withKey("Chris").withColumn(_.views).single().column(_.views)

    val myviewqueryresult = ExampleSchema.ExampleTable.query2.withKey("Chris").withColumn(_.views).single(skipCache = false)


    println("Views: " + views.get)
  }

  @Test def testWithKeys() {
    ExampleSchema.ExampleTable.put("Robbie").value(_.title, "My Bros, My Probs")
            .put("Ronnie").value(_.title, "My Weights, My Muskellz").execute()

    val bros = ExampleSchema.ExampleTable.query2.withKeys(Set("Robbie", "Ronnie")).withFamilies(_.meta).executeMap(skipCache = false)

    if (bros.isEmpty) fail("Failed to retrieve the data we just put!")

    for (bro <- bros) {
      bro._2.column(_.title) match {
        case Some(title) => println("%nBro: %s; title: %s".format(bro._2.rowid, title))
        case None => fail("FAILED TO GET TITLE!")

      }
    }

    println("ASYNC TIME")
    ExampleSchema.ExampleTable.query2.withKey("Robbie").withFamilies(_.meta).singleOptionAsync().get.prettyPrint()
  }

  @Test def testColumnValueMustBePresent() {
    ExampleSchema.ExampleTable
      .put("Manny").value(_.title, "Pep Boys #1").value(_.views, 1l)
      .put("Moe").value(_.title, "Pep Boys #2")
      .put("Jack").value(_.views, 3l)
      .execute()

    val mapRes = ExampleSchema.ExampleTable.query2
      .withKeys(Set("Manny", "Moe", "Jack"))
      .withColumns(_.title, _.views)
      .filter(
        _.and(
          _.columnValueMustBePresent(_.views)
        )
      ).multiMap(skipCache = false)

    // Moe doesn't have any views, so we should only see Manny and Jack.
    Assert.assertEquals(Set("Manny", "Jack"), mapRes.keySet)

    // The surviving rows should all have views (because that was required).
    Assert.assertTrue(mapRes.values.forall(_.column(_.views).isDefined))

    // At least one of the surviving rows should also have a title (because Manny has a title).
    Assert.assertTrue(mapRes.values.exists(_.isColumnPresent(_.title)))
  }

  @Test def testFamilyPutTimeStamps(): Unit = {
    val time3 = new DateTime()
    val time2 = time3.minusSeconds(1)
    val time1 = time3.minusSeconds(2)

    val values     = Map("time1" -> 1L   , "time2" -> 2L   , "time3" -> 3L   , "time4" -> 4L)
    val timeStamps = Map("time1" -> time1, "time2" -> time2, "time3" -> time3)

    ExampleSchema.ExampleTable.put("testFamilyPutTimeStamps").valueMap(_.viewCounts, values, timeStamps).execute()

    val time4 = new DateTime().plusSeconds(1)

    val row = ExampleSchema.ExampleTable.query2.withKey("testFamilyPutTimeStamps").withAllColumns.single()

    for (col <- values.keys) {
      val actTimeStamp = row.columnFromFamilyTimestamp(_.viewCounts, col).get

      timeStamps.get(col) match {
        case Some(expTimeStamp) =>
          Assert.assertEquals(expTimeStamp, actTimeStamp)

        case None =>
          Assert.assertTrue(actTimeStamp.getMillis >= time3.getMillis && actTimeStamp.getMillis < time4.getMillis)
      }
    }
  }

  @Test def testOpResultPlus(): Unit = {
    val result1: OpsResult = ExampleSchema.ExampleTable
      .put("Chris").value(_.title, "My Life, My Times")
      .put("Joe").value(_.title, "Joe's Life and Times")
      .put("Fred").value(_.viewsArr,Seq("Chris","Bissell"))
      .increment("Chris").value(_.views, 10l)
      .put("Chris").valueMap(_.viewCountsByDay, Map(YearDay(2011,16)->60l, YearDay(2011,17)->50l))
      .put("Fred").value(_.viewsMap, mutable.Map("Chris"->50l,"Bissell"->100l))
      .execute()

    val result2: OpsResult = ExampleSchema.ExampleTable
      .delete("Chris").family(_.viewCountsByDay)
      .put("Tom").value(_.title, "Toms's Life and Times")
      .put("Tom").valueMap(_.viewCountsByDay, Map(YearDay(2017,16)->60l, YearDay(2016,17)->50l))
      .increment("Tom").value(_.views, 20l)
      .execute()

    val result3: OpsResult = ExampleSchema.ExampleTable
      .delete("Tom").family(_.viewCountsByDay)
      .put("Moe").value(_.title, "Moe's Life and Times")
      .increment("Moe").value(_.views, 30l)
      .execute()

    val finalResult: OpsResult = result1 + result2 + result3
    Assert.assertEquals(OpsResult(2,8,3), finalResult)
  }
}

