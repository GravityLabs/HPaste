package com.gravity.hbase.schema

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.util.Bytes
import collection.mutable.ArrayBuffer
import org.junit.Test
import org.junit.Assert._
import junit.framework.TestCase

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object ClusterTest {
  val htest = new HBaseTestingUtility()
  htest.startMiniCluster()
  val fams = ArrayBuffer[Array[Byte]]()
  fams += Bytes.toBytes("meta")
  fams += Bytes.toBytes("views")
  fams += Bytes.toBytes("viewsByDay")

  val table = htest.createTable(Bytes.toBytes("schema_example"), fams.toArray)
}

class ClusterTest extends TestCase {

  object ExampleSchema extends Schema {
    implicit val conf = ClusterTest.htest.getConfiguration

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

  @Test def testCreateAndDelete() {
    val create = ExampleSchema.ExampleTable.createScript()
    println(create)
  }

  def dumpViewMap(key: String) {
    val dayViewsRes = ExampleSchema.ExampleTable.query.withKey(key).withColumnFamily(_.viewCountsByDay).withColumn(_.views).withColumn(_.title).single()

    val dayViewsMap = dayViewsRes.family(_.viewCountsByDay)

    for ((yearDay, views) <- dayViewsMap) {
      println("Got yearday " + yearDay + " with views " + views)
    }
  }

  @Test def testPut() {
    ExampleSchema.ExampleTable
            .put("Chris").value(_.title, "My Life, My Times")
            .put("Joe").value(_.title, "Joe's Life and Times")
            .increment("Chris").value(_.views, 10l)
            .execute()

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
      ExampleSchema.ExampleTable.title(bro) match {
        case Some(title) => println("%nBro: %s; title: %s".format(bro.rowid, title))
        case None => fail("FAILED TO GET TITLE!")
      }
    }
  }
}

