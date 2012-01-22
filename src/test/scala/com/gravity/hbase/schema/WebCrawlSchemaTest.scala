package com.gravity.hbase.schema

import org.junit.Test
import org.joda.time.{DateMidnight, DateTime}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object WebCrawlingSchema extends Schema {

  import com.gravity.hbase.schema._
  import scala.collection._

  implicit val conf = LocalCluster.getTestConfiguration

  class WebTable extends HbaseTable[WebTable, String, WebPageRow](tableName = "pages", rowKeyClass = classOf[String]) {
    def rowBuilder(result: DeserializedResult) = new WebPageRow(this, result)

    val meta = family[String, String, Any]("meta")
    val title = column(meta, "title", classOf[String])
    val lastCrawled = column(meta, "lastCrawled", classOf[DateTime])

    val content = family[String, String, Any]("text", compressed = true)
    val article = column(content, "article", classOf[String])
    val attributes = column(content, "attrs", classOf[Map[String, String]])

    val viewMetrics = family[String, DateMidnight, Long]("viewsByDay")


  }

  class WebPageRow(table: WebTable, result: DeserializedResult) extends HRow[WebTable, String](result, table)

  val WebTable = table(new WebTable)


  class SiteMetricsTable extends HbaseTable[SiteMetricsTable,String,SiteMetricsRow](tableName="site-metrics",rowKeyClass=classOf[String]) {
    def rowBuilder(result:DeserializedResult) = new SiteMetricsRow(this,result)

    val meta = family[String,String,Any]("meta")
    val name = column(meta,"name",classOf[String])

    val viewMetrics = family[String,DateMidnight,Long]("viewsByDay")
  }
  class SiteMetricsRow(table: SiteMetricsTable, result: DeserializedResult) extends HRow[SiteMetricsTable, String](result, table)
  val Sites = table(new SiteMetricsTable)

}

class WebCrawlSchemaTest extends HPasteTestCase(WebCrawlingSchema) {

  @Test def testWebTablePutsAndGets() {
    WebCrawlingSchema.WebTable
            .put("http://mycrawledsite.com/crawledpage.html")
            .value(_.title, "My Crawled Page Title")
            .value(_.lastCrawled, new DateTime())
            .value(_.article, "Jonsie went to the store.  She didn't notice the spinning of the Earth, nor did the Earth notice the expansion of the Universe.")
            .value(_.attributes, Map("foo" -> "bar", "custom" -> "data"))
            .valueMap(_.viewMetrics, Map(new DateMidnight(2011, 6, 5) -> 3l, new DateMidnight(2011, 6, 4) -> 34l))
            .execute()


    WebCrawlingSchema.WebTable.query2.withKey("http://mycrawledsite.com/crawledpage.html")
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
  }
}