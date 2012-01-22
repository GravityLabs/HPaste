package com.gravity.hbase.schema

import com.gravity.hbase.mapreduce._
import java.lang.String
import org.junit.Test
import org.joda.time.{DateMidnight, DateTime}
import com.gravity.hbase.mapreduce.{HMapReduceTask, HJob}
import java.net.URL
import com.gravity.hbase.schema._
import scala.collection._
import com.gravity.hbase.schema.WebCrawlingSchema.WebPageRow

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object WebCrawlingSchema extends Schema {


  implicit val conf = LocalCluster.getTestConfiguration

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

  class WebPageRow(table: WebTable, result: DeserializedResult) extends HRow[WebTable, String](result, table) {
    def domain = new URL(rowid).getAuthority
  }

  val WebTable = table(new WebTable)


  class SiteMetricsTable extends HbaseTable[SiteMetricsTable, String, SiteMetricsRow](tableName = "site-metrics", rowKeyClass = classOf[String]) {
    def rowBuilder(result: DeserializedResult) = new SiteMetricsRow(this, result)

    val meta = family[String, String, Any]("meta")
    val name = column(meta, "name", classOf[String])

    val searchMetrics = family[String, DateMidnight, Long]("searchesByDay")
  }

  class SiteMetricsRow(table: SiteMetricsTable, result: DeserializedResult) extends HRow[SiteMetricsTable, String](result, table)

  val Sites = table(new SiteMetricsTable)

}

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

class WebCrawlSchemaTest extends HPasteTestCase(WebCrawlingSchema) {

  @Test def testWebTablePutsAndGets() {
    WebCrawlingSchema.WebTable
            .put("http://mycrawledsite.com/crawledpage.html")
            .value(_.title, "My Crawled Page Title")
            .value(_.lastCrawled, new DateTime())
            .value(_.article, "Jonsie went to the store.  She didn't notice the spinning of the Earth, nor did the Earth notice the expansion of the Universe.")
            .value(_.attributes, Map("foo" -> "bar", "custom" -> "data"))
            .valueMap(_.searchMetrics, Map(new DateMidnight(2011, 6, 5) -> 3l, new DateMidnight(2011, 6, 4) -> 34l))
            .execute()


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
  }


  @Test def testAggregationMRJob() {
    WebCrawlingSchema.WebTable
            .put("http://mycrawledsite.com/crawledpage2.html")
            .value(_.title, "My Crawled Page Title")
            .value(_.lastCrawled, new DateTime())
            .value(_.article, "Jonsie went to the store.  She didn't notice the spinning of the Earth, nor did the Earth notice the expansion of the Universe.")
            .value(_.attributes, Map("foo" -> "bar", "custom" -> "data"))
            .valueMap(_.searchMetrics, Map(new DateMidnight(2011, 6, 5) -> 3l, new DateMidnight(2011, 6, 4) -> 34l))
            .put("http://mycrawledsite.com/crawledpage3.html")
            .value(_.title, "My Crawled Page Title")
            .value(_.lastCrawled, new DateTime())
            .value(_.article, "Jonsie went to the store.  She didn't notice the spinning of the Earth, nor did the Earth notice the expansion of the Universe.")
            .value(_.attributes, Map("foo" -> "bar", "custom" -> "data"))
            .valueMap(_.searchMetrics, Map(new DateMidnight(2011, 6, 5) -> 3l, new DateMidnight(2011, 6, 4) -> 34l))
            .put("http://mycrawledsite.com/crawledpage4.html")
            .value(_.title, "My Crawled Page Title")
            .value(_.lastCrawled, new DateTime())
            .value(_.article, "Jonsie went to the store.  She didn't notice the spinning of the Earth, nor did the Earth notice the expansion of the Universe.")
            .value(_.attributes, Map("foo" -> "bar", "custom" -> "data"))
            .valueMap(_.searchMetrics, Map(new DateMidnight(2011, 6, 5) -> 3l, new DateMidnight(2011, 6, 4) -> 34l))
            .execute()


    new WebSearchAggregationJob().run(Settings.None, LocalCluster.getTestConfiguration)

    WebCrawlingSchema.Sites.query2.withKey("mycrawledsite.com").singleOption() match {
      case Some(siteRow) => {
        siteRow.family(_.searchMetrics).foreach{println}
      }
      case None => {
        println("Didn't find the site, strange!")
      }
    }
  }

  @Test def testPagesBySiteJob() {
    WebCrawlingSchema.WebTable
                .put("http://mycrawledsite2.com/crawledpage2.html")
                .value(_.title, "My Crawled Page Title2")
                .valueMap(_.searchMetrics, Map(new DateMidnight(2011, 6, 5) -> 3l, new DateMidnight(2011, 6, 4) -> 34l))
                .put("http://mycrawledsite2.com/crawledpage3.html")
                .value(_.title, "My Crawled Page Title3")
                .valueMap(_.searchMetrics, Map(new DateMidnight(2011, 6, 5) -> 3l, new DateMidnight(2011, 6, 4) -> 34l))
                .put("http://mycrawledsite3.com/crawledpage4.html")
                .value(_.title, "My Crawled Page Title4")
                .valueMap(_.searchMetrics, Map(new DateMidnight(2011, 6, 5) -> 3l, new DateMidnight(2011, 6, 4) -> 34l))
                .put("http://mycrawledsite3.com/crawledpage4.html")
                .value(_.title, "My Crawled Page Title5")
                .valueMap(_.searchMetrics, Map(new DateMidnight(2011, 6, 5) -> 3l, new DateMidnight(2011, 6, 4) -> 34l))
                .put("http://mycrawledsite3.com/crawledpage4.html")
                .value(_.title, "My Crawled Page Title6")
                .valueMap(_.searchMetrics, Map(new DateMidnight(2011, 6, 5) -> 3l, new DateMidnight(2011, 6, 4) -> 34l))
                .execute()

    new WebTablePagesBySiteJob().run(Settings.None, LocalCluster.getTestConfiguration)
  }

}