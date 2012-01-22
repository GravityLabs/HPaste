package com.gravity.hbase.schema

import junit.framework.TestCase
import org.junit.Test
import org.joda.time.DateTime

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class WebCrawlSchemaTest extends TestCase {

  @Test def testPuttingAndGettingPages() {
    ExampleSchema.WebTable
            .put("http://mycrawledsite.com/crawledpage.html")
            .value(_.title, "My Crawled Page Title")
            .value(_.lastCrawled, new DateTime())
            .value(_.article, "Jonsie went to the store.  She didn't notice the spinning of the Earth, nor did the Earth notice the expansion of the Universe.")
            .value(_.attributes, Map("foo" -> "bar", "custom" -> "data"))
            .execute()


    ExampleSchema.WebTable.query2.withKey("http://mycrawledsite.com/crawledpage.html")
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