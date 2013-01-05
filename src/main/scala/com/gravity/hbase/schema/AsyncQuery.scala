package com.gravity.hbase.schema

import collection.mutable.Buffer
import org.apache.hadoop.hbase.filter.FilterList
import org.hbase.async.{GetRequest, HBaseClient}
import collection._
import org.apache.hadoop.conf.Configuration
import scala.collection.JavaConversions._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object AsyncClient {
  private val clients = new mutable.HashMap[String,HBaseClient]() with mutable.SynchronizedMap[String,HBaseClient]

  def client(conf:Configuration) = {
    val quorum = conf.get("hbase.zookeeper.quorum")
    println("USING QUORUM: " + quorum)
    if(quorum == "localhost") clients.getOrElseUpdate(quorum, new HBaseClient("localhost:21818"))
    else clients.getOrElseUpdate(quorum, new HBaseClient(quorum))
  }

}

