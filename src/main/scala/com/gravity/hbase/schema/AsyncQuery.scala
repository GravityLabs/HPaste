package com.gravity.hbase.schema

import org.apache.hadoop.conf.Configuration
import org.hbase.async.HBaseClient

import scala.collection._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object AsyncClient {
  private val clients = new mutable.HashMap[String,HBaseClient]() with mutable.SynchronizedMap[String,HBaseClient]

  def client(conf:Configuration) = {
    val quorum = conf.get("hbase.zookeeper.quorum")
    val clientPort = conf.get("hbase.zookeeper.property.clientPort")
    println("USING QUORUM: " + quorum)
    if(quorum == "localhost") clients.getOrElseUpdate(quorum, new HBaseClient("localhost:" + clientPort))
    else clients.getOrElseUpdate(quorum, new HBaseClient(quorum))
  }

}
