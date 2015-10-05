package com.gravity.hbase.schema

import scala.collection.mutable.Buffer
import org.apache.hadoop.io.Writable
import org.apache.hadoop.hbase.client.{Mutation, Row}
import scala.collection.JavaConversions._

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class OpsResult(numDeletes: Int, numPuts: Int, numIncrements: Int)
