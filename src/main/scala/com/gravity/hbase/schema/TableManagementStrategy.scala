package com.gravity.hbase.schema

import com.gravity.utilities.GrvConcurrentMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HConnection, HTable, HTableInterface}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
trait TableManagementStrategy {

  def getTable(htable: HbaseTable[_,_,_], conf: Configuration, timeoutMs: Int): HTableInterface

  def releaseTable(htable:HbaseTable[_,_,_], table:HTableInterface)

}

private object HBaseConnectionHolder {
  private val connections = new GrvConcurrentMap[Configuration, HConnection]()
  def getConnection(conf : Configuration): HConnection = connections.getOrElseUpdate(conf, org.apache.hadoop.hbase.client.HConnectionManager.createConnection(conf))
}

trait TablePoolStrategy extends TableManagementStrategy {
  this : HbaseTable[_,_,_] =>
  /** A pool of table objects with AutoFlush set to true */

  def getTable(htable: HbaseTable[_,_,_], conf: Configuration, timeoutMs: Int): HTableInterface = {
    val table = HBaseConnectionHolder.getConnection(conf).getTable(htable.tableName)
    if(timeoutMs > 0)
      htable.asInstanceOf[HTable].setOperationTimeout(timeoutMs)
    table
  }

  def releaseTable(htable: HbaseTable[_,_,_], table: HTableInterface) {
    table.close()
  }
}
