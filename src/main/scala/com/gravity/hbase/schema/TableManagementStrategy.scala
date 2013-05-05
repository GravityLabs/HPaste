package com.gravity.hbase.schema

import org.apache.hadoop.hbase.client.{HTablePool, HTableInterface}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
trait TableManagementStrategy {

  def getTable(htable: HbaseTable[_,_,_]): HTableInterface

  def releaseTable(htable:HbaseTable[_,_,_], table:HTableInterface)

}

trait TablePoolStrategy extends TableManagementStrategy {
  this : HbaseTable[_,_,_] =>
  /** A pool of table objects with AutoFlush set to true */

  var tablePool : HTablePool = new HTablePool(getConf, getTableConfig.tablePoolSize)

  def getTable(htable: HbaseTable[_,_,_]): HTableInterface = {
    tablePool.getTable(htable.tableName)
  }

  def releaseTable(htable: HbaseTable[_,_,_], table: HTableInterface) {
    table.close()
  }
}
