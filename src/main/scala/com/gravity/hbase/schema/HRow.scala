package com.gravity.hbase.schema

import scala.collection.JavaConversions._

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/**
 * Standard base class for all Row objects.
 * Inside of a *Row object, it is good to use lazy val and def as opposed to val.
 * Because HRow objects are now the first-class instantiation of a query result, and because
 * they are the type cached in Ehcache, they are good places to cache values.
 * @param result the raw [[org.apache.hadoop.hbase.client.Result]] returned from the `hbase` [[org.apache.hadoop.hbase.client.Get]]
 * @param table the underlying [[com.gravity.hbase.schema.HbaseTable]]
 * @tparam T the source [[com.gravity.hbase.schema.HbaseTable]] this result came from
 * @tparam R the `type` of the table's rowid
 *
 */
abstract class HRow[T <: HbaseTable[T, R, _], R](result: DeserializedResult, table: HbaseTable[T, R, _]) extends QueryResult[T, R](result, table, table.tableName) {

  def prettyPrint() {println(prettyFormat())}

  def prettyPrintNoValues() {println(prettyFormatNoValues())}

  def size = {
    var _size = 0
    for (i <- 0 until result.values.length) {
      val familyMap = result.values(i)
      if (familyMap != null) {
        _size += familyMap.values.size
      }
    }
    _size
  }

  def prettyFormatNoValues() = {
    val sb = new StringBuilder()
    sb.append("Row Key: " + result.rowid + " (" + result.values.size + " families)" + "\n")
    for (i <- 0 until result.values.length) {
      val familyMap = result.values(i)
      if (familyMap != null) {
        val family = table.familyByIndex(i)
        sb.append("\tFamily: " + family.familyName + " (" + familyMap.values.size + " items)\n")
      }
    }
    sb.toString
  }


  def prettyFormat() = {
    val sb = new StringBuilder()
    sb.append("Row Key: " + result.rowid + " (" + result.values.size + " families)" + "\n")
    for (i <- 0 until result.values.length) {
      val familyMap = result.values(i)
      if (familyMap != null) {
        val family = table.familyByIndex(i)
        sb.append("\tFamily: " + family.familyName + " (" + familyMap.values.size + " items)\n")
        for ((key, value) <- familyMap) {
          sb.append("\t\tColumn: " + key + "\n")
          sb.append("\t\t\tValue: " + value + "\n")
          sb.append("\t\t\tTimestamp: " + result.columnTimestampByNameAsDate(family, key) + "\n")
        }

      }
    }
    sb.toString
  }
}
