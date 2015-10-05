package com.gravity.hbase.schema

import scala.collection.mutable.Buffer
import org.apache.hadoop.hbase.client.Increment
import scala.collection.Map

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * An increment operation -- can increment multiple columns in a single go.
 * @param table
 * @param key
 * @param previous
 * @tparam T
 * @tparam R
 */
class IncrementOp[T <: HbaseTable[T, R, _], R](table: HbaseTable[T, R, _], key: Array[Byte], previous: Buffer[OpBase[T, R]] = Buffer[OpBase[T, R]]()) extends OpBase[T, R](table, key, previous) {
  val increment: Increment = new Increment(key)
  increment.setWriteToWAL(false)

  def +(that: OpBase[T, R]): IncrementOp[T, R] = new IncrementOp(table,key, previous ++ that.previous)


  def value[F, K, Long](column: (T) => Column[T, R, F, K, Long], value: java.lang.Long): IncrementOp[T, R] = {
    val col = column(table.pops)
    increment.addColumn(col.familyBytes, col.columnBytes, value)
    this
  }

  def valueMap[F, K, Long](family: (T) => ColumnFamily[T, R, F, K, Long], values: Map[K, Long]): IncrementOp[T, R] = {
    val fam = family(table.pops)
    for ((key, value) <- values) {
      increment.addColumn(fam.familyBytes, fam.keyConverter.toBytes(key), value.asInstanceOf[java.lang.Long])
    }
    this
  }
}
