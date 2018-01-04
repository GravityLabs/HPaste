/**Licensed to Gravity.com under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Gravity.com licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gravity.hbase.schema

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import java.io._
import org.apache.hadoop.io.Writable
import scala.collection._
import mutable.{ArrayBuffer, Buffer}
import org.joda.time.DateTime
import com.gravity.hbase.schema._
import java.math.BigInteger
import java.nio.ByteBuffer
import org.apache.commons.lang.ArrayUtils
import java.util.{Arrays, HashMap}
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator
import org.apache.hadoop.hbase.{HColumnDescriptor, KeyValue}
import com.gravity.hbase.{AnyConverterSignal, AnyNotSupportedException}
import org.hbase.async.HBaseClient

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/**When a query comes back, there are a bucket of column families and columns to retrieve.  This class retrieves them.
 *
 * @tparam T the source [[com.gravity.hbase.schema.HbaseTable]] this result came from
 * @tparam R the `type` of the table's rowid
 *
 * @param result the raw [[org.apache.hadoop.hbase.client.Result]] returned from the `hbase` [[org.apache.hadoop.hbase.client.Get]]
 * @param table the underlying [[com.gravity.hbase.schema.HbaseTable]]
 * @param tableName the name of the actual table
 */
class QueryResult[T <: HbaseTable[T, R, _], R](val result: DeserializedResult, val table: HbaseTable[T, R, _], val tableName: String) extends Serializable {


  /**This is a convenience method to allow consumers to check
   * if a column has a value present in the result without
   * invoking the deserialization of the value
   *
   * @tparam F the type of the column family name
   * @tparam K the type of the column family qualifier
   * @tparam V the type of the column family value
   *
   * @param column the underlying table's column `val`
   *
   * @return `true` if the column value is present and otherwise `false`
   */
  def isColumnPresent[F, K, V](column: (T) => Column[T, R, F, K, V]): Boolean = {
    val co = column(table.pops)
    result.hasColumn(co)
  }

  /**Extracts and deserializes the value of the `column` specified
   *
   * @tparam F the type of the column family name
   * @tparam K the type of the column family qualifier
   * @tparam V the type of the column family value
   *
   * @param column the underlying table's column `val`
   *
   * @return `Some` value of type `V` if the column value is present, otherwise `None`
   *
   * @note if there is no explicitly defined `val` for the desired column, use `columnFromFamily`
   */
  def column[F, K, V](column: (T) => Column[T, R, F, K, V]): Option[V] = {
    val co = column(table.pops)
    val colVal = result.columnValueSpecific(co)
    if (colVal == null) {
      None
    }
    else {
      Some[V](colVal.asInstanceOf[V])
    }
  }

  /**Extracts and deserializes the value of the `family` + `columnName` specified
   *
   * @tparam F the type of the column family name
   * @tparam K the type of the column family qualifier
   * @tparam V the type of the column family value
   *
   * @param family the underlying table's family `val`
   * @param columnName value of the desired column's qualifier
   *
   * @return `Some` value of type `V` if the column value is present, otherwise `None`
   */
  def columnFromFamily[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], columnName: K): Option[V] = {
    val fam = family(table.pops)
    val colVal = result.columnValue(fam, columnName.asInstanceOf[AnyRef])
    if (colVal == null) {
      None
    }
    else {
      Some[V](colVal.asInstanceOf[V])
    }
  }

  /**Extracts and deserializes the Timestamp of the `family` + `columnName` specified
   *
   * @tparam F the type of the column family name
   * @tparam K the type of the column family qualifier
   * @tparam V the type of the column family value
   *
   * @param family the underlying table's family `val`
   * @param columnName value of the desired column's qualifier
   *
   * @return `Some` [[org.joda.time.DateTime]] if the column value is present, otherwise `None`
   */
  def columnFromFamilyTimestamp[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], columnName: K): Option[DateTime] = {
    val fam = family(table.pops)
    val colVal = result.columnTimestampByNameAsDate(fam, columnName.asInstanceOf[AnyRef])
    if (colVal == null) {
      None
    }
    else {
      Some(colVal)
    }
  }

  /**Extracts column timestamp of the specified `column`
   *
   * @tparam F the type of the column family name
   * @tparam K the type of the column family qualifier
   * @tparam V the type of the column family value
   *
   * @param column the underlying table's column `val`
   *
   * @return `Some` [[org.joda.time.DateTime]] if the column value is present, otherwise `None`
   */
  def columnTimestamp[F, K, V](column: (T) => Column[T, R, F, K, V]): Option[DateTime] = {
    val co = column(table.pops)
    val res = result.columnTimestampAsDate(co)
    if (res == null) {
      None
    }
    else {
      Some(res)
    }
  }

  /**Extracts most recent column timestamp of the specified `family`
   *
   * @tparam F the type of the column family name
   * @tparam K the type of the column family qualifier
   * @tparam V the type of the column family value
   *
   * @param family the underlying table's family `val`
   *
   * @return `Some` [[org.joda.time.DateTime]] if at least one column value is present, otherwise `None`
   */
  def familyLatestTimestamp[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V]): Option[DateTime] = {
    val fam = family(table.pops)
    val familyPairs = result.familyMap(fam)
    if (familyPairs != null) {
      var ts = -1l
      for (kv <- familyPairs) {
        val tsn = result.columnTimestampByName(fam, kv._1)
        if (tsn > ts) ts = tsn
      }
      if (ts > 0) {
        Some(new DateTime(ts))
      }
      else {
        None
      }

    } else {
      None
    }
  }

  /**Extracts and deserializes the entire family as a `Map[K, V]`
   *
   * @tparam F the type of the column family name
   * @tparam K the type of the column family qualifier
   * @tparam V the type of the column family value
   *
   * @param family the underlying table's family `val`
   *
   */
  def family[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V]): Map[K, V] = {
    val fm = family(table.pops)
    result.familyValueMap[K, V](fm)

  }

  /**Extracts and deserializes only the keys (qualifiers) of the family as a `Set[K]`
   *
   * @tparam F the type of the column family name
   * @tparam K the type of the column family qualifier
   *
   * @param family the underlying table's family `val`
   *
   */
  def familyKeySet[F, K](family: (T) => ColumnFamily[T, R, F, K, _]): Set[K] = {
    val fm = family(table.pops)
    result.familyKeySet[K](fm)
  }

  /**The row identifier deserialized as type `R`
   *
   */
  def rowid: R = result.getRow[R]()

  def getTableName: String = tableName
}

/**
 * A query for setting up a scanner across the whole table or key subsets.
 * There is a lot of room for expansion in this class -- caching parameters, scanner specs, key-only, etc.
 */














trait Schema {
  val tables: mutable.Set[HbaseTable[_, _, _]] = scala.collection.mutable.Set[HbaseTable[_, _, _]]()

  def table[T <: HbaseTable[T, _, _]](table: T): T = {
    tables += table
    table.init()
    table
  }


}

case class YearDay(year: Int, day: Int)

case class CommaSet(items: Set[String]) {
  def mkString: String = items.mkString

  def mkString(sep: String): String = items.mkString(sep)

  def mkString(start: String, sep: String, end: String): String = items.mkString(start, sep, end)
}

object CommaSet {
  val empty: CommaSet = CommaSet(Set.empty[String])

  def apply(items: String*): CommaSet = CommaSet(items.toSet)
}

