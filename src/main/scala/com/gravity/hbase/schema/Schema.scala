/** Licensed to Gravity.com under one
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
import com.gravity.hbase.AnyNotSupportedException
import org.apache.hadoop.hbase.{HColumnDescriptor, KeyValue}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/** When a query comes back, there are a bucket of column families and columns to retrieve.  This class retrieves them.
  *
  * @tparam T the source [[com.gravity.hbase.schema.HbaseTable]] this result came from
  * @tparam R the `type` of the table's rowid
  *
  * @param result the raw [[org.apache.hadoop.hbase.client.Result]] returned from the `hbase` [[org.apache.hadoop.hbase.client.Get]]
  * @param table the underlying [[com.gravity.hbase.schema.HbaseTable]]
  * @param tableName the name of the actual table
  */
class QueryResult[T <: HbaseTable[T, R, _], R](val result: DeserializedResult, val table: HbaseTable[T, R, _], val tableName: String) extends Serializable {


  /** This is a convenience method to allow consumers to check
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

  /** Extracts and deserializes the value of the `column` specified
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
  def column[F, K, V](column: (T) => Column[T, R, F, K, V]) = {
    val co = column(table.pops)
    val colVal = result.columnValueSpecific(co)
    if (colVal == null) {
      None
    }
    else {
      Some[V](colVal.asInstanceOf[V])
    }
  }

  /** Extracts and deserializes the value of the `family` + `columnName` specified
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
  def columnFromFamily[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], columnName: K) = {
    val fam = family(table.pops)
    val colVal = result.columnValue(fam, columnName.asInstanceOf[AnyRef])
    if (colVal == null) {
      None
    }
    else {
      Some[V](colVal.asInstanceOf[V])
    }
  }

  /** Extracts and deserializes the Timestamp of the `family` + `columnName` specified
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
  def columnFromFamilyTimestamp[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], columnName: K) = {
    val fam = family(table.pops)
    val colVal = result.columnTimestampByNameAsDate(fam, columnName.asInstanceOf[AnyRef])
    if (colVal == null) {
      None
    }
    else {
      Some(colVal)
    }
  }

  /** Extracts column timestamp of the specified `column`
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

  /** Extracts most recent column timestamp of the specified `family`
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

  /** Extracts and deserializes the entire family as a `Map[K, V]`
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

  /** Extracts and deserializes only the keys (qualifiers) of the family as a `Set[K]`
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

  /** The row identifier deserialized as type `R`
    *
    */
  def rowid = result.getRow[R]()

  def getTableName = tableName
}

/**
  * A query for setting up a scanner across the whole table or key subsets.
  * There is a lot of room for expansion in this class -- caching parameters, scanner specs, key-only, etc.
  */


/**
  * An individual data modification operation (put, increment, or delete usually)
  * These operations are chained together by the client, and then executed in bulk.
  */
class OpBase[T <: HbaseTable[T, R, _], R](val table: HbaseTable[T, R, _], key: Array[Byte], previous: Buffer[OpBase[T, R]] = Buffer[OpBase[T, R]]()) {

  previous += this

  def put(key: R, writeToWAL: Boolean = true) = {
    val po = new PutOp(table, table.rowKeyConverter.toBytes(key), previous, writeToWAL)
    po
  }

  def increment(key: R) = {
    val inc = new IncrementOp(table, table.rowKeyConverter.toBytes(key), previous)
    inc
  }

  def delete(key: R) = {
    val del = new DeleteOp(table, table.rowKeyConverter.toBytes(key), previous)
    del
  }

  def size = previous.size

  def getOperations: Iterable[Writable] = {
    val calls = Buffer[Writable]()
    previous.foreach {
      case put: PutOp[T, R] => {
        calls += put.put
      }
      case delete: DeleteOp[T, R] => {
        calls += delete.delete
      }
      case increment: IncrementOp[T, R] => {
        calls += increment.increment
      }
    }

    calls

  }

  /**
    * This is an experimental call that utilizes a shared instance of a table to flush writes.
    */
  def executeBuffered(tableName: String = table.tableName) = {

    val (deletes, puts, increments) = prepareOperations

    table.withBufferedTable(tableName) {
      bufferTable =>
        if (puts.size > 0) {
          bufferTable.put(puts)
        }
        if (deletes.size > 0) {
          bufferTable.delete(deletes)
        }
        if (increments.size > 0) {
          increments.foreach {
            increment =>
              bufferTable.increment(increment)
          }
        }
    }

  }

  def prepareOperations = {
    val puts = Buffer[Put]()
    val deletes = Buffer[Delete]()
    val increments = Buffer[Increment]()

    previous.foreach {
      case put: PutOp[T, R] => {
        puts += put.put
      }
      case delete: DeleteOp[T, R] => {
        deletes += delete.delete
      }
      case increment: IncrementOp[T, R] => {
        increments += increment.increment
      }
    }

    (deletes, puts, increments)
  }

  def execute(tableName: String = table.tableName) = {
    val (deletes, puts, increments) = prepareOperations
    table.withTable(tableName) {
      table =>
        if (puts.size > 0) {
          table.put(puts)
          //IN THEORY, the operations will happen in order.  If not, break this into two different batched calls for deletes and puts
        }
        if (deletes.size > 0) {
          table.delete(deletes)
        }
        if (increments.size > 0) {
          increments.foreach(increment => table.increment(increment))
        }
    }

    OpsResult(0, puts.size, increments.size)
  }
}

case class OpsResult(numDeletes: Int, numPuts: Int, numIncrements: Int)

/**
  * An increment operation -- can increment multiple columns in a single go.
  */
class IncrementOp[T <: HbaseTable[T, R, _], R](table: HbaseTable[T, R, _], key: Array[Byte], previous: Buffer[OpBase[T, R]] = Buffer[OpBase[T, R]]()) extends OpBase[T, R](table, key, previous) {
  val increment = new Increment(key)
  increment.setWriteToWAL(false)

  def value[F, K, Long](column: (T) => Column[T, R, F, K, Long], value: java.lang.Long) = {
    val col = column(table.pops)
    increment.addColumn(col.familyBytes, col.columnBytes, value)
    this
  }

  def valueMap[F, K, Long](family: (T) => ColumnFamily[T, R, F, K, Long], values: Map[K, Long]) = {
    val fam = family(table.pops)
    for ((key, value) <- values) {
      increment.addColumn(fam.familyBytes, fam.keyConverter.toBytes(key), value.asInstanceOf[java.lang.Long])
    }
    this
  }
}

/**
  * A Put operation.  Can work across multiple columns or entire column families treated as Maps.
  */
class PutOp[T <: HbaseTable[T, R, _], R](table: HbaseTable[T, R, _], key: Array[Byte], previous: Buffer[OpBase[T, R]] = Buffer[OpBase[T, R]](), writeToWAL: Boolean = true) extends OpBase[T, R](table, key, previous) {
  val put = new Put(key)
  put.setWriteToWAL(writeToWAL)

  def value[F, K, V](column: (T) => Column[T, R, F, K, V], value: V, timeStamp: DateTime = null) = {
    val col = column(table.asInstanceOf[T])
    if (timeStamp == null) {
      put.add(col.familyBytes, col.columnBytes, col.valueConverter.toBytes(value))
    } else {
      put.add(col.familyBytes, col.columnBytes, timeStamp.getMillis, col.valueConverter.toBytes(value))
    }
    this
  }

  def valueMap[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], values: Map[K, V]) = {
    val fam = family(table.pops)
    for ((key, value) <- values) {
      put.add(fam.familyBytes, fam.keyConverter.toBytes(key), fam.valueConverter.toBytes(value))
    }
    this
  }
}

/**
  * A deletion operation.  If nothing is specified but a key, will delete the whole row.  If a family is specified, will just delete the values in
  * that family.
  */
class DeleteOp[T <: HbaseTable[T, R, _], R](table: HbaseTable[T, R, _], key: Array[Byte], previous: Buffer[OpBase[T, R]] = Buffer[OpBase[T, R]]()) extends OpBase[T, R](table, key, previous) {
  val delete = new Delete(key)

  def family[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V]) = {
    val fam = family(table.pops)
    delete.deleteFamily(fam.familyBytes)
    this
  }

  def values[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], qualifiers: Set[K]) = {
    val fam = family(table.pops)
    for (q <- qualifiers) {
      delete.deleteColumns(fam.familyBytes, fam.keyConverter.toBytes(q))
    }
    this
  }
}

/**
  * A query for retrieving values.  It works somewhat differently than the data modification operations, in that you do the following:
  * 1. Specify one or more keys
  * 2. Specify columns and families to scan in for ALL the specified keys
  *
  * In other words there's no concept of having multiple rows fetched with different columns for each row (that seems to be a rare use-case and
  * would make the API very complex).
  */

trait KeyValueConvertible[F, K, V] {
  val familyConverter: ByteConverter[F]
  val keyConverter: ByteConverter[K]
  val valueConverter: ByteConverter[V]

  def keyToBytes(key:K) = keyConverter.toBytes(key)
  def valueToBytes(value:V) = valueConverter.toBytes(value)

  def keyToBytesUnsafe(key:AnyRef) = keyConverter.toBytes(key.asInstanceOf[K])
  def valueToBytesUnsafe(value:AnyRef) = valueConverter.toBytes(value.asInstanceOf[V])

  def keyFromBytesUnsafe(bytes:Array[Byte]) = keyConverter.fromBytes(bytes).asInstanceOf[AnyRef]
  def valueFromBytesUnsafe(bytes:Array[Byte]) = valueConverter.fromBytes(bytes).asInstanceOf[AnyRef]

  def family: ColumnFamily[_, _, _, _, _]
}

/**
  * Represents the specification of a Column Family
  */
class ColumnFamily[T <: HbaseTable[T, R, _], R, F, K, V](val table: HbaseTable[T, R, _], val familyName: F, val compressed: Boolean = false, val versions: Int = 1, val index: Int,val ttlInSeconds:Int = HColumnDescriptor.DEFAULT_TTL)(implicit c: ByteConverter[F], d: ByteConverter[K], e: ByteConverter[V]) extends KeyValueConvertible[F, K, V] {
  val familyConverter = c
  val keyConverter = d
  val valueConverter = e
  val familyBytes = c.toBytes(familyName)


  def family = this
}

/**
  * Represents the specification of a Column.
  */
class Column[T <: HbaseTable[T, R, _], R, F, K, V](table: HbaseTable[T, R, _], columnFamily: ColumnFamily[T, R, F, K, _], val columnName: K,val columnIndex:Int)(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) extends KeyValueConvertible[F, K, V] {
  val columnBytes = kc.toBytes(columnName)
  val familyBytes = columnFamily.familyBytes
  val columnNameRef = columnName.asInstanceOf[AnyRef]

  val familyConverter = fc
  val keyConverter = kc
  val valueConverter = kv

  def getQualifier: K = columnName

  def family = columnFamily.asInstanceOf[ColumnFamily[_, _, _, _, _]]


}

trait Schema {
  val tables = scala.collection.mutable.Set[HbaseTable[_, _, _]]()

  def table[T <: HbaseTable[T, _, _]](table: T) = {
    tables += table
    table
  }

}


case class DeserializedResult(rowid: AnyRef, famCount: Int) {

  def isEmpty = values.size == 0

  def getRow[R]() = rowid.asInstanceOf[R]


  def familyValueMap[K, V](fam: ColumnFamily[_, _, _, _, _]) = {
    val famMap = family(fam)
    if (famMap != null) {
      famMap.asInstanceOf[java.util.HashMap[K, V]]
    } else {
      new java.util.HashMap[K, V]()
    }
  }

  def familyKeySet[K](fam: ColumnFamily[_, _, _, _, _]) = {
    val famMap = family(fam)
    if (famMap != null) {
      famMap.keySet.asInstanceOf[java.util.Set[K]]
    } else {
      new java.util.HashSet[K]()
    }
  }

  def family(family: ColumnFamily[_, _, _, _, _]) = {
    values(family.index)
  }

  def familyOf(column: Column[_, _, _, _, _]) = family(column.family)

  def familyMap(fam: ColumnFamily[_, _, _, _, _]) = family(fam)

  def hasColumn(column: Column[_, _, _, _, _]) = {
    val valueMap = familyOf(column)
    if (valueMap == null || valueMap.size == 0) false else true
  }

  def columnValue(fam: ColumnFamily[_, _, _, _, _], columnName: AnyRef) = {
    val valueMap = family(fam)
    if (valueMap == null) {
      null
    } else {
      valueMap.get(columnName)
    }
  }

  def columnTimestamp(fam: ColumnFamily[_, _, _, _, _], columnName: AnyRef) = {
    val res = timestampLookaside(fam.index)
    if (res != null) {
      val colRes = res.get(columnName)
      colRes
    }
    else {
      0l
    }
  }


  def columnTimestampAsDate(column: Column[_, _, _, _, _]) = {
    val cts = columnTimestamp(column.family, column.columnNameRef)
    if (cts > 0) {
      new DateTime(cts)
    } else {
      null
    }
  }

  def columnTimestampByName(fam: ColumnFamily[_, _, _, _, _], columnName: AnyRef) = {
    val cts = columnTimestamp(fam, columnName)
    cts
  }

  def columnTimestampByNameAsDate(fam: ColumnFamily[_, _, _, _, _], columnName: AnyRef) = {
    val cts = columnTimestamp(fam, columnName)
    if (cts > 0) {
      new DateTime(cts)
    }
    else {
      null
    }
  }


  def columnValueSpecific(column: Column[_, _, _, _, _]) = {
    columnValue(column.family, column.columnNameRef)
  }


  var values = new Array[java.util.HashMap[AnyRef, AnyRef]](famCount)

  var timestampLookaside = new Array[java.util.HashMap[AnyRef, Long]](famCount)


  /** This is a map whose key is the family type, and whose values are maps of column keys to columnvalues paired with their timestamps */
  //  val values = new java.util.HashMap[ColumnFamily[_, _, _, _, _], java.util.HashMap[AnyRef, AnyRef]]()

  //  val timestampLookaside = new java.util.HashMap[ColumnFamily[_, _, _, _, _], java.util.HashMap[AnyRef, Long]]()

  def add(family: ColumnFamily[_, _, _, _, _], qualifier: AnyRef, value: AnyRef, timeStamp: Long) {
    var map = values(family.index)
    if (map == null) {
      map = new java.util.HashMap[AnyRef, AnyRef]()
      values(family.index) = map
    }
    map.put(qualifier, value)

    var tsMap = timestampLookaside(family.index)
    if (tsMap == null) {
      tsMap = new java.util.HashMap[AnyRef, Long]()
      timestampLookaside(family.index) = tsMap
    }
    tsMap.put(qualifier, timeStamp)
    //Add timestamp lookaside
  }

  var errorBuffer: Buffer[(Array[Byte], Array[Byte], Array[Byte], Long)] = _

  def addErrorBuffer(family: Array[Byte], qualifier: Array[Byte], value: Array[Byte], timestamp: Long) {
    if (errorBuffer == null) {
      errorBuffer = Buffer()
    }
    errorBuffer.append((family, qualifier, value, timestamp))
  }

  def hasErrors = (errorBuffer != null)
}

/** Standard base class for all Row objects.
  *
  * Inside of a *Row object, it is good to use lazy val and def as opposed to val.
  * Because HRow objects are now the first-class instantiation of a query result, and because they are the type cached in Ehcache, they are good places to cache values.
  */
abstract class HRow[T <: HbaseTable[T, R, _], R](result: DeserializedResult, table: HbaseTable[T, R, _]) extends QueryResult[T, R](result, table, table.tableName) {

  def prettyPrint() {println(prettyFormat())}

  def prettyPrintNoValues() {println(prettyFormatNoValues())}

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


/**
  * Represents a Table.  Expects an instance of HBaseConfiguration to be present.
  * A parameter-type T should be the actual table that is implementing this one (this is to allow syntactic sugar for easily specifying columns during
  * queries).
  * A parameter-type R should be the type of the key for the table.
  */
abstract class HbaseTable[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](val tableName: String, var cache: QueryResultCache[T, R, RR] = new NoOpCache[T, R, RR](), rowKeyClass: Class[R])(implicit conf: Configuration, keyConverter: ByteConverter[R]) {

  def rowBuilder(result: DeserializedResult): RR

  val rowKeyConverter = keyConverter
  /** Provides the client with an instance of the superclass this table was defined against. */
  def pops = this.asInstanceOf[T]

  /** A method injected by the super class that will build a strongly-typed row object.  */
  def buildRow(result: Result): RR = {
    rowBuilder(convertResult(result))
  }

  /** A pool of table objects with AutoFlush set to true */
  val tablePool = new HTablePool(conf, 50)

  /** A pool of table objects with AutoFlush set to false --therefore usable for asynchronous write buffering */
  val bufferTablePool = new HTablePool(conf, 1, new HTableInterfaceFactory {
    def createHTableInterface(config: Configuration, tableName: Array[Byte]): HTableInterface = {
      val table = new HTable(conf, tableName)
      table.setWriteBufferSize(2000000L)
      table.setAutoFlush(false)
      table
    }

    def releaseHTableInterface(table: HTableInterface) {
      try {
        table.close()
      } catch {
        case ex: IOException => throw new RuntimeException(ex)
      }
    }
  })


  var famLookup: Array[Array[Byte]] = null
  var colFamLookup: Array[Array[Byte]] = null
  var famIdx: IndexedSeq[KeyValueConvertible[_, _, _]] = null
  var colFamIdx: IndexedSeq[KeyValueConvertible[_, _, _]] = null

  val bc = new ByteArrayComparator()

  implicit val o = new math.Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = {
      bc.compare(a, b)
    }
  }

  /** Looks up a KeyValueConvertible by the family and column bytes provided.
    * Because of the rules of the system, the lookup goes as follows:
    * 1. Find a column first.  If you find a column first, it means there is a strongly-typed column defined.
    * 2. If no column, then find the family.
    *
    */
  def converterByBytes(famBytes: Array[Byte], colBytes: Array[Byte]): KeyValueConvertible[_, _, _] = {

    if (famLookup == null) {
      famLookup = Array.ofDim[Array[Byte]](families.size)
      for ((fam, idx) <- families.zipWithIndex) {
        famLookup(idx) = fam.familyBytes
      }
      Arrays.sort(famLookup, bc)
      famIdx = families.sortBy(_.familyBytes).toIndexedSeq
    }

    if (colFamLookup == null) {
      colFamLookup = Array.ofDim[Array[Byte]](columns.size)


      for ((col, idx) <- columns.zipWithIndex) {
        colFamLookup(idx) = ArrayUtils.addAll(col.familyBytes, col.columnBytes)
      }
      Arrays.sort(colFamLookup, bc)
      colFamIdx = columns.sortBy(col => ArrayUtils.addAll(col.familyBytes, col.columnBytes)).toIndexedSeq
    }

    val fullKey = ArrayUtils.addAll(famBytes, colBytes)
    val resIdx = Arrays.binarySearch(colFamLookup, fullKey, bc)
    if (resIdx > -1) {
      colFamIdx(resIdx)
    } else {
      val resFamIdx = Arrays.binarySearch(famLookup, famBytes, bc)
      if (resFamIdx > -1) {
        famIdx(resFamIdx)
      }
      else {
        throw new RuntimeException("Unable to locate family or column definition")
      }
    }


  }

  /** Converts a result to a DeserializedObject. A conservative implementation that is slower than convertResultRaw but will always be more stable against
    * binary changes to Hbase's KeyValue format.
    */
  def convertResult(result: Result) = {
    if (result.isEmpty) {
      throw new RuntimeException("Attempting to deserialize an empty result.  If you want to handle the eventuality of an empty result, call singleOption() instead of single()")
    }
    val keyValues = result.raw()
    val buff = result.getBytes.get()

    val rowId = keyConverter.fromBytes(buff, keyValues(0).getRowOffset, keyValues(0).getRowLength).asInstanceOf[AnyRef]

    val ds = DeserializedResult(rowId, families.size)

    var itr = 0

    while (itr < keyValues.length) {
      val kv = keyValues(itr)
      val family = kv.getFamily
      val key = kv.getQualifier
      try {
        val c = converterByBytes(family, key)
        if(!c.isInstanceOf[ByteConverter[Any]]) {
          val f = c.family
          val k = c.keyConverter.fromBytes(buff, kv.getQualifierOffset, kv.getQualifierLength).asInstanceOf[AnyRef]
          val r = c.valueConverter.fromBytes(buff, kv.getValueOffset, kv.getValueLength).asInstanceOf[AnyRef]
          val ts = kv.getTimestamp

          ds.add(f, k, r, ts)
        }else {
          //TODO: Just like AnyNotSupportException, add a counter here because this means a column was removed, but the data is still in the database.
        }
      } catch {
        case ex: AnyNotSupportedException => {
          //This means a column came back that is no longer part of the specification
          //TODO: Keep counters of columns that were encountered and we were unable to deserialize
//          println("Attempted to lookup column: " + new String(key) + " in family: " + new String(family) + " and didn't find a serializer")
//          ds.addErrorBuffer(family, key, value, kv.getTimestamp)
        }
        case ex: Exception => {
          //          println(ex.getMessage)
          //          println(ex.getStackTraceString)
  //        ds.addErrorBuffer(family, key, value, kv.getTimestamp)
        }
      } finally {
        itr = itr + 1
      }

    }
    ds
  }

  /** Converts a result to a DeserializedObject, avoiding the Result object's inner parsing by moving it out and directly into the deserializers.
    *
    */
  def convertResultRaw(result: Result) = {


    val bytes = result.getBytes()
    val buf = bytes.get()
    var offset = bytes.getOffset
    val finalOffset = bytes.getSize + offset
    var row: Array[Byte] = null
    var ds: DeserializedResult = null

    while (offset < finalOffset) {
      val keyLength = Bytes.toInt(buf, offset)
      offset = offset + Bytes.SIZEOF_INT

      val keyOffset = offset + KeyValue.ROW_OFFSET
      val rowLength = Bytes.toShort(buf, keyOffset)
      val familyOffset = offset + KeyValue.ROW_OFFSET + Bytes.SIZEOF_SHORT + rowLength + Bytes.SIZEOF_BYTE
      val familyLength = buf(familyOffset - 1)
      val family = new Array[Byte](familyLength)
      System.arraycopy(buf, familyOffset, family, 0, familyLength)

      val qualifierOffset = familyOffset + familyLength
      val qualifierLength = keyLength - (KeyValue.KEY_INFRASTRUCTURE_SIZE + rowLength + familyLength)
      val key = new Array[Byte](qualifierLength)
      System.arraycopy(buf, qualifierOffset, key, 0, qualifierLength)

      val valueOffset = keyOffset + keyLength
      val valueLength = Bytes.toInt(buf, offset + Bytes.SIZEOF_INT)
      val value = new Array[Byte](valueLength)
      System.arraycopy(buf, valueOffset, value, 0, valueLength)

      val tsOffset = keyOffset + keyLength - KeyValue.TIMESTAMP_TYPE_SIZE
      val ts = Bytes.toLong(buf, tsOffset)

      if (row == null) {
        val rowOffset = keyOffset + Bytes.SIZEOF_SHORT
        row = new Array[Byte](rowLength)
        System.arraycopy(buf, rowOffset, row, 0, rowLength)
        val rowId = keyConverter.fromBytes(result.getRow).asInstanceOf[AnyRef]
        ds = DeserializedResult(rowId, families.size)
      }

      try {
        val c = converterByBytes(family, key)
        val f = c.family
        val k = c.keyConverter.fromBytes(key).asInstanceOf[AnyRef]
        val r = c.valueConverter.fromBytes(value).asInstanceOf[AnyRef]
        println("Adding value " + r)
        ds.add(f, k, r, ts)
      } catch {
        case ex: Exception => {
          println("Adding error buffer")
          ds.addErrorBuffer(family, key, value, ts)
        }
      }

      offset = offset + keyLength
    }
    ds
  }


  def familyBytes = families.map(family => family.familyBytes)

  def familyByIndex(idx: Int) = familyArray(idx)

  lazy val familyArray = {
    val arr = new Array[ColumnFamily[_, _, _, _, _]](families.length)
    families.foreach {
      fam =>
        arr(fam.index) = fam
    }
    arr
  }

  def columnByIndex(idx:Int) = columnArray(idx)
  lazy val columnArray = {
    val arr = new Array[Column[_,_,_,_,_]](columns.length)
    columns.foreach{col=>arr(col.columnIndex)=col}
    arr
  }


  //alter 'articles', NAME => 'html', VERSIONS =>1, COMPRESSION=>'lzo'

  /*
  WARNING - Currently assumes the family names are strings (which is probably a best practice, but we support byte families)
   */
  def createScript(tableNameOverride: String = tableName) = {
    val create = "create '" + tableNameOverride + "', "
    create + (for (family <- families) yield {
      familyDef(family)
    }).mkString(",")
  }

  def deleteScript(tableNameOverride: String = tableName) = {
    val delete = "disable '" + tableNameOverride + "'\n"

    delete + "delete '" + tableNameOverride + "'"
  }

  def alterScript(tableNameOverride: String = tableName, families: Seq[ColumnFamily[T, _, _, _, _]] = families) = {

    var alter = "flush '" + tableNameOverride + "'\n"
    alter += "disable '" + tableNameOverride + "'\n"
    alter += "alter '" + tableNameOverride + "', "
    alter += (for (family <- families) yield {
      familyDef(family)
    }).mkString(",")
    alter += "\nenable '" + tableNameOverride + "'"
    alter
  }

  def familyDef(family: ColumnFamily[T, _, _, _, _]) = {
    val compression = if (family.compressed) ", COMPRESSION=>'lzo'" else ""
    val ttl = if(family.ttlInSeconds < HColumnDescriptor.DEFAULT_TTL) ", TTL=>'"+family.ttlInSeconds+"'" else ""
    "{NAME => '%s', VERSIONS => %d%s%s}".format(Bytes.toString(family.familyBytes), family.versions, compression,ttl)
  }


  def getTable(name: String) = tablePool.getTable(name)

  def getBufferedTable(name: String) = bufferTablePool.getTable(name)

  private val columns = ArrayBuffer[Column[T, R, _, _, _]]()
  val families = ArrayBuffer[ColumnFamily[T, R, _, _, _]]()

  val columnsByName = mutable.Map[AnyRef,Column[T,R,_,_,_]]()

  private val columnsByBytes = mutable.Map[ByteBuffer, KeyValueConvertible[_, _, _]]()
  private val familiesByBytes = mutable.Map[ByteBuffer, KeyValueConvertible[_, _, _]]()

  var columnIdx = 0
  def column[F, K, V](columnFamily: ColumnFamily[T, R, F, K, _], columnName: K, valueClass: Class[V])(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) = {
    val c = new Column[T, R, F, K, V](this, columnFamily, columnName,columnIdx)
    columns += c

    val famBytes = columnFamily.familyBytes
    val colBytes = c.columnBytes
    val fullKey = ArrayUtils.addAll(famBytes, colBytes)
    val bufferKey = ByteBuffer.wrap(fullKey)

    columnsByName.put(columnName.asInstanceOf[AnyRef], c)
    columnsByBytes.put(bufferKey, c)
    columnIdx = columnIdx + 1
    c
  }

  var familyIdx = 0

  def family[F, K, V](familyName: F, compressed: Boolean = false, versions: Int = 1, rowTtlInSeconds:Int=Int.MaxValue)(implicit c: ByteConverter[F], d: ByteConverter[K], e: ByteConverter[V]) = {
    val family = new ColumnFamily[T, R, F, K, V](this, familyName, compressed, versions, familyIdx,rowTtlInSeconds)
    familyIdx = familyIdx + 1
    families += family
    familiesByBytes.put(ByteBuffer.wrap(family.familyBytes), family)
    family
  }

  def getTableOption(name: String) = {
    try {
      Some(getTable(name))
    } catch {
      case e: Exception => None
    }
  }


  def withTableOption[Q](name: String)(work: (Option[HTableInterface]) => Q): Q = {
    val table = getTableOption(name)
    try {
      work(table)
    } finally {
      table foreach (tbl => tablePool.putTable(tbl))
    }
  }

  def withBufferedTable[Q](mytableName: String = tableName)(work: (HTableInterface) => Q): Q = {
    val table = getBufferedTable(mytableName)
    try {
      work(table)
    } finally {
      bufferTablePool.putTable(table)
    }
  }

  def withTable[Q](mytableName: String = tableName)(funct: (HTableInterface) => Q): Q = {
    withTableOption(mytableName) {
      case Some(table) => {
        funct(table)
      }
      case None => throw new RuntimeException("Table " + tableName + " does not exist")
    }
  }

  def scan = new ScanQuery(this)

  def query = new Query(this)

  def query2 = new Query2(this)

  def put(key: R, writeToWAL: Boolean = true) = new PutOp(this, keyConverter.toBytes(key))

  def delete(key: R) = new DeleteOp(this, keyConverter.toBytes(key))

  def increment(key: R) = new IncrementOp(this, keyConverter.toBytes(key))

  /** All tables automatically receive a family called "meta" */
  val meta = family[String, String, Any]("meta")

}

case class YearDay(year: Int, day: Int)

case class CommaSet(items: Set[String]) {
  def mkString: String = items.mkString

  def mkString(sep: String): String = items.mkString(sep)

  def mkString(start: String, sep: String, end: String): String = items.mkString(start, sep, end)
}

object CommaSet {
  val empty = CommaSet(Set.empty[String])

  def apply(items: String*): CommaSet = CommaSet(items.toSet)
}

