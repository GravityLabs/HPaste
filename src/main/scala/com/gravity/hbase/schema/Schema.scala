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
import mutable.{ListBuffer, SynchronizedMap, HashMap, Buffer}
import org.joda.time.DateTime
import com.gravity.hbase.schema._
import com.google.common.collect._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/** It is expensive to deserialize column values in tight loops.  This construct provides temporary caches for such a scenario.  This introduces its own overhead
  * and should be factored out.  Essentially, a result object is needed that allows the user to assign vals where needed.
  *
  */
trait ContextCache {

  val cache = new HashMap[String, Any]() with SynchronizedMap[String, Any]
  val famCache = new HashMap[ColumnFamily[_, _, _, _, _], Any]() with SynchronizedMap[ColumnFamily[_, _, _, _, _], Any]
  val colCache = new HashMap[Column[_, _, _, _, _], Any]() with SynchronizedMap[Column[_, _, _, _, _], Any]

  def getOrUpdateFam[T](key: ColumnFamily[_, _, _, _, _])(value: => T): T = famCache.getOrElseUpdate(key, value).asInstanceOf[T]

  def getOrUpdateCol[T](key: Column[_, _, _, _, _])(value: => T): T = colCache.getOrElseUpdate(key, value).asInstanceOf[T]

  def getOrUpdate[T](key: String)(value: => T): T = cache.getOrElseUpdate(key, value).asInstanceOf[T]
}

/** When a query comes back, there are a bucket of column families and columns to retrieve.  This class retrieves them.
  *
  * @tparam T the source [[com.gravity.hbase.schema.HbaseTable]] this result came from
  * @tparam R the `type` of the table's rowid
  *
  * @param result the raw [[org.apache.hadoop.hbase.client.Result]] returned from the `hbase` [[org.apache.hadoop.hbase.client.Get]]
  * @param table the underlying [[com.gravity.hbase.schema.HbaseTable]]
  * @param tableName the name of the actual table
  */
class QueryResult[T <: HbaseTable[T, R, _], R](val result: Result, val table: HbaseTable[T, R, _], val tableName: String) extends Serializable with ContextCache {


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
    val col = result.getColumnLatest(co.familyBytes, co.columnBytes)
    col != null
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
  def column[F, K, V](column: (T) => Column[T, R, F, K, V])(implicit c: ByteConverter[V]): Option[V] = {
    val co = column(table.pops)
    getOrUpdateCol(co) {
      val col = result.getColumnLatest(co.familyBytes, co.columnBytes)
      if (col != null) {
        Some(c.fromBytes(col.getValue))
      } else {
        None
      }
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
  def columnFromFamily[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], columnName: K)(implicit c: ByteConverter[K], d: ByteConverter[V]): Option[V] = {
    val fam = family(table.pops)
    val qual = c.toBytes(columnName)
    val kvs = result.raw()
    kvs.find(kv => Bytes.equals(kv.getFamily, fam.familyBytes) && Bytes.equals(kv.getQualifier, qual)) match {
      case Some(v) => Some(d.fromBytes(v.getValue))
      case None => None
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
  def columnTimestamp[F, K, V](column: (T) => Column[T, R, F, K, V])(implicit c: ByteConverter[V]): Option[DateTime] = {
    val co = column(table.pops)
    val col = result.getColumnLatest(co.familyBytes, co.columnBytes)
    if (col != null) {
      Some(new DateTime(col.getTimestamp))
    } else {
      None
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
  def familyLatestTimestamp[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V])(implicit c: ByteConverter[F], d: ByteConverter[K], e: ByteConverter[V]): Option[DateTime] = {
    val fam = family(table.pops)
    var ts = -1l
    for (kv <- result.raw()) {
      if (Bytes.equals(kv.getFamily, fam.familyBytes)) {
        val tsn = kv.getTimestamp
        if (tsn > ts) ts = tsn
      }
    }
    if (ts >= 0) {
      Some(new DateTime(ts))
    }
    else {
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
  def family[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V])(implicit c: ByteConverter[F], d: ByteConverter[K], e: ByteConverter[V]): Map[K, V] = {
    val fm = family(table.pops)
    getOrUpdateFam(fm) {
      val kvs = result.raw()
      val mymap = scala.collection.mutable.Map[K, V]()
      mymap.sizeHint(kvs.size)
      for (kv <- kvs) yield {
        if (Bytes.equals(kv.getFamily, fm.familyBytes)) {
          mymap.put(d.fromBytes(kv.getQualifier), e.fromBytes(kv.getValue))
        }
      }
      mymap
    }
  }

  /** Extracts and deserializes only the keys (qualifiers) of the family as a `Set[K]`
    *
    * @tparam F the type of the column family name
    * @tparam K the type of the column family qualifier
    * @tparam V the type of the column family value
    *
    * @param family the underlying table's family `val`
    *
    */
  def familyKeySet[F, K](family: (T) => ColumnFamily[T, R, F, K, _])(implicit c: ByteConverter[F], d: ByteConverter[K]): Set[K] = {
    val fm = family(table.pops)
    val kvs = result.raw()
    val myset = scala.collection.mutable.Set[K]()
    myset.sizeHint(kvs.size)
    for (kv <- kvs) yield {
      if (Bytes.equals(kv.getFamily, fm.familyBytes)) {
        myset.add(d.fromBytes(kv.getQualifier))
      }
    }
    myset
  }

  /** The row identifier deserialized as type `R`
    *
    */
  def rowid(implicit c: ByteConverter[R]) = c.fromBytes(result.getRow)

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
class OpBase[T <: HbaseTable[T, R, _], R](table: HbaseTable[T, R, _], key: Array[Byte], previous: Buffer[OpBase[T, R]] = Buffer[OpBase[T, R]]()) {

  previous += this

  def put(key: R, writeToWAL: Boolean = true)(implicit c: ByteConverter[R]) = {
    val po = new PutOp(table, c.toBytes(key), previous, writeToWAL)
    po
  }

  def increment(key: R)(implicit c: ByteConverter[R]) = {
    val inc = new IncrementOp(table, c.toBytes(key), previous)
    inc
  }

  def delete(key: R)(implicit c: ByteConverter[R]) = {
    val del = new DeleteOp(table, c.toBytes(key), previous)
    del
  }

  def size = previous.size

  def getOperations: Iterable[Writable] = {
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

    deletes ++ puts ++ increments

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

  def value[F, K, Long](column: (T) => Column[T, R, F, K, Long], value: java.lang.Long)(implicit c: ByteConverter[F], d: ByteConverter[K]) = {
    val col = column(table.pops)
    increment.addColumn(col.familyBytes, col.columnBytes, value)
    this
  }

  def valueMap[F, K, Long](family: (T) => ColumnFamily[T, R, F, K, Long], values: Map[K, Long])(implicit c: ByteConverter[F], d: ByteConverter[K]) = {
    val fam = family(table.pops)
    for ((key, value) <- values) {
      increment.addColumn(fam.familyBytes, d.toBytes(key), value.asInstanceOf[java.lang.Long])
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

  def value[F, K, V](column: (T) => Column[T, R, F, K, V], value: V)(implicit c: ByteConverter[F], d: ByteConverter[K], e: ByteConverter[V]) = {
    val col = column(table.asInstanceOf[T])
    put.add(col.familyBytes, col.columnBytes, e.toBytes(value))
    this
  }

  def valueMap[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], values: Map[K, V])(implicit c: ByteConverter[K], d: ByteConverter[V]) = {
    val fam = family(table.pops)
    for ((key, value) <- values) {
      put.add(fam.familyBytes, c.toBytes(key), d.toBytes(value))
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

  def values[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], qualifiers: Set[K])(implicit vc: ByteConverter[K]) = {
    val fam = family(table.pops)
    for (q <- qualifiers) {
      delete.deleteColumns(fam.familyBytes, vc.toBytes(q))
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

  def family : ColumnFamily[_,_,_,_,_]
}

/**
  * Represents the specification of a Column Family
  */
class ColumnFamily[T <: HbaseTable[T, R, _], R, F, K, V](val table: HbaseTable[T, R, _], val familyName: F, val compressed: Boolean = false, val versions: Int = 1)(implicit c: ByteConverter[F], d: ByteConverter[K], e: ByteConverter[V]) extends KeyValueConvertible[F, K, V] {
  val familyConverter = c
  val keyConverter = d
  val valueConverter = e
  val familyBytes = c.toBytes(familyName)

  def family = this
}

/**
  * Represents the specification of a Column.
  */
class Column[T <: HbaseTable[T, R, _], R, F, K, V](table: HbaseTable[T, R, _], columnFamily: ColumnFamily[T,R,F,K,_], columnName: K)(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) extends KeyValueConvertible[F, K, V] {
  val columnBytes = kc.toBytes(columnName)
  val familyBytes = columnFamily.familyBytes

  val familyConverter = fc
  val keyConverter = kc
  val valueConverter = kv

  def getQualifier: K = columnName

  def family = columnFamily.asInstanceOf[ColumnFamily[_,_,_,_,_]]

  def getValue(res: QueryResult[T, R]) = {
    kv.fromBytes(res.result.getColumnLatest(familyBytes, columnBytes).getValue)
  }

  def apply(res: QueryResult[T, R]): Option[V] = {
    val rawValue = res.result.getColumnLatest(familyBytes, columnBytes)
    if (rawValue == null) {
      None
    }
    else {
      Some(kv.fromBytes(rawValue.getValue))
    }
  }

  def getOrDefault(res: QueryResult[T, R], default: V): V = apply(res) match {
    case Some(value) => value
    case None => default
  }
}

trait Schema {
  val tables = scala.collection.mutable.Set[HbaseTable[_, _, _]]()

  def table[T <: HbaseTable[T, _, _]](table: T) = {
    tables += table
    table
  }

}

/** Standard base class for all Row objects.
  *
  * Inside of a *Row object, it is good to use lazy val and def as opposed to val.
  * Because HRow objects are now the first-class instantiation of a query result, and because they are the type cached in Ehcache, they are good places to cache values.
  */
abstract class HRow[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R, RR]](result: Result, table: T) extends QueryResult[T, R](result, table, table.tableName)

case class DeserializedResult(rowid:AnyRef) {
  val values = new mutable.HashMap[ColumnFamily[_,_,_,_,_],mutable.Map[AnyRef,AnyRef]]()

  def add(family:ColumnFamily[_,_,_,_,_], qualifier:AnyRef, value:AnyRef) {
    val map = values.getOrElseUpdate(family, new HashMap[AnyRef,AnyRef]())
    map.put(qualifier,value)
  }
}

/**
  * Represents a Table.  Expects an instance of HBaseConfiguration to be present.
  * A parameter-type T should be the actual table that is implementing this one (this is to allow syntactic sugar for easily specifying columns during
  * queries).
  * A parameter-type R should be the type of the key for the table.
  */
class HbaseTable[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R, RR]](val tableName: String, var cache: QueryResultCache[T, R, RR] = new NoOpCache[T, R, RR](), rowKeyClass: Class[R], rowBuilder: (Result, T) => RR)(implicit conf: Configuration, keyConverter:ByteConverter[R]) {

  def pops = this.asInstanceOf[T]

  def buildRow(result: Result): RR = {rowBuilder(result, pops)}

  val tablePool = new HTablePool(conf, 50)

  def converterByBytes(famBytes: Array[Byte], colBytes: Array[Byte]) : KeyValueConvertible[_,_,_] = {
    //First make sure an overriding column has not been defined
    val col = columns.find {c => Bytes.equals(c.familyBytes, famBytes) && Bytes.equals(c.columnBytes, colBytes)}

    if(col.isEmpty) {
      families.find {f=> Bytes.equals(f.familyBytes, famBytes)}.get
    }else {
      col.get
    }
  }

  def convertResult(result: Result) = {
    val keyValues = result.raw()
    val rowId = keyConverter.fromBytes(result.getRow).asInstanceOf[AnyRef]

    val ds = DeserializedResult(rowId)

    val multiMap = ArrayListMultimap.create[AnyRef,(AnyRef,AnyRef)]()


    for {kv <- keyValues
                       family = kv.getFamily
                       key = kv.getQualifier
                       value = kv.getValue} yield {
      val c = converterByBytes(family, key)
      val f = c.family
      val k = c.keyConverter.fromBytes(key).asInstanceOf[AnyRef]
      val r = c.valueConverter.fromBytes(value).asInstanceOf[AnyRef]

      ds.add(f,k,r)
    }
    ds
  }


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


  private val columns = Buffer[Column[T, R, _, _, _]]()
  val families = Buffer[ColumnFamily[T, R, _, _, _]]()

  def familyBytes = families.map(family => family.familyBytes)

  val meta = family[String, String, Any]("meta")


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
    var alter = "disable '" + tableNameOverride + "'\n"
    alter += "alter '" + tableNameOverride + "', "
    alter += (for (family <- families) yield {
      familyDef(family)
    }).mkString(",")
    alter += "\nenable '" + tableNameOverride + "'"
    alter
  }

  def familyDef(family: ColumnFamily[T, _, _, _, _]) = {
    val compression = if (family.compressed) ", COMPRESSION=>'lzo'" else ""
    "{NAME => '%s', VERSIONS => %d%s}".format(Bytes.toString(family.familyBytes), family.versions, compression)
  }


  def getTable(name: String) = tablePool.getTable(name)

  def getBufferedTable(name: String) = bufferTablePool.getTable(name)

  def column[F, K, V](columnFamily: ColumnFamily[T, R, F, K, _], columnName: K, valueClass: Class[V])(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) = {
    val c = new Column[T, R, F, K, V](this, columnFamily, columnName)
    columns += c
    c
  }

  def family[F, K, V](familyName: F, compressed: Boolean = false, versions: Int = 1)(implicit c: ByteConverter[F], d: ByteConverter[K], e: ByteConverter[V]) = {
    val family = new ColumnFamily[T, R, F, K, V](this, familyName, compressed, versions)
    families += family
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

  def put(key: R, writeToWAL: Boolean = true)(implicit c: ByteConverter[R]) = new PutOp(this, c.toBytes(key))

  def delete(key: R)(implicit c: ByteConverter[R]) = new DeleteOp(this, c.toBytes(key))

  def increment(key: R)(implicit c: ByteConverter[R]) = new IncrementOp(this, c.toBytes(key))
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

