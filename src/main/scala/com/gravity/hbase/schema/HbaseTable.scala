package com.gravity.hbase.schema

import java.nio.ByteBuffer
import java.{lang, util}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.apache.hadoop.hbase.filter.{ColumnPaginationFilter, SingleColumnValueExcludeFilter, PageFilter, RegexStringComparator, SkipFilter, FamilyFilter, BinaryComparator, QualifierFilter, SubstringComparator, BinaryPrefixComparator, SingleColumnValueFilter, FilterList, Filter}
import org.joda.time.{ReadableInstant, DateTime}

import scala.collection.JavaConversions._
import scala.collection.mutable.{Buffer, ArrayBuffer}
import scala.collection._
import org.apache.commons.lang.ArrayUtils
import org.apache.hadoop.hbase.util.Bytes
import com.gravity.hbase.AnyConverterSignal
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator
import java.io.{Serializable, IOException}
import org.apache.hadoop.conf.Configuration
import java.util.Arrays
import org.apache.hadoop.hbase.{CellUtil, HColumnDescriptor, KeyValue}
import scala.Int
import org.apache.hadoop.hbase.client._

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * Represents the structural configuration for a table
 * @param maxFileSizeInBytes
 */
case class HbaseTableConfig(
                                   maxFileSizeInBytes:Long = -1,
                                   memstoreFlushSizeInBytes:Long = -1,
                                   tablePoolSize:Int = 5
                                   )

object HbaseTable {
  def defaultConfig = HbaseTableConfig()
}

/**
 * Represents a Table.  Expects an instance of HBaseConfiguration to be present.
 * A parameter-type T should be the actual table that is implementing this one
 * (this is to allow syntactic sugar for easily specifying columns during
 * queries).
 * A parameter-type R should be the type of the key for the table.
 * @param tableName
 * @param cache
 * @param rowKeyClass
 * @param logSchemaInconsistencies
 * @param conf
 * @param keyConverter
 * @tparam T
 * @tparam R
 * @tparam RR
 */
abstract class HbaseTable[T <: HbaseTable[T, R, RR], R, RR <: T#HRow](val tableName: String, var cache: QueryResultCache[T, R,RR] = new NoOpCache[T, R, RR](), rowKeyClass: Class[R], logSchemaInconsistencies: Boolean = false, tableConfig:HbaseTableConfig = HbaseTable.defaultConfig)(implicit conf: Configuration, keyConverter: ByteConverter[R])
  extends TablePoolStrategy
{

  def makeDeserializedResult(rowid: AnyRef, famCount: Int) = DeserializedResult(rowid, famCount)

  def makeQueryResult(result:DeserializedResult): QueryResult = new QueryResult(result)

  def rowBuilder(result: DeserializedResult): RR

  def getTableConfig = tableConfig
  def getConf = conf

  def emptyRow(key:Array[Byte]) = rowBuilder(new DeserializedResult(rowKeyConverter.fromBytes(key).asInstanceOf[AnyRef],families.size))
  def emptyRow(key:R) = rowBuilder(new DeserializedResult(key.asInstanceOf[AnyRef],families.size))

  val rowKeyConverter = keyConverter

  /**Provides the client with an instance of the superclass this table was defined against. */
  def pops = this.asInstanceOf[T]

  /**A method injected by the super class that will build a strongly-typed row object.  */
  def buildRow(result: Result): RR = {
    rowBuilder(convertResult(result))
  }


  /**A pool of table objects with AutoFlush set to false --therefore usable for asynchronous write buffering */
  val bufferTablePool = {
    new HTablePool(conf, 2, new HTableInterfaceFactory {
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
  }) }


  @volatile var famLookup: Array[Array[Byte]] = null
  @volatile var colFamLookup: Array[Array[Byte]] = null
  @volatile var famIdx: IndexedSeq[KeyValueConvertible[_, _]] = null
  @volatile var colFamIdx: IndexedSeq[KeyValueConvertible[_, _]] = null

  val bc = new ByteArrayComparator()

  implicit val o = new math.Ordering[Array[Byte]] {
    def compare(a: Array[Byte], b: Array[Byte]): Int = {
      bc.compare(a, b)
    }
  }


  /**Looks up a KeyValueConvertible by the family and column bytes provided.
   * Because of the rules of the system, the lookup goes as follows:
   * 1. Find a column first.  If you find a column first, it means there is a strongly-typed column defined.
   * 2. If no column, then find the family.
   *
   */
  def converterByBytes(famBytes: Array[Byte], colBytes: Array[Byte]): KeyValueConvertible[_, _] = {

    if (colFamLookup.length == 0 || famLookup.length == 0) {
      throw new RuntimeException("Attempting to lookup 0 length columns and families--HBaseTable is corrupt")
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
        null
      }
    }


  }

  /**Converts a result to a DeserializedObject. A conservative implementation that is slower than convertResultRaw but will always be more stable against
   * binary changes to Hbase's KeyValue format.
   */
  def convertResult(result: Result) = {
    if (result.isEmpty) {
      throw new RuntimeException("Attempting to deserialize an empty result.  If you want to handle the eventuality of an empty result, call singleOption() instead of single()")
    }
    val cells = result.rawCells()

    import JavaConversions._

    val rowId = keyConverter.fromBytes(result.getRow).asInstanceOf[AnyRef]
    val ds = DeserializedResult(rowId, families.size)

    val scanner = result.cellScanner()

    while(scanner.advance()) {
      val cell = scanner.current()
      try {

        val familyBytes = cell.getFamily
        val keyBytes = cell.getQualifier

        val c = converterByBytes(familyBytes, keyBytes)
        if (c == null) {
          if (logSchemaInconsistencies) {
            println("Table: " + tableName + " : Null Converter : " + Bytes.toString(cell.getFamilyArray))
          }
        }
        else if (!c.keyConverter.isInstanceOf[AnyConverterSignal] && !c.valueConverter.isInstanceOf[AnyConverterSignal]) {
          val f = c.family
          val k = c.keyConverter.fromBytes(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength).asInstanceOf[AnyRef]
          val r = c.valueConverter.fromBytes(cell.getValueArray, cell.getValueOffset, cell.getValueLength).asInstanceOf[AnyRef]
          ds.add(f,k, r, cell.getTimestamp)
        } else {
          if (logSchemaInconsistencies) {
            println("Table: " + tableName + " : Any Converter : " + Bytes.toString(cell.getFamilyArray))
          }
        }
      }
    }
    ds
  }



  def familyBytes = families.map(family => family.familyBytes)

  def familyByIndex(idx: Int) = familyArray(idx)

  lazy val familyArray = {
    val arr = new Array[ColumnFamily[_, _]](families.length)
    families.foreach {
      fam =>
        arr(fam.index) = fam
    }
    arr
  }

  def columnByIndex(idx: Int) = columnArray(idx)

  lazy val columnArray = {
    val arr = new Array[Column[_,_]](columns.length)
    columns.foreach {col => arr(col.columnIndex) = col}
    arr
  }


  //alter 'articles', NAME => 'html', VERSIONS =>1, COMPRESSION=>'lzo'

  /**
   * Generates a creation script for the table, based on the column families and table config.
   * @param tableNameOverride
   * @return
   */
  def createScript(tableNameOverride: String = tableName) = {
    var create = "create '" + tableNameOverride + "', "
    create += (for (family <- families) yield {
      familyDef(family)
    }).mkString(",")

    create += alterTableAttributesScripts(tableNameOverride)

    create
  }

  def alterTableAttributesScripts(tableName:String) = {
    var alterScript = ""
    if(tableConfig.memstoreFlushSizeInBytes > -1) {
      alterScript += alterTableAttributeScript(tableName, "MEMSTORE_FLUSHSIZE", tableConfig.memstoreFlushSizeInBytes.toString)
    }
    if(tableConfig.maxFileSizeInBytes > -1) {
      alterScript += alterTableAttributeScript(tableName, "MAX_FILESIZE", tableConfig.maxFileSizeInBytes.toString)
    }
    alterScript
  }

  def alterTableAttributeScript(tableName:String, attributeName:String, value:String) = {
    "\nalter '" + tableName + "', {METHOD => 'table_att', "+attributeName+" => '" + value + "'}"
  }

  def deleteScript(tableNameOverride: String = tableName) = {
    val delete = "disable '" + tableNameOverride + "'\n"

    delete + "delete '" + tableNameOverride + "'"
  }

  /**
   * Generates a production-friendly alter script (flush, disable, alter, enable)
   * @param tableNameOverride
   * @param families
   * @return
   */
  def alterScript(tableNameOverride: String = tableName, families: Seq[ColumnFamily[_, _]] = families) = {

    var alter = "flush '" + tableNameOverride + "'\n"
    alter += "disable '" + tableNameOverride + "'\n"
    alter += "alter '" + tableNameOverride + "', "
    alter += (for (family <- families) yield {
      familyDef(family)
    }).mkString(",")

    alter += alterTableAttributesScripts(tableNameOverride)
    alter += "\nenable '" + tableNameOverride + "'"
    alter
  }

  def familyDef(family: ColumnFamily[_, _]) = {
    val compression = if (family.compressed) ", COMPRESSION=>'lzo'" else ""
    val ttl = if (family.ttlInSeconds < HColumnDescriptor.DEFAULT_TTL) ", TTL=>'" + family.ttlInSeconds + "'" else ""
    "{NAME => '%s', VERSIONS => %d%s%s}".format(Bytes.toString(family.familyBytes), family.versions, compression, ttl)
  }



  def getBufferedTable(name: String) = bufferTablePool.getTable(name)

  private val columns = ArrayBuffer[Column[_,_]]()
  val families = ArrayBuffer[ColumnFamily[_,_]]()

  val columnsByName = mutable.Map[AnyRef, Column[_,_]]()

  private val columnsByBytes = mutable.Map[ByteBuffer, KeyValueConvertible[_,_]]()
  private val familiesByBytes = mutable.Map[ByteBuffer, KeyValueConvertible[_,_]]()

  var columnIdx = 0

  def column[K,V](columnFamily: ColumnFamily[K,_], columnName: K, valueClass: Class[V])(implicit kc: ByteConverter[K], kv: ByteConverter[V]) = {
    val c = new Column[K,V](columnFamily, columnName, columnIdx)
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

  def family[K, V](familyName: String, compressed: Boolean = false, versions: Int = 1, rowTtlInSeconds: Int = Int.MaxValue)(implicit d: ByteConverter[K], e: ByteConverter[V]) = {
    val family = new ColumnFamily[K, V](this, familyName, compressed, versions, familyIdx, rowTtlInSeconds)
    familyIdx = familyIdx + 1
    families += family
    familiesByBytes.put(ByteBuffer.wrap(family.familyBytes), family)
    family
  }

  def getTableOption(name: String) = {
    try {
      Some(getTable(this))
    } catch {
      case e: Exception => None
    }
  }


  def withTableOption[Q](name: String)(work: (Option[HTableInterface]) => Q): Q = {
    val table = getTableOption(name)
    try {
      work(table)
    } finally {
      table foreach (tbl => releaseTable(this,tbl))
    }
  }

  def withBufferedTable[Q](mytableName: String = tableName)(work: (HTableInterface) => Q): Q = {
    val table = getBufferedTable(mytableName)
    try {
      work(table)
    } finally {
      table.close()
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

  def query2 = new Query2Builder()

  def put(key: R, writeToWAL: Boolean = true) = new PutOp(keyConverter.toBytes(key), writeToWAL = writeToWAL)

  def delete(key: R) = new DeleteOp(keyConverter.toBytes(key))

  def increment(key: R) = new IncrementOp(keyConverter.toBytes(key))


  def init() {
    famLookup = Array.ofDim[Array[Byte]](families.size)
    for ((fam, idx) <- families.zipWithIndex) {
      famLookup(idx) = fam.familyBytes
    }
    Arrays.sort(famLookup, bc)
    famIdx = families.sortBy(_.familyBytes).toIndexedSeq

    colFamLookup = Array.ofDim[Array[Byte]](columns.size)
    for ((col, idx) <- columns.zipWithIndex) {
      colFamLookup(idx) = ArrayUtils.addAll(col.familyBytes, col.columnBytes)
    }
    Arrays.sort(colFamLookup, bc)
    colFamIdx = columns.sortBy(col => ArrayUtils.addAll(col.familyBytes, col.columnBytes)).toIndexedSeq
  }

  abstract class HRow(result: DeserializedResult) extends QueryResult(result) {

    def table = HbaseTable.this

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
          val family = familyByIndex(i)
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
          val family = familyByIndex(i)
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


  class QueryResult(val result: DeserializedResult) extends Serializable {


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
    def isColumnPresent[F, K, V](column: (T) => Column[K, V]): Boolean = {
      val co = column(pops)
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
    def column[F, K, V](column: (T) => Column[K, V]) = {
      val co = column(pops)
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
    def columnFromFamily[K, V](family: (T) => ColumnFamily[K, V], columnName: K) = {
      val fam = family(pops)
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
    def columnFromFamilyTimestamp[K, V](family: (T) => ColumnFamily[K, V], columnName: K) = {
      val fam = family(pops)
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
    def columnTimestamp[K, V](column: (T) => Column[K, V]): Option[DateTime] = {
      val co = column(pops)
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
    def familyLatestTimestamp[K, V](family: (T) => ColumnFamily[K, V]): Option[DateTime] = {
      val fam = family(pops)
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
    def family[K, V](family: (T) => ColumnFamily[K, V]): java.util.Map[K, V] = {
      val fm = family(pops)
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
    def familyKeySet[K](family: (T) => ColumnFamily[K, _]): java.util.Set[K] = {
      val fm = family(pops)
      result.familyKeySet[K](fm)
    }

    /**The row identifier deserialized as type `R`
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
   * A query for retrieving values.  It works somewhat differently than the data modification operations, in that you do the following:
   * 1. Specify one or more keys
   * 2. Specify columns and families to scan in for ALL the specified keys
   *
   * In other words there's no concept of having multiple rows fetched with different columns for each row (that seems to be a rare use-case and
   * would make the API very complex).
   */

  trait KeyValueConvertible[K, V] {
    val keyConverter: ByteConverter[K]
    val valueConverter: ByteConverter[V]

    def keyToBytes(key: K) = keyConverter.toBytes(key)

    def valueToBytes(value: V) = valueConverter.toBytes(value)

    def keyToBytesUnsafe(key: AnyRef) = keyConverter.toBytes(key.asInstanceOf[K])

    def valueToBytesUnsafe(value: AnyRef) = valueConverter.toBytes(value.asInstanceOf[V])

    def keyFromBytesUnsafe(bytes: Array[Byte]) = keyConverter.fromBytes(bytes).asInstanceOf[AnyRef]

    def valueFromBytesUnsafe(bytes: Array[Byte]) = valueConverter.fromBytes(bytes).asInstanceOf[AnyRef]

    def family: ColumnFamily[_, _]
  }

  /**
   * Represents the specification of a Column Family
   */
  class ColumnFamily[K, V](val table: HbaseTable[T, R, _], val familyName: String, val compressed: Boolean = false, val versions: Int = 1, val index: Int, val ttlInSeconds: Int = HColumnDescriptor.DEFAULT_TTL)(implicit d: ByteConverter[K], e: ByteConverter[V]) extends KeyValueConvertible[K, V] {
    val keyConverter = d
    val valueConverter = e
    val familyBytes = StringConverter.toBytes(familyName)


    def family = this


  }

  /**
   * Represents the specification of a Column.
   */
  class Column[K,V](columnFamily: ColumnFamily[_,_], val columnName: K, val columnIndex: Int)(implicit kc: ByteConverter[K], kv: ByteConverter[V]) extends KeyValueConvertible[K, V] {
    val columnBytes = kc.toBytes(columnName)
    val familyBytes = columnFamily.familyBytes
    val columnNameRef = columnName.asInstanceOf[AnyRef]

    val keyConverter = kc
    val valueConverter = kv

    def getQualifier: K = columnName

    def family = columnFamily.asInstanceOf[ColumnFamily[_, _]]


  }

  /**
   * The container for the result values deserialized from Hbase.
   * @param rowid
   * @param famCount
   */
  case class DeserializedResult(rowid: AnyRef, famCount: Int) {

    def isEmpty = values.size == 0

    def isEmptyRow = ! values.exists(family=>family != null && family.size() > 0)

    def getRow[R]() = rowid.asInstanceOf[R]


    def familyValueMap[K, V](fam: ColumnFamily[_, _]) = {
      val famMap = family(fam)
      if (famMap != null) {
        famMap.asInstanceOf[java.util.Map[K, V]]
      } else {
        new gnu.trove.map.hash.THashMap[K, V]()
      }
    }

    def familyKeySet[K](fam: ColumnFamily[_, _]) = {
      val famMap = family(fam)
      if (famMap != null) {
        famMap.keySet.asInstanceOf[java.util.Set[K]]
      } else {
        new gnu.trove.set.hash.THashSet[K]()
      }
    }

    def family(family: ColumnFamily[_, _]) = {
      values(family.index)
    }

    def familyOf(column: Column[_, _]) = family(column.family)

    def familyMap(fam: ColumnFamily[_, _]) = family(fam)

    def hasColumn(column: Column[_, _]) = {
      val valueMap = familyOf(column)
      if (valueMap == null || valueMap.size == 0) false else true
    }

    def columnValue(fam: ColumnFamily[_, _], columnName: AnyRef) = {
      val valueMap = family(fam)
      if (valueMap == null) {
        null
      } else {
        valueMap.get(columnName)
      }
    }

    def columnTimestamp(fam: ColumnFamily[_, _], columnName: AnyRef) = {
      val res = timestampLookaside(fam.index)
      if (res != null) {
        val colRes = res.get(columnName)
        colRes
      }
      else {
        0l
      }
    }


    def columnTimestampAsDate(column: Column[_, _]) = {
      val cts = columnTimestamp(column.family, column.columnNameRef)
      if (cts > 0) {
        new DateTime(cts)
      } else {
        null
      }
    }

    def columnTimestampByName(fam: ColumnFamily[_, _], columnName: AnyRef) = {
      val cts = columnTimestamp(fam, columnName)
      cts
    }

    def columnTimestampByNameAsDate(fam: ColumnFamily[_, _], columnName: AnyRef) = {
      val cts = columnTimestamp(fam, columnName)
      if (cts > 0) {
        new DateTime(cts)
      }
      else {
        null
      }
    }


    def columnValueSpecific(column: Column[_, _]) = {
      columnValue(column.family, column.columnNameRef)
    }


    var values = new Array[java.util.Map[AnyRef, AnyRef]](famCount)

    private val timestampLookaside = new Array[gnu.trove.map.TObjectLongMap[AnyRef]](famCount)



    /**This is a map whose key is the family type, and whose values are maps of column keys to columnvalues paired with their timestamps */
    //  val values = new java.util.HashMap[ColumnFamily[_, _, _, _, _], java.util.HashMap[AnyRef, AnyRef]]()

    //  val timestampLookaside = new java.util.HashMap[ColumnFamily[_, _, _, _, _], java.util.HashMap[AnyRef, Long]]()

    def add(family: ColumnFamily[_, _], qualifier: AnyRef, value: AnyRef, timeStamp: Long) {
      var map = values(family.index)
      if (map == null) {
        map = new gnu.trove.map.hash.THashMap[AnyRef, AnyRef]()
        values(family.index) = map
      }
      map.put(qualifier, value)

      var tsMap = timestampLookaside(family.index)
      if (tsMap == null) {
        tsMap = new gnu.trove.map.hash.TObjectLongHashMap[AnyRef]()
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

  /**
   * Expresses a scan, get, or batched get against hbase.  Which one it becomes depends on what
   * calls you make.  If you specify withKey() it will
   * become a Get, withKeys() will make into a batched get, and no keys at all will make it a Scan.
   *
   * @tparam T the table to work with
   * @tparam R the row key type
   * @tparam RR the row result type
   */
  trait BaseQuery {

    val keys = mutable.Buffer[Array[Byte]]()
    val families = mutable.Buffer[Array[Byte]]()
    val columns = mutable.Buffer[(Array[Byte], Array[Byte])]()
    var currentFilter: Filter = _
    // new FilterList(Operator.MUST_PASS_ALL)
    var startRowBytes: Array[Byte] = null
    var endRowBytes: Array[Byte] = null
    var batchSize = -1
    var startTime: Long = Long.MinValue
    var endTime: Long = Long.MaxValue

    def filter(filterFx: ((FilterBuilder) => Unit)*): this.type = {
      val fb = new FilterBuilder(true)
      for (fx <- filterFx) {
        fx(fb)
      }
      currentFilter = fb.coreList
      this
    }

    def filterOr(filterFx: ((FilterBuilder) => Unit)*): this.type = {
      val fb = new FilterBuilder(false)
      for (fx <- filterFx) {
        fx(fb)
      }
      currentFilter = fb.coreList
      this

    }

    /**
     * Lets you make your own filter, for experimentation
     * @param builder the builder what builds a filter
     */
    def withFilter(builder : => Filter) {
      currentFilter = builder
    }

    class FilterBuilder(and: Boolean) {
      var coreList: FilterList = if (and) new FilterList(Operator.MUST_PASS_ALL) else new FilterList(Operator.MUST_PASS_ONE)
      val clauseBuilder = new ClauseBuilder()

      private def addFilter(filter: FilterList) {
        //coreList = filter
        coreList.addFilter(filter)
      }


      def or(clauses: ((ClauseBuilder) => Option[Filter])*) = {
        val orFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE)
        for (ctx <- clauses) {
          val filter = ctx(clauseBuilder)
          if (filter.isDefined) {
            orFilter.addFilter(filter.get)
          }
        }
        if (orFilter.getFilters.size() > 0) {
          addFilter(orFilter)
        }
        this
      }

      def and(clauses: ((ClauseBuilder) => Option[Filter])*) = {
        val andFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL)
        for (cfx <- clauses) {
          val filter = cfx(clauseBuilder)
          if (filter.isDefined) {
            andFilter.addFilter(filter.get)
          }
        }
        if (andFilter.getFilters.size() > 0) {
          addFilter(andFilter)
        }
        this
      }

    }

    class ClauseBuilder() {

      def columnValueMustNotEqual[K, V](column: (T) => Column[K, V], value: V) = {
        val c = column(pops)
        val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.NOT_EQUAL, c.valueConverter.toBytes(value))
        vc.setFilterIfMissing(true)
        vc.setLatestVersionOnly(true)
        Some(vc)
      }

      def columnValueMustStartWith[K, V](column: (T) => Column[K, String], prefix: String) = {
        val c = column(pops)
        val prefixFilter = new BinaryPrefixComparator(Bytes.toBytes(prefix))
        val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.EQUAL, prefixFilter)
        Some(vc)
      }


      def noClause = None

      def columnValueMustContain[K, V](column: (T) => Column[K, String], substr: String) = {
        val c = column(pops)
        val substrFilter = new SubstringComparator(substr)
        val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.EQUAL, substrFilter)
        Some(vc)
      }

      /**
       * Untested
       */
      def whereFamilyHasKeyGreaterThan[K](family: (T) => ColumnFamily[K, _], key: K) = {
        val f = family(pops)
        val fl = new FilterList(Operator.MUST_PASS_ALL)
        val ts = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(f.keyConverter.toBytes(key)))
        val ff = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(f.familyBytes))
        fl.addFilter(ts)
        fl.addFilter(ff)
        val sk = new SkipFilter(fl)
        Some(sk)
      }

      def columnValueMustPassRegex[K, V](column: (T) => Column[K, String], regex: String) = {
        val c = column(pops)
        val regexFilter = new RegexStringComparator(regex)
        val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.EQUAL, regexFilter)
        Some(vc)
      }


      def columnValueMustNotContain[K](column: (T) => Column[K, String], substr: String) = {
        val c = column(pops)
        val substrFilter = new SubstringComparator(substr)
        val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.NOT_EQUAL, substrFilter)
        Some(vc)
      }


      def maxRowsPerServer(rowsize: Int): Option[Filter] = {
        val pageFilter = new PageFilter(rowsize)
        Some(pageFilter)
      }

      def columnValueMustEqual[K, V](column: (T) => Column[K, V], value: V) = {
        val c = column(pops)
        val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.EQUAL, c.valueConverter.toBytes(value))
        vc.setFilterIfMissing(true)
        vc.setLatestVersionOnly(true)
        Some(vc)
      }

      def columnValueMustBeIn[K,V](column: (T) => Column[K,V], values: Set[V]) = {
        val c = column(pops)
        val fl = new FilterList(FilterList.Operator.MUST_PASS_ONE)
        values.foreach{value=>
          val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.EQUAL, c.valueConverter.toBytes(value))
          vc.setFilterIfMissing(true)
          vc.setLatestVersionOnly(true)
          fl.addFilter(vc)
        }

        Some(fl)
      }

      def columnValueMustBeGreaterThan[K, V](column: (T) => Column[K, V], value: V) = {
        val c = column(pops)
        val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.GREATER, c.valueConverter.toBytes(value))
        vc.setFilterIfMissing(true)
        vc.setLatestVersionOnly(true)
        Some(vc)
      }

      def columnValueMustBeLessThan[K, V](column: (T) => Column[K, V], value: V) = {
        val c = column(pops)
        val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.LESS, c.valueConverter.toBytes(value))
        vc.setFilterIfMissing(true)
        vc.setLatestVersionOnly(true)
        Some(vc)
      }

      def columnValueMustBePresent[K, V](column: (T) => Column[K, V]) = {
        val c = column(pops)
        val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.NOT_EQUAL, Bytes.toBytes(0))
        vc.setFilterIfMissing(true)
        vc.setLatestVersionOnly(true)
        Some(vc)
      }

      def lessThanColumnKey[K, V](family: (T) => ColumnFamily[K, V], value: K) = {
        val fam = family(pops)
        val valueFilter = new QualifierFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(fam.keyConverter.toBytes(value)))
        val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(family(pops).familyBytes))
        val andFilter = new FilterList(Operator.MUST_PASS_ALL)
        andFilter.addFilter(familyFilter)
        andFilter.addFilter(valueFilter)
        Some(andFilter)
      }

      def greaterThanColumnKey[K, V](family: (T) => ColumnFamily[K, V], value: K) = {
        val fam = family(pops)
        val andFilter = new FilterList(Operator.MUST_PASS_ALL)
        val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(fam.familyBytes))
        val valueFilter = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(fam.keyConverter.toBytes(value)))
        andFilter.addFilter(familyFilter)
        andFilter.addFilter(valueFilter)
        Some(andFilter)
      }

      //  def columnFamily[F,K,V](family: (T) => ColumnFamily[T,R,F,K,V])(implicit c: ByteConverter[F]): Query[T,R] = {
      //    val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(family(table.pops).familyBytes))
      //    currentFilter.addFilter(familyFilter)
      //    this
      //  }

      /**
       * This filter only returns rows that have the desired column (as expected),
       * but does NOT actually return the column value with the results (which is behavior that you probably didn't expect).
       *
       * That's pretty surprising behavior, so it's now deprecated in favor of columnValueMustBePresent,
       * which filters out rows that don't contain the column and also does actually return the column value with the results.
       */
      @deprecated def whereColumnMustExist[K, _](column: (T) => Column[K, _]) = {
        val c = column(pops)
        val valFilter = new SingleColumnValueExcludeFilter(c.familyBytes, c.columnBytes, CompareOp.NOT_EQUAL, new Array[Byte](0))
        valFilter.setFilterIfMissing(true)
        Some(valFilter)
      }

      def betweenColumnKeys[K, V](family: (T) => ColumnFamily[K, V], lower: K, upper: K) = {
        val fam = family(pops)
        val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(fam.familyBytes))
        val begin = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(fam.keyConverter.toBytes(lower)))
        val end = new QualifierFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(fam.keyConverter.toBytes(upper)))

        val filterList = new FilterList(Operator.MUST_PASS_ALL)
        filterList.addFilter(familyFilter)
        filterList.addFilter(begin)
        filterList.addFilter(end)
        Some(filterList)
      }

      def inFamily(family: (T) => ColumnFamily[_, _]) = {
        val fam = family(pops)
        val ff = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(fam.familyBytes))
        Some(ff)
      }

      def allInFamilies(familyList: ((T) => ColumnFamily[_, _])*) = {
        val filterList = new FilterList(Operator.MUST_PASS_ONE)
        for (family <- familyList) {
          val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(family(pops).familyBytes))
          filterList.addFilter(familyFilter)
        }
        Some(filterList)
      }

      /**
       * Limits the amount of columns returned within a single family where `pageSize` is the number of columns desired and `pageOffset` is the starting column index
       * @param family the family which contains the columns desired to be paged
       * @param pageSize the maximum number of columns to be returned for this family
       * @param pageOffset the offset that is to represent the first column of this page to return
       * @tparam F the family's type
       * @tparam K the family's key type
       * @tparam V the family's value type
       */
      def withPaginationForFamily[K, V](family: (T) => ColumnFamily[K, V], pageSize: Int, pageOffset: Int) = {
        val fam = family(pops)
        val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(fam.familyBytes))
        val paging = new ColumnPaginationFilter(pageSize, pageOffset)

        val filterList = new FilterList(Operator.MUST_PASS_ALL)
        filterList.addFilter(familyFilter)
        filterList.addFilter(paging)

        Some(filterList)
      }
    }

    /**The key to fetch (this makes it into a Get request against hbase) */
    def withKey(key: R): this.type = {
      keys += rowKeyConverter.toBytes(key)
      this
    }

    /**Multiple keys to fetch (this makes it into a multi-Get request against hbase) */
    def withKeys(keys: Set[R]): this.type = {
      for (key <- keys) {
        withKey(key)
      }
      this
    }



    def betweenDates(start: ReadableInstant, end: ReadableInstant): this.type = {
      startTime = start.getMillis
      endTime = end.getMillis
      this
    }

    def afterDate(start: ReadableInstant): this.type = {
      startTime = start.getMillis
      this
    }

    def untilDate(end: ReadableInstant): this.type = {
      endTime = end.getMillis
      this
    }

    def withStartRow(row: R): this.type = {
      startRowBytes = rowKeyConverter.toBytes(row)
      this
    }

    def withEndRow(row: R): this.type = {
      endRowBytes = rowKeyConverter.toBytes(row)
      this
    }

    def withBatchSize(size: Int): this.type = {
      batchSize = size
      this
    }

  }

  trait MinimumFiltersToExecute {

    this: BaseQuery =>

    def withFamilies(firstFamily: (T) => ColumnFamily[_, _], familyList: ((T) => ColumnFamily[_, _])*): Query2

    def withColumnsInFamily[K, V](family: (T) => ColumnFamily[K, V], firstColumn: K, columnList: K*): Query2

    @deprecated("withColumnsInFamily can select one or more columns from a single family")
    def withColumn[K, V](family: (T) => ColumnFamily[K, V], columnName: K): Query2

    @deprecated("withColumns can select one or more columns")
    def withColumn[K, V](column: (T) => Column[K, V]): Query2

    def withColumns[K, V](firstColumn: (T) => Column[_, _], columnList: ((T) => Column[_, _])*): Query2

  }

  class Query2 private(
                                                                        override val keys: mutable.Buffer[Array[Byte]],
                                                                        override val families: mutable.Buffer[Array[Byte]],
                                                                        override val columns: mutable.Buffer[(Array[Byte], Array[Byte])]) extends BaseQuery with MinimumFiltersToExecute {

    private[schema] def this(
                              keys: mutable.Buffer[Array[Byte]] = mutable.Buffer[Array[Byte]](),
                              families: mutable.Buffer[Array[Byte]] = mutable.Buffer[Array[Byte]](),
                              columns: mutable.Buffer[(Array[Byte], Array[Byte])] = mutable.Buffer[(Array[Byte], Array[Byte])](),
                              currentFilter: Filter,
                              startRowBytes: Array[Byte],
                              endRowBytes: Array[Byte],
                              batchSize: Int,
                              startTime: Long,
                              endTime: Long) = {
      this(keys, families, columns)
      this.currentFilter = currentFilter
      this.startRowBytes = startRowBytes
      this.endRowBytes = endRowBytes
      this.batchSize = batchSize
      this.startTime = startTime
      this.endTime = endTime
    }

    def getTableName = tableName

    override def withFamilies(firstFamily: (T) => ColumnFamily[_, _], familyList: ((T) => ColumnFamily[_, _])*) = {
      for (family <- firstFamily +: familyList) {
        val fam = family(pops)
        families += fam.familyBytes
      }
      this
    }

    override def withColumnsInFamily[K, V](family: (T) => ColumnFamily[K, V], firstColumn: K, columnList: K*) = {
      val fam = family(pops)
      for (column <- firstColumn +: columnList) {
        columns += (fam.familyBytes -> fam.keyConverter.toBytes(column))
      }
      this
    }

    override def withColumn[K, V](family: (T) => ColumnFamily[K, V], columnName: K) = {
      val fam = family(pops)
      columns += (fam.familyBytes -> fam.keyConverter.toBytes(columnName))
      this
    }

    override def withColumn[K, V](column: (T) => Column[K, V]) = {
      val col = column(pops)
      columns += (col.familyBytes -> col.columnBytes)
      this
    }

    override def withColumns[K, V](firstColumn: (T) => Column[_, _], columnList: ((T) => Column[_, _])*) = {
      for (column <- firstColumn +: columnList) {
        val col = column(pops)
        columns += (col.familyBytes -> col.columnBytes)
      }
      this
    }

    def single(tableName: String = tableName, ttl: Int = 30, skipCache: Boolean = true) = singleOption(tableName, ttl, skipCache, noneOnEmpty = false).get




    /**
     *
     * @param tableName The Name of the table
     * @param ttl The time to live for any cached results
     * @param skipCache Whether to skip any cacher defined for this table
     * @param noneOnEmpty If true, will return None if the result is empty.  If false, will return an empty row.  If caching is true, will cache the empty row.
     * @return
     */
    def singleOption(tableName: String = tableName, ttl: Int = 30, skipCache: Boolean = true, noneOnEmpty: Boolean = true): Option[RR] = {
      require(keys.size == 1, "Calling single() with more than one key")
      require(keys.size >= 1, "Calling a Get operation with no keys specified")
      val get = new Get(keys.head)
      get.setMaxVersions(1)

      if (startTime != Long.MinValue || endTime != Long.MaxValue) {
        get.setTimeRange(startTime, endTime)
      }

      for (family <- families) {
        get.addFamily(family)
      }
      for ((columnFamily, column) <- columns) {
        get.addColumn(columnFamily, column)
      }
      if (currentFilter != null) {
        get.setFilter(currentFilter)
      }

      if (skipCache || cache.isInstanceOf[NoOpCache[T, R, RR]])
        withTableOption(tableName) {
          case Some(htable) =>
            val result = htable.get(get)
            if (noneOnEmpty && result.isEmpty) {
              None
            }
            else if (!noneOnEmpty && result.isEmpty) {
              val qr = emptyRow(keys.head)
              Some(qr)
            }
            else {
              val qr = buildRow(result)
              Some(qr)
            }
          case None => None
        }
      else {
        val cacheKey = cache.getKeyFromGet(get)
        cache.getLocalResult(cacheKey) match {
          case Found(result) =>
            cache.instrumentRequest(1, 1, 0, 0, 0)
            Some(result)
          case FoundEmpty =>
            cache.instrumentRequest(1, 1, 0, 0, 0)
            if (noneOnEmpty)
              None
            else {
              val qr = emptyRow(keys.head)
              Some(qr)
            }
          case NotFound =>
            cache.getRemoteResult(cacheKey) match {
              case Found(result) =>
                cache.putResultLocal(cacheKey, Some(result), ttl)
                cache.instrumentRequest(1, 0, 1, 1, 0)
                Some(result)
              case FoundEmpty =>
                cache.instrumentRequest(1, 0, 1, 1, 0)
                if (noneOnEmpty) //then also don't cache.
                  None
                else {
                  cache.putResultLocal(cacheKey, None, ttl) //just cache None, so the cache will return "FoundEmpty", but RETURN the empty row to keep behavior consistent
                  val qr = emptyRow(keys.head)
                  Some(qr)
                }
              case NotFound =>
                val fromTable = withTableOption(tableName) {
                  case Some(htable) =>
                    val result = htable.get(get)
                    if (noneOnEmpty && result.isEmpty) {
                      //this means DO NOT CACHE EMPTY. But return None
                      None
                    }
                    else if (!noneOnEmpty && result.isEmpty) {
                      //this means - return empty row, cache None. believe it or not this is easier than the alternative, because of the logic in multi-get
                      cache.putResultLocal(cacheKey, None, ttl)
                      cache.putResultRemote(cacheKey, None, ttl)
                      val qr = emptyRow(keys.head)
                      Some(qr)
                    }
                    else {
                      //the result isn't empty so we don't care about the noneOnEmpty settings
                      val qr = buildRow(result)
                      val ret = Some(qr)
                      cache.putResultLocal(cacheKey, ret, ttl)
                      cache.putResultRemote(cacheKey, ret, ttl)
                      ret
                    }
                  case None => None //the table doesn't even exist. let's just go on our merry way, because it's probably something bigger than us gone wrong
                }
                cache.instrumentRequest(1, 0, 1, 0, 1)
                fromTable
              case Error(message, exceptionOption) => //don't save back to remote if there was an error - it's likely overloaded and this just creates a cascading failure
                val fromTable = withTableOption(tableName) {
                  case Some(htable) =>
                    val result = htable.get(get)
                    if (noneOnEmpty && result.isEmpty) {
                      //this means DO NOT CACHE EMPTY. But return None
                      None
                    }
                    else if (!noneOnEmpty && result.isEmpty) {
                      //this means - return empty row, cache None. believe it or not this is easier than the alternative, because of the logic in multi-get
                      cache.putResultLocal(cacheKey, None, ttl)
                      val qr = emptyRow(keys.head)
                      Some(qr)
                    }
                    else {
                      //the result isn't empty so we don't care about the noneOnEmpty settings
                      val qr = buildRow(result)
                      val ret = Some(qr)
                      cache.putResultLocal(cacheKey, ret, ttl)
                      ret
                    }
                  case None => None //the table doesn't even exist. let's just go on our merry way, because it's probably something bigger than us gone wrong
                }

                cache.instrumentRequest(1, 0, 1, 0, 1)
                fromTable
            }
          case Error(message, exceptionOption) => //don't save back to the local cache if there was an error retrieving
            cache.getRemoteResult(cacheKey) match {
              case Found(result) =>
                cache.instrumentRequest(1, 0, 1, 1, 0)
                Some(result)
              case FoundEmpty =>
                cache.instrumentRequest(1, 0, 1, 1, 0)
                if (noneOnEmpty)
                  None
                else {
                  val qr = emptyRow(keys.head)
                  Some(qr)
                }
              case NotFound =>
                val fromTable = withTableOption(tableName) {
                  case Some(htable) =>
                    val result = htable.get(get)
                    if (noneOnEmpty && result.isEmpty) {
                      None
                    }
                    else if (!noneOnEmpty && result.isEmpty) {
                      val qr = emptyRow(keys.head)
                      Some(qr)
                    }
                    else {
                      //the result isn't empty so we don't care about the noneOnEmpty settings
                      val qr = buildRow(result)
                      val ret = Some(qr)
                      ret
                    }
                  case None => None //the table doesn't even exist. let's just go on our merry way, because it's probably something bigger than us gone wrong
                }
                cache.instrumentRequest(1, 0, 1, 0, 1)
                fromTable
              case Error(remoteMessage, remoteExceptionOption) => //don't save back to remote if there was an error - it's likely overloaded and this just creates a cascading failure
                cache.instrumentRequest(1, 0, 1, 0, 1)
                withTableOption(tableName) {
                  case Some(htable) =>
                    val result = htable.get(get)
                    if (noneOnEmpty && result.isEmpty) {
                      None
                    }
                    else if (!noneOnEmpty && result.isEmpty) {
                      val qr = emptyRow(keys.head)
                      Some(qr)
                    }
                    else {
                      val qr = buildRow(result)
                      val ret = Some(qr)
                      ret
                    }
                  case None => None //the table doesn't even exist. let's just go on our merry way, because it's probably something bigger than us gone wrong
                }
            }
        }
      }
    }

    /**
     * Kept for binary compatibility.  Will be deprecated for multi()
     * @param tableName The Name of the table
     * @param ttl The Time to Live for any cached results
     * @param skipCache Whether to skip any defined cache
     * @return
     */
    def executeMap(tableName: String = tableName, ttl: Int = 30, skipCache:Boolean=true) : Map[R,RR] = multiMap(tableName, ttl, skipCache, returnEmptyRows = false)

    /**
     * @param tableName The Name of the table
     * @param ttl The Time to Live for any cached results
     * @param skipCache Whether to skip any defined cache
     * @param returnEmptyRows If this is on, then empty rows will be returned and cached.  Often times empty rows are not considered in caching situations
     *                        so this is off by default to keep the mental model simple.
     * @return
     */
    def multiMap(tableName: String = tableName, ttl: Int = 30, skipCache: Boolean = true, returnEmptyRows:Boolean=false): Map[R, RR] = {
      if (keys.isEmpty) return Map.empty[R, RR] // don't get all started with nothing to do

      // init our result map and give it a hint of the # of keys we have
      val resultMap = mutable.Map[R, RR]()
      resultMap.sizeHint(keys.size) // perf optimization

      val localKeysToGetsAndCacheKeys: Map[Int, (Get, String)] = buildKeysToGetsAndCacheKeys()
      val gets = localKeysToGetsAndCacheKeys.values.map(_._1).toList

      if(returnEmptyRows) {
        for (key <- keys) {
          resultMap(rowKeyConverter.fromBytes(key)) = emptyRow(key)
        }
      }

      if(skipCache || cache.isInstanceOf[NoOpCache[T, R, RR]]) {
        if (gets.nonEmpty) {
          withTable(tableName) {
            htable =>
              htable.get(gets).foreach(res => {
                if (res != null && !res.isEmpty) {
                  val qr = buildRow(res) // construct query result
                  resultMap(qr.rowid) = qr // place it in our result map
                }
              })
          }
        }
      }
      else {
        val resultBuffer = mutable.Map[String, Option[RR]]() //the results, built from local cache, remote cache, and finally the
        var localhits = 0
        var localmisses = 0
        var remotehits = 0
        var remotemisses = 0

        val remoteCacheMisses = mutable.Buffer[String]()
        val cacheKeysToGets = localKeysToGetsAndCacheKeys.values.map(tuple => tuple._2 -> tuple._1).toMap //gets.map(get => table.cache.getKeyFromGet(get) -> get).toMap
        val cacheKeysRemaining = mutable.Set[String](localKeysToGetsAndCacheKeys.values.map(_._2).toSeq: _*)

        val localCacheResults = cache.getLocalResults(cacheKeysRemaining)

        localCacheResults.foreach { case (key, localCacheResult) =>
          localCacheResult match {
            case Found(result) =>
              cacheKeysRemaining.remove(key)
              localhits += 1
              resultBuffer.update(key, Some(result))
            case FoundEmpty =>
              cacheKeysRemaining.remove(key)
              localhits += 1
              if(returnEmptyRows) resultBuffer.update(key, None)
            case NotFound =>
              localmisses += 1
            case Error(message, exceptionOption) =>
              localmisses += 1
          }
        }

        val remoteCacheResults = cache.getRemoteResults(cacheKeysRemaining)

        remoteCacheResults.foreach { case (key, remoteCacheResult) =>
          remoteCacheResult match {
            case Found(result) =>
              resultBuffer.update(key, Some(result))
              cacheKeysRemaining.remove(key)
              remotehits += 1
              cache.putResultLocal(key, Some(result), ttl)
            case FoundEmpty =>
              cacheKeysRemaining.remove(key)
              remotehits += 1
              if(returnEmptyRows) {
                //return empty rows also means cache empty rows
                resultBuffer.update(key, None)
                cache.putResultLocal(key, None, ttl)
              }
            case NotFound =>
              remoteCacheMisses += key
              remotemisses += 1
            case Error(message, exceptionOption) =>
              //don't record the miss key here, to prevent a saveback
              remotemisses += 1
          }
        }

        //anything in remaining keys that remote cache does not have, add to missing
        cacheKeysRemaining.filter(key => !remoteCacheResults.contains(key)).map{ missingKey =>
          remoteCacheMisses += missingKey
          remotemisses += 1
        }

        val allCacheMissGets = cacheKeysRemaining.map(key => cacheKeysToGets(key)).toList

        if (allCacheMissGets.nonEmpty) {
          withTable(tableName) {
            htable =>
              htable.get(allCacheMissGets).foreach(res => {
                if (res != null && !res.isEmpty) {
                  val localKey = getIntFromBytes(res.getRow)
                  val cacheKey = localKeysToGetsAndCacheKeys(localKey)._2
                  val theThingWeWant = Some(buildRow(res))
                  //put it in local cache now, and the resultBuffer will be used to send to remote in bulk later
                  cache.putResultLocal(cacheKey, theThingWeWant, ttl)
                  resultBuffer.update(cacheKey, theThingWeWant)
                }
              })
          }

          if (returnEmptyRows) {
            //then we need also need to cache Nones for everything that came back empty. while processing each result, we didn't have the associated cache keys,
            //so we have to derive from other data which to associate with None.
            //results.keySet is everything we've gotten back, one way or another
            //cacheKeysToGets.keySet is everything, total.
            //so everything minus things we've already gotten back is things that were missing from the table fetch
            val keyForThingsThatDontExist = cacheKeysToGets.keySet.diff(resultBuffer.keySet) //result buffer will have nones from remote cache and local cache, but the ones from remote will already be in local
            keyForThingsThatDontExist.map(keyForThingThatDoesntExist => {
              //and each of those should be cached as None. put it in local cache now, and the resultBuffer will be used to send to remote in bulk later
              cache.putResultLocal(keyForThingThatDoesntExist, None, ttl)
              resultBuffer.update(keyForThingThatDoesntExist, None)
            })
          }
        }

        val remoteCacheMissesSet = remoteCacheMisses.toSet
        val resultsToSaveRemote = resultBuffer.filterKeys(key => remoteCacheMissesSet.contains(key)) //we only want to save Nones if we are returning empty rows, but there will only be Nones in the results if that is true
        cache.putResultsRemote(resultsToSaveRemote, ttl)
        cache.instrumentRequest(keys.size, localhits, localmisses, remotehits, remotemisses)

        resultBuffer.values.map(valueOpt => {
          valueOpt match {
            case Some(value) => resultMap(value.rowid) = value
            case None => //if we want to return empty rows, they are already there. if we don't... they aren't, and we don't have to change anything.
          }
        })
      }

      resultMap // DONE!
    }

    private def getIntFromBytes(bytes: Array[Byte]) : Int = {
      scala.util.hashing.MurmurHash3.arrayHash(bytes)
    }

    private def buildKeysToGetsAndCacheKeys(): Map[Int, (Get, String)] = {
      if (keys.isEmpty) return Map.empty[Int,(Get, String)] // no keys..? nothing to see here... move along... move along.

      val gets = mutable.Buffer[(Int,Get)]() // buffer for the raw `Get's

      for (key <- keys) {
        val get = new Get(key)
        if (startTime != Long.MinValue || endTime != Long.MaxValue) {
          get.setTimeRange(startTime, endTime)
        }

        gets += Tuple2(getIntFromBytes(key), get)
      }

      // since the families and columns will be identical for all `Get's, only build them once
      val firstGet = gets(0)._2

      // add all families to the first `Get'
      for (family <- families) {
        firstGet.addFamily(family)
      }
      // add all columns to the first `Get'
      for ((columnFamily, column) <- columns) {
        firstGet.addColumn(columnFamily, column)
      }
      if (currentFilter != null) {
        firstGet.setFilter(currentFilter)
      }


      var pastFirst = false
      for (get <- gets) {
        if (pastFirst) {
          // we want to skip the first `Get' as it already has families/columns

          // for all subsequent `Get's, we will build their familyMap from the first `Get'
          firstGet.getFamilyMap.foreach((kv: (Array[Byte], util.NavigableSet[Array[Byte]])) => {
            get._2.getFamilyMap.put(kv._1, kv._2)
          })
          if (currentFilter != null) {
            get._2.setFilter(currentFilter)
          }
        }
        else {
          pastFirst = true
        }
      }

      gets.map{case (key, get) => key -> Tuple2(get, cache.getKeyFromGet(get))}.toMap //I feel like there must be a way to do this with just one toMap but i'm not sure what it is.
    }

    def makeScanner(maxVersions: Int = 1, cacheBlocks: Boolean = true, cacheSize: Int = 100) = {
      require(keys.size == 0, "A scanner should not specify keys, use singleOption or execute or executeMap")
      val scan = new Scan()
      scan.setMaxVersions(maxVersions)
      scan.setCaching(cacheSize)
      scan.setCacheBlocks(cacheBlocks)

      if (batchSize > -1) {
        scan.setBatch(batchSize)
      }

      if (startTime != Long.MinValue || endTime != Long.MaxValue) {
        scan.setTimeRange(startTime, endTime)
      }

      if (startRowBytes != null) {
        scan.setStartRow(startRowBytes)
      }
      if (endRowBytes != null) {
        scan.setStopRow(endRowBytes)
      }

      for (family <- families) {
        scan.addFamily(family)
      }
      for (column <- columns) {
        scan.addColumn(column._1, column._2)
      }

      if (currentFilter != null) {
        scan.setFilter(currentFilter)
      }

      scan
    }

    def scan(handler: (RR) => Unit, maxVersions: Int = 1, cacheBlocks: Boolean = true, cacheSize: Int = 100, useLocalCache: Boolean = false, localTTL: Int = 30) {
      val scan = makeScanner(maxVersions, cacheBlocks, cacheSize)

      val results = if (useLocalCache) mutable.Buffer[RR]() else mutable.Buffer.empty[RR]

      def cacheHandler(rr: RR) {
        if (useLocalCache) results += rr
      }

      def cacheComplete() {
        if (useLocalCache && results.nonEmpty) cache.putScanResult(scan, results.toSeq, localTTL)
      }

      val whatWeGetFromCache = if (useLocalCache) cache.getScanResult(scan) else None

      whatWeGetFromCache match {
        case Some(result) =>
          println("cache hit against key " + scan.toString)
          result.foreach(handler)
        case None =>

          withTable() {
            htable =>

              val scanner = htable.getScanner(scan)

              try {
                var done = false
                while (!done) {
                  val result = scanner.next()
                  if (result != null) {
                    val rr = buildRow(result)
                    cacheHandler(rr)
                    handler(rr)
                  } else {
                    done = true
                  }
                }
              } finally {
                cacheComplete()
                scanner.close()
              }
          }
      }

    }

    def scanToIterable[I](handler: (RR) => I, maxVersions: Int = 1, cacheBlocks: Boolean = true, cacheSize: Int = 100, useLocalCache: Boolean = false, localTTL: Int = 30) = {
      val scan = makeScanner(maxVersions, cacheBlocks, cacheSize)

      val results = if (useLocalCache) mutable.Buffer[RR]() else mutable.Buffer.empty[RR]

      def cacheHandler(rr: RR) {
        if (useLocalCache) results += rr
      }

      def cacheComplete() {
        if (useLocalCache && results.nonEmpty) cache.putScanResult(scan, results.toSeq, localTTL)
      }

      val whatWeGetFromCache = if (useLocalCache) cache.getScanResult(scan) else None

      val results2 = whatWeGetFromCache match {
        case Some(rrs) => rrs.map(rr => handler(rr))
        case None =>
          val runResults = withTable() {
            htable =>
              val scanner = htable.getScanner(scan)
              try {
                for (result <- scanner; if result != null) yield {
                  val rr = buildRow(result)
                  cacheHandler(rr)
                  handler(rr)
                }
              } finally {
                cacheComplete()
                scanner.close()
              }
          }

          runResults
      }
      results2
    }

    trait Stopable extends Throwable

    object YouCanStopNow extends Stopable

    /**Similar to the scan method but if your handler returns false, it will stop scanning.
      *
      */
    def scanUntil(handler: (RR) => Boolean, maxVersions: Int = 1, cacheBlocks: Boolean = true, cacheSize: Int = 100) {
      withTable() {
        htable =>
          val scan = makeScanner(maxVersions, cacheBlocks, cacheSize)

          val scanner = htable.getScanner(scan)

          try {
            for (result <- scanner) {
              if (!handler(buildRow(result))) throw YouCanStopNow
            }
          } catch {
            case _: Stopable => // nothing to see here... move along. move along.
          } finally {
            scanner.close()
          }
      }
    }

  }


  class Query2Builder extends BaseQuery with MinimumFiltersToExecute {

    def toQuery2 = new Query2(keys, families, columns, currentFilter, startRowBytes, endRowBytes, batchSize, startTime, endTime)

    def withAllColumns = toQuery2

    override def withFamilies(firstFamily: (T) => ColumnFamily[_, _], familyList: ((T) => ColumnFamily[_, _])*) = {
      for (family <- firstFamily +: familyList) {
        val fam = family(pops)
        families += fam.familyBytes
      }
      toQuery2
    }

    override def withColumnsInFamily[K, V](family: (T) => ColumnFamily[K, V], firstColumn: K, columnList: K*) = {
      val fam = family(pops)
      for (column <- firstColumn +: columnList) {
        columns += (fam.familyBytes -> fam.keyConverter.toBytes(column))
      }
      toQuery2
    }

    override def withColumn[K, V](family: (T) => ColumnFamily[K, V], columnName: K) = {
      val fam = family(pops)
      columns += (fam.familyBytes -> fam.keyConverter.toBytes(columnName))
      toQuery2
    }

    override def withColumn[K, V](column: (T) => Column[K, V]) = {
      val col = column(pops)
      columns += (col.familyBytes -> col.columnBytes)
      toQuery2
    }

    override def withColumns[K, V](firstColumn: (T) => Column[_, _], columnList: ((T) => Column[_, _])*) = {
      for (column <- firstColumn +: columnList) {
        val col = column(pops)
        columns += (col.familyBytes -> col.columnBytes)
      }
      toQuery2
    }

  }


  /**
   * An individual data modification operation (put, increment, or delete usually)
   * These operations are chained together by the client, and then executed in bulk.
   * @param table
   * @param key
   * @param previous
   * @tparam T
   * @tparam R
   */
  abstract class OpBase(key: Array[Byte], val previous: Buffer[OpBase] = Buffer[OpBase]()) {

    def getTableName = tableName
    previous += this

    def +(that:OpBase) : OpBase

    def put(key: R, writeToWAL: Boolean = true) = {
      val po = new PutOp(rowKeyConverter.toBytes(key), previous, writeToWAL)
      po
    }

    def increment(key: R) = {
      val inc = new IncrementOp(rowKeyConverter.toBytes(key), previous)
      inc
    }

    def delete(key: R) = {
      val del = new DeleteOp(rowKeyConverter.toBytes(key), previous)
      del
    }

    def size = previous.size

    def getOperations: Iterable[Mutation] = {
      val calls = Buffer[Mutation]()
      previous.foreach {
        case put: PutOp => {
          calls += put.put
        }
        case delete: DeleteOp => {
          calls += delete.delete
        }
        case increment: IncrementOp => {
          calls += increment.increment
        }
      }

      calls

    }

    /**
     * This is an experimental call that utilizes a shared instance of a table to flush writes.
     */
    def withExecuteBuffered(tableName: String = tableName) {

      val (ops, deletes, puts, increments) = prepareOperations

      if (ops.size == 0) {
      } else {
        withBufferedTable(tableName) {
          bufferTable =>
            bufferTable.batch(ops)
        }
      }

    }

    def prepareOperations = {
      val ops = Buffer[Row]()

      var puts = 0
      var increments = 0
      var deletes = 0

      previous.foreach {
        case put: PutOp => {
          if (!put.put.isEmpty) {
            ops += put.put
            puts += 1
          }
        }
        case delete: DeleteOp => {
          ops += delete.delete
          deletes += 1
        }
        case increment: IncrementOp => {
          ops += increment.increment
          increments += 1
        }
      }

      (ops, puts, deletes, increments)
    }

    def execute(tableName: String = tableName) = {
      val (ops, puts, deletes, increments) = prepareOperations

      if (ops.size == 0) {
        //No need to do anything if there are no real operations to execute
      } else {
        withTable(tableName) {
          table =>
            table.batch(ops)

          //          if (puts.size > 0) {
          //            table.put(puts)
          //            //IN THEORY, the operations will happen in order.  If not, break this into two different batched calls for deletes and puts
          //          }
          //          if (deletes.size > 0) {
          //            table.delete(deletes)
          //          }
          //          if (increments.size > 0) {
          //            increments.foreach(increment => table.increment(increment))
          //          }
        }
      }


      OpsResult(deletes, puts, increments)
    }
  }

  /**
   * A Put operation.  Can work across multiple columns or entire column families treated as Maps.
   * @param table
   * @param key
   * @param previous
   * @param writeToWAL
   * @tparam T
   * @tparam R
   */
  class PutOp(key: Array[Byte], previous: Buffer[OpBase] = Buffer[OpBase](), writeToWAL: Boolean = true) extends OpBase(key, previous) {
    val put = new Put(key)
    put.setWriteToWAL(writeToWAL)


    def +(that: OpBase) = new PutOp(key, previous ++ that.previous, writeToWAL)

    def value[K, V](column: (T) => Column[K, V], value: V, timeStamp: DateTime = null) = {
      val col = column(pops)
      if (timeStamp == null) {
        put.add(col.familyBytes, col.columnBytes, col.valueConverter.toBytes(value))
      } else {
        put.add(col.familyBytes, col.columnBytes, timeStamp.getMillis, col.valueConverter.toBytes(value))
      }
      this
    }

    def valueMap[K, V](family: (T) => ColumnFamily[K, V], values: Map[K, V]) = {
      val fam = family(pops)
      for ((key, value) <- values) {
        put.add(fam.familyBytes, fam.keyConverter.toBytes(key), fam.valueConverter.toBytes(value))
      }
      this
    }
  }

  /**
   * A deletion operation.  If nothing is specified but a key, will delete the whole row.
   * If a family is specified, will just delete the values in
   * that family.
   * @param table
   * @param key
   * @param previous
   * @tparam T
   * @tparam R
   */
  class DeleteOp(key: Array[Byte], previous: Buffer[OpBase] = Buffer[OpBase]()) extends OpBase(key, previous) {
    val delete = new Delete(key)

    def +(that: OpBase) = new DeleteOp(key, previous ++ that.previous)


    def family[K, V](family: (T) => ColumnFamily[K, V]) = {
      val fam = family(pops)
      delete.deleteFamily(fam.familyBytes)
      this
    }

    def values[K, V](family: (T) => ColumnFamily[K, V], qualifiers: Set[K]) = {
      val fam = family(pops)
      for (q <- qualifiers) {
        delete.deleteColumns(fam.familyBytes, fam.keyConverter.toBytes(q))
      }
      this
    }
  }

  /**
   * An increment operation -- can increment multiple columns in a single go.
   * @param table
   * @param key
   * @param previous
   * @tparam T
   * @tparam R
   */
  class IncrementOp(key: Array[Byte], previous: Buffer[OpBase] = Buffer[OpBase]()) extends OpBase(key, previous) {
    val increment = new Increment(key)
    increment.setWriteToWAL(false)

    def +(that: OpBase) = new IncrementOp(key, previous ++ that.previous)


    def value[K, Long](column: (T) => Column[K, Long], value: java.lang.Long) = {
      val col = column(pops)
      increment.addColumn(col.familyBytes, col.columnBytes, value)
      this
    }

    def valueMap[K, Long](family: (T) => ColumnFamily[K, Long], values: Map[K, Long]) = {
      val fam = family(pops)
      for ((key, value) <- values) {
        increment.addColumn(fam.familyBytes, fam.keyConverter.toBytes(key), value.asInstanceOf[java.lang.Long])
      }
      this
    }
  }

  class ScanQuery {
    val scan = new Scan()
    scan.setCaching(100)
    scan.setMaxVersions(1)

    val filterBuffer = scala.collection.mutable.Buffer[Filter]()

    def executeWithCaching(operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL, ttl: Int = 30): Seq[RR] = {
      completeScanner(operator)
      val results = cache.getScanResult(scan) match {
        case Some(result) => {
          result
        }
        case None => {
          val results = scala.collection.mutable.Buffer[RR]()
          withTable() {
            htable =>
              val scanner = htable.getScanner(scan)
              try {
                for (result <- scanner) {
                  results += buildRow(result)
                }
                cache.putScanResult(scan, results.toSeq, ttl)
                results
              } finally {
                scanner.close()
              }
          }
        }
      }

      results
    }

    def execute(handler: (RR) => Unit, operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL) {
      withTable() {
        htable =>
          completeScanner(operator)
          val scanner = htable.getScanner(scan)

          try {
            for (result <- scanner) {
              handler(buildRow(result))
            }
          } finally {
            scanner.close()
          }
      }
    }

    /*
    Prepares the scanner for use by chaining the filters together.  Should be called immediately before passing the scanner to the table.
     */
    def completeScanner(operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL) {
      if (filterBuffer.size > 0) {
        val filterList = new FilterList(operator)
        filterBuffer.foreach {filter => filterList.addFilter(filter)}
        scan.setFilter(filterList)
      }
    }

    def executeToSeq[I](handler: (RR) => I, operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL): Seq[I] = {
      val results = Buffer[I]()

      withTable() {
        htable =>
          completeScanner(operator)
          val scanner = htable.getScanner(scan)

          try {
            for (result <- scanner; if (result != null)) {
              results += handler(buildRow(result))
            }
          } finally {
            scanner.close()
          }
      }

      results.toSeq
    }

    def withFamily[K, V](family: FamilyExtractor[T, R, K, V]) = {
      val fam = family(pops)
      scan.addFamily(fam.familyBytes)
      this
    }

    def withColumn[K, V](column: ColumnExtractor[T, R, K, V]) = {
      val col = column(pops)
      scan.addColumn(col.familyBytes, col.columnBytes)

      this
    }

    def withFilter(f: () => Filter) = {
      filterBuffer.add(f())
      this
    }

    def addFilter(filter: Filter) = withFilter(() => filter)

    def withColumnOp[K, V](column: ColumnExtractor[T, R, K, V], compareOp: CompareOp, value: Option[V], excludeIfNull: Boolean)(implicit c: ByteConverter[V]) = {
      val col = column(pops)
      val filter = new SingleColumnValueFilter(
        col.familyBytes,
        col.columnBytes,
        compareOp,
        value match {case Some(v) => c.toBytes(v); case None => new Array[Byte](0)}
      )
      filter.setFilterIfMissing(excludeIfNull)
      filterBuffer += filter
      this
    }

    def withStartKey(key: R)(implicit c: ByteConverter[R]) = {scan.setStartRow(c.toBytes(key)); this}

    def withEndKey(key: R)(implicit c: ByteConverter[R]) = {scan.setStopRow(c.toBytes(key)); this}

    def withCaching(rowsToCache: Int) = {scan.setCaching(rowsToCache); this;}
  }


}
