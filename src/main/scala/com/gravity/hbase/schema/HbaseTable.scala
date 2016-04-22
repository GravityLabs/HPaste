package com.gravity.hbase.schema

import java.nio.ByteBuffer
import java.{lang, util}
import scala.collection.mutable.ArrayBuffer
import scala.collection._
import org.apache.commons.lang.ArrayUtils
import org.apache.hadoop.hbase.util.Bytes
import com.gravity.hbase.AnyConverterSignal
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator
import java.io.IOException
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
  def defaultConfig: HbaseTableConfig = HbaseTableConfig()
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
abstract class HbaseTable[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](val tableName: String, var cache: QueryResultCache[T, R, RR] = new NoOpCache[T, R, RR](), rowKeyClass: Class[R], logSchemaInconsistencies: Boolean = false, tableConfig:HbaseTableConfig = HbaseTable.defaultConfig)(implicit keyConverter: ByteConverter[R])
  extends TablePoolStrategy
{

  def rowBuilder(result: DeserializedResult): RR

  def getTableConfig: HbaseTableConfig = tableConfig

  def emptyRow(key:Array[Byte]): RR = rowBuilder(new DeserializedResult(rowKeyConverter.fromBytes(key).asInstanceOf[AnyRef],families.size))
  def emptyRow(key:R): RR = rowBuilder(new DeserializedResult(key.asInstanceOf[AnyRef],families.size))

  val rowKeyConverter: ByteConverter[R] = keyConverter

  /**Provides the client with an instance of the superclass this table was defined against. */
  def pops: T = this.asInstanceOf[T]

  /**A method injected by the super class that will build a strongly-typed row object.  */
  def buildRow(result: Result): RR = {
    rowBuilder(convertResult(result))
  }

  @volatile private var famLookup: Array[Array[Byte]] = null
  @volatile private var colFamLookup: Array[Array[Byte]] = null
  @volatile private var famIdx: IndexedSeq[KeyValueConvertible[_, _, _]] = null
  @volatile private var colFamIdx: IndexedSeq[KeyValueConvertible[_, _, _]] = null

  private val bc: ByteArrayComparator = new ByteArrayComparator()

  implicit private val o: Ordering[Array[Byte]] = new math.Ordering[Array[Byte]] {
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
  def converterByBytes(famBytes: Array[Byte], colBytes: Array[Byte]): KeyValueConvertible[_, _, _] = {

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
  def convertResult(result: Result): DeserializedResult = {
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
          ds.add(f, k, r, cell.getTimestamp)
        } else {
          if (logSchemaInconsistencies) {
            println("Table: " + tableName + " : Any Converter : " + Bytes.toString(cell.getFamilyArray))
          }
        }
      }
    }
    ds
  }



  def familyBytes: ArrayBuffer[Array[Byte]] = families.map(family => family.familyBytes)

  def familyByIndex(idx: Int): Fam[_, _] = familyArray(idx)

  private lazy val familyArray: Array[Fam[ _,_]] = {
    val arr = new Array[Fam[_, _]](families.length)
    families.foreach {
      fam =>
        arr(fam.index) = fam
    }
    arr
  }

  def columnByIndex(idx: Int): TypedCol[_, _] = columnArray(idx)

  lazy val columnArray: Array[TypedCol[_, _]] = {
    val arr = new Array[TypedCol[_, _]](columns.length)
    columns.foreach {col => arr(col.columnIndex) = col}
    arr
  }


  //alter 'articles', NAME => 'html', VERSIONS =>1, COMPRESSION=>'lzo'

  /**
   * Generates a creation script for the table, based on the column families and table config.
   * @param tableNameOverride
   * @return
   */
  def createScript(tableNameOverride: String = tableName): String = {
    var create = "create '" + tableNameOverride + "', "
    create += (for (family <- families) yield {
      familyDef(family)
    }).mkString(",")

    create += alterTableAttributesScripts(tableNameOverride)

    create
  }

  def alterTableAttributesScripts(tableName:String): String = {
    var alterScript = ""
    if(tableConfig.memstoreFlushSizeInBytes > -1) {
      alterScript += alterTableAttributeScript(tableName, "MEMSTORE_FLUSHSIZE", tableConfig.memstoreFlushSizeInBytes.toString)
    }
    if(tableConfig.maxFileSizeInBytes > -1) {
      alterScript += alterTableAttributeScript(tableName, "MAX_FILESIZE", tableConfig.maxFileSizeInBytes.toString)
    }
    alterScript
  }

  def alterTableAttributeScript(tableName:String, attributeName:String, value:String): String = {
    "\nalter '" + tableName + "', {METHOD => 'table_att', "+attributeName+" => '" + value + "'}"
  }

  def deleteScript(tableNameOverride: String = tableName): String = {
    val delete = "disable '" + tableNameOverride + "'\n"

    delete + "delete '" + tableNameOverride + "'"
  }

  /**
   * Generates a production-friendly alter script (flush, disable, alter, enable)
   * @param tableNameOverride
   * @param families
   * @return
   */
  def alterScript(tableNameOverride: String = tableName, families: Seq[Fam[_, _]] = families): String = {

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

  def familyDef(family: Fam[_, _]): String = {
    val compression = if (family.compressed) ", COMPRESSION=>'lzo'" else ""
    val ttl = if (family.ttlInSeconds < HColumnDescriptor.DEFAULT_TTL) ", TTL=>'" + family.ttlInSeconds + "'" else ""
    "{NAME => '%s', VERSIONS => %d%s%s}".format(Bytes.toString(family.familyBytes), family.versions, compression, ttl)
  }

  private val columns: ArrayBuffer[TypedCol[_, _]] = ArrayBuffer[TypedCol[_, _]]()
  val families: ArrayBuffer[Fam[_, _]] = ArrayBuffer[Fam[_, _]]()

  val columnsByName: mutable.Map[AnyRef, TypedCol[_, _]] = mutable.Map[AnyRef, TypedCol[_, _]]()

  private val columnsByBytes: mutable.Map[ByteBuffer, KeyValueConvertible[_, _, _]] = mutable.Map[ByteBuffer, KeyValueConvertible[_, _, _]]()
  private val familiesByBytes: mutable.Map[ByteBuffer, KeyValueConvertible[_, _, _]] = mutable.Map[ByteBuffer, KeyValueConvertible[_, _, _]]()

  private var columnIdx: Int = 0

  def columnTyped[K,V](columnFamily: Fam[K, _], columnName: K, valueClass: Class[V])(implicit ck: ByteConverter[K], kv: ByteConverter[V]): TypedCol[K, V] = {
    val c = new TypedCol[K, V](columnFamily, columnName, columnIdx)
    columns += c

    val famBytes = columnFamily.familyBytes
    val colBytes = ck.toBytes(columnName)
    val fullKey = ArrayUtils.addAll(famBytes, colBytes)
    val bufferKey = ByteBuffer.wrap(fullKey)

    columnsByName.put(columnName.asInstanceOf[AnyRef], c)
    columnsByBytes.put(bufferKey, c)
    columnIdx = columnIdx + 1
    c
  }

  def columnTyped[K,V](columnFamily: Fam[K, _], columnName: K)(implicit ck: ByteConverter[K], kv: ByteConverter[V]): TypedCol[K, V] = {
    val c = new TypedCol[K, V](columnFamily, columnName, columnIdx)
    columns += c

    val famBytes = columnFamily.familyBytes
    val colBytes = ck.toBytes(columnName)
    val fullKey = ArrayUtils.addAll(famBytes, colBytes)
    val bufferKey = ByteBuffer.wrap(fullKey)

    columnsByName.put(columnName.asInstanceOf[AnyRef], c)
    columnsByBytes.put(bufferKey, c)
    columnIdx = columnIdx + 1
    c
  }


  def column[V](columnFamily:Fam[String,_], columnName:String)(implicit kv:ByteConverter[V]) : Col[V] = {
    val c = new Col[V](columnFamily, columnName, columnIdx)
    columns += c

    val famBytes = columnFamily.familyBytes
    val colBytes = StringConverter.toBytes(columnName)
    val fullKey = ArrayUtils.addAll(famBytes, colBytes)
    val bufferKey = ByteBuffer.wrap(fullKey)

    columnsByName.put(columnName.asInstanceOf[AnyRef], c)
    columnsByBytes.put(bufferKey, c)
    columnIdx = columnIdx + 1
    c
  }

  def column[V](columnFamily: Fam[String, _], columnName: String, valueClass: Class[V])(implicit kv: ByteConverter[V]): Col[V] = {
    val c = new Col[V](columnFamily, columnName, columnIdx)
    columns += c

    val famBytes = columnFamily.familyBytes
    val colBytes = StringConverter.toBytes(columnName)
    val fullKey = ArrayUtils.addAll(famBytes, colBytes)
    val bufferKey = ByteBuffer.wrap(fullKey)

    columnsByName.put(columnName.asInstanceOf[AnyRef], c)
    columnsByBytes.put(bufferKey, c)
    columnIdx = columnIdx + 1
    c
  }

  private var familyIdx = 0

  def family[K, V](familyName: String, compressed: Boolean = false, versions: Int = 1, rowTtlInSeconds: Int = Int.MaxValue)(implicit d: ByteConverter[K], e: ByteConverter[V]): Fam[K, V] = {
    val family = new Fam[K, V](familyName, compressed, versions, familyIdx, rowTtlInSeconds)
    familyIdx = familyIdx + 1
    families += family
    familiesByBytes.put(ByteBuffer.wrap(family.familyBytes), family)
    family
  }

  def getTableOption(name: String, conf: Configuration, timeOutMs: Int): Option[HTableInterface] = {
    try {
      Some(getTable(this, conf, timeOutMs))
    } catch {
      case e: Exception => None
    }
  }


  def withTableOption[Q](name: String, conf: Configuration, timeOutMs: Int)(work: (Option[HTableInterface]) => Q): Q = {
    val table = getTableOption(name, conf, timeOutMs)
    try {
      work(table)
    } finally {
      table foreach (tbl => releaseTable(this,tbl))
    }
  }

  def withTable[Q](mytableName: String, conf: Configuration, timeOutMs: Int)(funct: (HTableInterface) => Q): Q = {
    withTableOption(mytableName, conf, timeOutMs) {
      case Some(table) => {
        funct(table)
      }
      case None => throw new RuntimeException("Table " + tableName + " does not exist")
    }
  }

  def query2: Query2Builder[T, R, RR] = new Query2Builder(this)

  def put(key: R, writeToWAL: Boolean = true): PutOp[T, R] = new PutOp(this, keyConverter.toBytes(key), writeToWAL = writeToWAL)

  def delete(key: R): DeleteOp[T, R] = new DeleteOp(this, keyConverter.toBytes(key))

  def increment(key: R): IncrementOp[T, R] = new IncrementOp(this, keyConverter.toBytes(key))


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

  /**
   * Represents the specification of a Column.
   */
  class Col[V](columnFamily: Fam[String, _], override val columnName: String, override val columnIndex: Int)(implicit kv: ByteConverter[V])
    extends TypedCol[String, V](columnFamily, columnName, columnIndex) {
    override val columnBytes: Array[Byte] = StringConverter.toBytes(columnName)
    override val familyBytes: Array[Byte] = columnFamily.familyBytes
    override val columnNameRef: AnyRef = columnName.asInstanceOf[AnyRef]

    override val familyConverter: StringConverter.type = StringConverter
    override val keyConverter: StringConverter.type = StringConverter
    override val valueConverter: ByteConverter[V] = kv

    override def getQualifier: String = columnName

    override def family: ColumnFamily[_, _, _, _, _] = columnFamily.asInstanceOf[ColumnFamily[_, _, _, _, _]]


  }


  /**
   * Represents the specification of a Column.
   */
  class TypedCol[K, V](columnFamily: Fam[K, _], override val columnName: K, override val columnIndex: Int)(implicit kc: ByteConverter[K], kv: ByteConverter[V])
    extends Column[T,R,String, K, V](this,columnFamily, columnName, columnIndex) {
    override val columnBytes: Array[Byte] = kc.toBytes(columnName)
    override val familyBytes: Array[Byte] = columnFamily.familyBytes
    override val columnNameRef: AnyRef = columnName.asInstanceOf[AnyRef]

    override val familyConverter: StringConverter.type = StringConverter
    override val keyConverter: ByteConverter[K] = kc
    override val valueConverter: ByteConverter[V] = kv

    override def getQualifier: K = columnName

    override def family: ColumnFamily[_, _, _, _, _] = columnFamily.asInstanceOf[ColumnFamily[_, _, _, _, _]]


  }

  /**
   * Represents the specification of a Column Family
   */
  class Fam[K, V](override val familyName: String, override val compressed: Boolean = false, override val versions: Int = 1, override val index: Int, override val ttlInSeconds: Int = HColumnDescriptor.DEFAULT_TTL)(implicit d: ByteConverter[K], e: ByteConverter[V])
    extends ColumnFamily[T,R,String, K, V](this, familyName,compressed,versions,index, ttlInSeconds) {
    override val familyConverter: StringConverter.type = StringConverter
    override val keyConverter: ByteConverter[K] = d
    override val valueConverter: ByteConverter[V] = e
    override val familyBytes: Array[Byte] = familyConverter.toBytes(familyName)


    override def family: Fam[K, V] = this
  }



}


/**
 * Represents the specification of a Column Family
 */
class ColumnFamily[T <: HbaseTable[T, R, _], R, F, K, V](val table: HbaseTable[T, R, _], val familyName: F, val compressed: Boolean = false, val versions: Int = 1, val index: Int, val ttlInSeconds: Int = HColumnDescriptor.DEFAULT_TTL)(implicit c: ByteConverter[F], d: ByteConverter[K], e: ByteConverter[V]) extends KeyValueConvertible[F, K, V] {
  val familyConverter: ByteConverter[F] = c
  val keyConverter: ByteConverter[K] = d
  val valueConverter: ByteConverter[V] = e
  val familyBytes: Array[Byte] = c.toBytes(familyName)


  def family: ColumnFamily[T, R, F, K, V] = this
}

/**
 * Represents the specification of a Column.
 */
class Column[T <: HbaseTable[T, R, _], R, F, K, V](table: HbaseTable[T, R, _], columnFamily: ColumnFamily[T, R, F, K, _], val columnName: K, val columnIndex: Int)(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) extends KeyValueConvertible[F, K, V] {
  val columnBytes: Array[Byte] = kc.toBytes(columnName)
  val familyBytes: Array[Byte] = columnFamily.familyBytes
  val columnNameRef: AnyRef = columnName.asInstanceOf[AnyRef]

  val familyConverter: ByteConverter[F] = fc
  val keyConverter: ByteConverter[K] = kc
  val valueConverter: ByteConverter[V] = kv

  def getQualifier: K = columnName

  def family: ColumnFamily[_, _, _, _, _] = columnFamily.asInstanceOf[ColumnFamily[_, _, _, _, _]]


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

  def keyToBytes(key: K): Array[Byte] = keyConverter.toBytes(key)

  def valueToBytes(value: V): Array[Byte] = valueConverter.toBytes(value)

  def keyToBytesUnsafe(key: AnyRef): Array[Byte] = keyConverter.toBytes(key.asInstanceOf[K])

  def valueToBytesUnsafe(value: AnyRef): Array[Byte] = valueConverter.toBytes(value.asInstanceOf[V])

  def keyFromBytesUnsafe(bytes: Array[Byte]): AnyRef = keyConverter.fromBytes(bytes).asInstanceOf[AnyRef]

  def valueFromBytesUnsafe(bytes: Array[Byte]): AnyRef = valueConverter.fromBytes(bytes).asInstanceOf[AnyRef]

  def family: ColumnFamily[_, _, _, _, _]
}
