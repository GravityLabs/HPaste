package com.gravity.hbase.schema

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import java.io._
import org.apache.hadoop.io.{BytesWritable, Writable}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{Filter, FilterList, SingleColumnValueFilter}
import scala.collection._
import java.util.NavigableSet
import scala.collection.mutable.Buffer
import org.joda.time.DateTime

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class ScanCachePolicy(ttlMinutes: Int)

trait QueryResultCache[T <: HbaseTable[T, R], R] {

  def getScanResult(key: Scan): Option[Seq[QueryResult[T, R]]]

  def putScanResult(key: Scan, value: Seq[QueryResult[T, R]], ttl: Int)

  def getResult(key: Get): Option[QueryResult[T, R]]

  def putResult(key: Get, value: QueryResult[T, R], ttl: Int)
}

class NoOpCache[T <: HbaseTable[T, R], R] extends QueryResultCache[T, R] {

  override def getScanResult(key: Scan): Option[Seq[QueryResult[T, R]]] = None

  override def putScanResult(key: Scan, value: Seq[QueryResult[T, R]], ttl: Int) {}

  override def getResult(key: Get): Option[QueryResult[T, R]] = None


  override def putResult(key: Get, value: QueryResult[T, R], ttl: Int) {}
}

/**
* Class to be implemented by custom converters
*/
abstract class ByteConverter[T] {
  def toBytes(t: T): Array[Byte]

  def fromBytes(bytes: Array[Byte]): T

  def fromByteString(str: String): T = {
    fromBytes(Bytes.toBytesBinary(str))
  }

  def toByteString(item: T) = {
    Bytes.toStringBinary(toBytes(item))
  }

  def toBytesWritable(t: T): BytesWritable = {
    new BytesWritable(toBytes(t))
  }

  def fromBytesWritable(bytes: BytesWritable): T = {
    fromBytes(bytes.getBytes)
  }
}

/**
* Simple high performance conversions from complex types to bytes
*/
abstract class ComplexByteConverter[T] extends ByteConverter[T] {
  override def toBytes(t: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()

    val dout = new DataOutputStream(bos)
    write(t, dout)

    bos.toByteArray
  }

  def write(data: T, output: DataOutputStream)

  override def fromBytes(bytes: Array[Byte]): T = {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))
    read(din)
  }

  def read(input: DataInputStream): T
}


class MapConverter[K, V](implicit c: ByteConverter[K], d: ByteConverter[V]) extends ComplexByteConverter[Map[K, V]] {
  override def write(map: Map[K, V], output: DataOutputStream) {
    val length = map.size
    output.writeInt(length)

    for ((k, v) <- map) {
      val keyBytes = c.toBytes(k)
      val valBytes = d.toBytes(v)
      output.writeInt(keyBytes.size)
      output.write(keyBytes)
      output.writeInt(valBytes.size)
      output.write(valBytes)
    }
  }

  override def read(input: DataInputStream) = {
    val length = input.readInt()
    //    val map = mutable.Map[K,V]()
    Map[K, V]((for (i <- 0 until length) yield {
      val keyLength = input.readInt
      val keyArr = new Array[Byte](keyLength)
      input.read(keyArr)
      val key = c.fromBytes(keyArr)

      val valLength = input.readInt
      val valArr = new Array[Byte](valLength)
      input.read(valArr)
      val value = d.fromBytes(valArr)

      (key -> value)
    }): _*)
  }
}



class SetConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[Set[T]] {

  override def write(set: Set[T], output: DataOutputStream) {
    val length = set.size
    output.writeInt(length)

    set.foreach{itm=>
      val bytes = c.toBytes(itm)
      output.writeInt(bytes.size)
      output.write(bytes)
    }
  }

  override def read(input:DataInputStream) : Set[T] = {
    val length = input.readInt()
    Set((for(i <- 0 until length) yield {
      val arrLength = input.readInt()
      val arr = new Array[Byte](arrLength)
      input.read(arr)
      c.fromBytes(arr)
    }):_*)
  }
}


class SeqConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[Seq[T]] {
  override def write(seq: Seq[T], output: DataOutputStream) {
    writeSeq(seq, output)
  }

  def writeSeq(seq: Seq[T], output: DataOutputStream) {
    val length = seq.size
    output.writeInt(length)

    for (t <- seq) {
      val bytes = c.toBytes(t)
      output.writeInt(bytes.size)
      output.write(bytes)
    }
  }

  override def read(input: DataInputStream) = readSeq(input)

  def readSeq(input: DataInputStream) = {
    val length = input.readInt()

    Seq((for (i <- 0 until length) yield {
      val arrLength = input.readInt()
      val arr = new Array[Byte](arrLength)
      input.read(arr)
      c.fromBytes(arr)
    }):_*)
  }

}

class BufferConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[Buffer[T]] {
  override def write(buf: Buffer[T], output: DataOutputStream) {
    writeBuf(buf, output)
  }

  def writeBuf(buf: Buffer[T], output: DataOutputStream) {
    val length = buf.size
    output.writeInt(length)

    for (t <- buf) {
      val bytes = c.toBytes(t)
      output.writeInt(bytes.size)
      output.write(bytes)
    }
  }

  override def read(input: DataInputStream) = readBuf(input)

  def readBuf(input: DataInputStream) = {
    val length = input.readInt()

    Buffer((for (i <- 0 until length) yield {
      val arrLength = input.readInt()
      val arr = new Array[Byte](arrLength)
      input.read(arr)
      c.fromBytes(arr)
    }):_*)
  }

}


/**
* When a query comes back, there are a bucket of column families and columns to retrieve.  This class retrieves them.
*/
class QueryResult[T <: HbaseTable[T, R], R](val result: Result, table: HbaseTable[T, R], val tableName: String) extends Serializable {
  def column[F, K, V](column: (T) => Column[T, R, F, K, V])(implicit c: ByteConverter[V]): Option[V] = {
    val co = column(table.pops)
    val col = result.getColumnLatest(co.familyBytes, co.columnBytes)
    if (col != null) {
      Some(c.fromBytes(col.getValue))
    } else {
      None
    }
  }

  def columnFromFamily[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], columnName: K)(implicit c: ByteConverter[K], d: ByteConverter[V]): Option[V] = {
    val fam = family(table.pops)
    val qual = c.toBytes(columnName)
    val kvs = result.raw()
    kvs.find(kv => Bytes.equals(kv.getFamily, fam.familyBytes) && Bytes.equals(kv.getQualifier, qual)) match {
      case Some(v) => Some(d.fromBytes(v.getValue))
      case None => None
    }
  }

  def columnTimestamp[F,K,V](column: (T) => Column[T,R,F,K,V])(implicit c: ByteConverter[V]): Option[DateTime] = {
    val co = column(table.pops)
    val col = result.getColumnLatest(co.familyBytes, co.columnBytes)
    if(col != null) {
      Some(new DateTime(col.getTimestamp))
    }else {
      None
    }
  }

  def familyLatestTimestamp[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V])(implicit c: ByteConverter[F], d: ByteConverter[K], e: ByteConverter[V]): Option[DateTime] = {
    val fam = family(table.pops)
    var ts = -1l
    for(kv <- result.raw()) {
      if(Bytes.equals(kv.getFamily,fam.familyBytes)) {
        val tsn = kv.getTimestamp
        if(tsn > ts) ts = tsn
      }
    }
    if(ts >= 0) Some(new DateTime(ts))
    else None
  }

  def family[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V])(implicit c: ByteConverter[F], d: ByteConverter[K], e: ByteConverter[V]): Map[K, V] = {
    val fm = family(table.pops)
    val kvs = result.raw()
    val mymap = scala.collection.mutable.Map[K,V]()
    mymap.sizeHint(kvs.size)
    for(kv <- kvs) yield {
      if(Bytes.equals(kv.getFamily, fm.familyBytes)) {
        mymap.put(d.fromBytes(kv.getQualifier), e.fromBytes(kv.getValue))
      }
    }
    mymap
  }

  def familyKeySet[F, K](family: (T) => ColumnFamily[T, R, F, K, _])(implicit c: ByteConverter[F], d: ByteConverter[K]): Set[K] = {
    val fm = family(table.pops)
    val kvs = result.raw()
    val myset = scala.collection.mutable.Set[K]()
    myset.sizeHint(kvs.size)
    for(kv <- kvs) yield {
      if(Bytes.equals(kv.getFamily, fm.familyBytes)) {
        myset.add(d.fromBytes(kv.getQualifier))
      }
    }
    myset
  }

  def rowid(implicit c: ByteConverter[R]) = c.fromBytes(result.getRow)

  def getTableName = tableName
}

/**
* A query for setting up a scanner across the whole table or key subsets.
* There is a lot of room for expansion in this class -- caching parameters, scanner specs, key-only, etc.
*/
class ScanQuery[T <: HbaseTable[T, R], R](table: HbaseTable[T, R]) {
  val scan = new Scan()
  scan.setCaching(100)
  scan.setMaxVersions(1)

  val filterBuffer = scala.collection.mutable.Buffer[Filter]()

  def executeWithCaching(operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL, ttl: Int = 30): Seq[QueryResult[T, R]] = {
    completeScanner(operator)
    val results = table.cache.getScanResult(scan) match {
      case Some(result) => {
        println("cache hit against key " + scan.toString)
        result
      }
      case None => {
        println("cache miss against key " + scan.toString)
        val results = scala.collection.mutable.Buffer[QueryResult[T, R]]()
        table.withTable() {
          htable =>
            val scanner = htable.getScanner(scan)
            try {
              for (result <- scanner) {
                results += new QueryResult[T, R](result, table, table.tableName)
              }
              table.cache.putScanResult(scan, results.toSeq, ttl)
              results
            } finally {
              scanner.close()
            }
        }
      }
    }

    results
  }

  def execute(handler: (QueryResult[T, R]) => Unit, operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL) {
    table.withTable() {
      htable =>
        completeScanner(operator)
        val scanner = htable.getScanner(scan)

        try {
          for (result <- scanner) {
            handler(new QueryResult[T, R](result, table, table.tableName))
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

  def executeToSeq[I](handler: (QueryResult[T, R]) => I, operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL): Seq[I] = {
    val results = Buffer[I]()

    table.withTable() {
      htable =>
        completeScanner(operator)
        val scanner = htable.getScanner(scan)

        try {
          for (result <- scanner; if (result != null)) {
            results += handler(new QueryResult[T, R](result, table, table.tableName))
          }
        } finally {
          scanner.close()
        }
    }

    results.toSeq
  }

  def withFamily[F, K, V](family: FamilyExtractor[T, R, F, K, V]) = {
    val fam = family(table.pops)
    scan.addFamily(fam.familyBytes)
    this
  }

  def withFilter(f: () => Filter) = {
    filterBuffer.add(f())
    this
  }

  def addFilter(filter: Filter) = withFilter(() => filter)

  def withColumnOp[F, K, V](column: ColumnExtractor[T, R, F, K, V], compareOp: CompareOp, value: Option[V], excludeIfNull: Boolean)(implicit c: ByteConverter[V]) = {
    val col = column(table.pops)
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

  def withStartKey[R](key: R)(implicit c: ByteConverter[R]) = {scan.setStartRow(c.toBytes(key)); this}

  def withEndKey[R](key: R)(implicit c: ByteConverter[R]) = {scan.setStopRow(c.toBytes(key)); this}

  def withCaching(rowsToCache: Int) = {scan.setCaching(rowsToCache); this;}
}

/**
* An individual data modification operation (put, increment, or delete usually)
* These operations are chained together by the client, and then executed in bulk.
*/
class OpBase[T <: HbaseTable[T, R], R](table: HbaseTable[T, R], key: Array[Byte], previous: Buffer[OpBase[T, R]] = Buffer[OpBase[T, R]]()) {

  previous += this

  def put(key: R)(implicit c: ByteConverter[R]) = {
    val po = new PutOp(table, c.toBytes(key), previous)
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

  def execute(tableName: String = table.tableName) = {
    val puts = Buffer[Put]()
    val deletes = Buffer[Delete]()
    val increments = Buffer[Increment]()
    table.withTable(tableName) {
      table =>

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

        if (deletes.size > 0 || puts.size > 0) {
          //IN THEORY, the operations will happen in order.  If not, break this into two different batched calls for deletes and puts
          table.batch(deletes ++ puts)
        }
        if (increments.size > 0) {
          increments.foreach(increment => table.increment(increment))
        }
    }

    OpsResult(deletes.size, puts.size, increments.size)
  }
}

case class OpsResult(numDeletes: Int, numPuts: Int, numIncrements: Int)

/**
* An increment operation -- can increment multiple columns in a single go.
*/
class IncrementOp[T <: HbaseTable[T, R], R](table: HbaseTable[T, R], key: Array[Byte], previous: Buffer[OpBase[T, R]] = Buffer[OpBase[T, R]]()) extends OpBase[T, R](table, key, previous) {
  val increment = new Increment(key)

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
class PutOp[T <: HbaseTable[T, R], R](table: HbaseTable[T, R], key: Array[Byte], previous: Buffer[OpBase[T, R]] = Buffer[OpBase[T, R]]()) extends OpBase[T, R](table, key, previous) {
  val put = new Put(key)

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
class DeleteOp[T <: HbaseTable[T, R], R](table: HbaseTable[T, R], key: Array[Byte], previous: Buffer[OpBase[T, R]] = Buffer[OpBase[T, R]]()) extends OpBase[T, R](table, key, previous) {
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
class Query[T <: HbaseTable[T, R], R](table: HbaseTable[T, R]) {

  val keys = Buffer[Array[Byte]]()
  val families = Buffer[Array[Byte]]()
  val columns = Buffer[(Array[Byte], Array[Byte])]()


  def withKey(key: R)(implicit c: ByteConverter[R]) = {
    keys += c.toBytes(key)
    this
  }

  def withKeys(keys: Set[R])(implicit c: ByteConverter[R]) = {
    for (key <- keys) {
      withKey(key)(c)
    }
    this
  }

  def withColumnFamily[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V])(implicit c: ByteConverter[F]): Query[T, R] = {
    val fam = family(table.pops)
    families += c.toBytes(fam.familyName)
    this
  }

  def withColumn[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], columnName: K)(implicit c: ByteConverter[F], d: ByteConverter[K]): Query[T, R] = {
    val fam = family(table.pops)
    columns += (fam.familyBytes -> d.toBytes(columnName))
    this
  }

  def withColumn[F, K, V](column: (T) => Column[T, R, F, K, V])(implicit c: ByteConverter[K]): Query[T, R] = {
    val col = column(table.pops)
    columns += (col.familyBytes -> col.columnBytes)
    this
  }

  def single(tableName: String = table.tableName, ttl: Int = 30, skipCache: Boolean = true) = singleOption(tableName, ttl, skipCache, false).get

  def singleOption(tableName: String = table.tableName, ttl: Int = 30, skipCache: Boolean = true, noneOnEmpty: Boolean = true): Option[QueryResult[T, R]] = {
    require(keys.size == 1, "Calling single() with more than one key")
    val get = new Get(keys.head)
    get.setMaxVersions(1)
    for (family <- families) {
      get.addFamily(family)
    }
    for ((columnFamily, column) <- columns) {
      get.addColumn(columnFamily, column)
    }

    val fromCache = if (skipCache) None else table.cache.getResult(get)

    fromCache match {
      case Some(result) => Some(result)
      case None => {
        table.withTableOption(tableName) {
          case Some(htable) => {
            val result = htable.get(get)
            if (noneOnEmpty && result.isEmpty) {
              None
            } else {
              val qr = new QueryResult(result, table, tableName)
              if (!skipCache  && !result.isEmpty) table.cache.putResult(get, qr, ttl)
              Some(qr)
            }
          }
          case None => None
        }
      }
    }

  }

  def execute(tableName: String = table.tableName, ttl: Int = 30, skipCache: Boolean = true): Seq[QueryResult[T, R]] = {
    if (keys.isEmpty) return Seq.empty[QueryResult[T, R]] // no keys..? nothing to see here... move along... move along.

    val results = Buffer[QueryResult[T, R]]() // buffer for storing all results retrieved

    // if we are utilizing cache, we'll need to be able to recall the `Get' later to use as the cache key
    val getsByKey = if (skipCache) mutable.Map.empty[String, Get] else mutable.Map[String, Get]()

    if (!skipCache) getsByKey.sizeHint(keys.size) // perf optimization

    // buffer for all `Get's that really need to be gotten
    val cacheMisses = Buffer[Get]()

    val gets = buildGetsAndCheckCache(skipCache) {
      case (get: Get, key: Array[Byte]) => if (!skipCache) getsByKey.put(new String(key), get)
    } {
      case (qropt: Option[QueryResult[T, R]], get: Get) => if (!skipCache) qropt match {
        case Some(result) => results += result // got it! place it in our result buffer
        case None => cacheMisses += get // missed it! place the get in the buffer
      }
    }

    // identify what still needs to be `Get'ed ;-}
    val hbaseGets = if (skipCache) gets else cacheMisses

    if (!hbaseGets.isEmpty) { // only do this if we have something to do
      table.withTable(tableName) {
        htable =>
          htable.get(hbaseGets).foreach(res => {
            if (res != null && !res.isEmpty) { // ignore empty results
              val qr = new QueryResult[T, R](res, table, tableName) // construct query result

              // now is where we need to retrive the 'get' used for this result so that we can
              // pass this 'get' as the key for our local cache
              if (!skipCache) table.cache.putResult(getsByKey(new String(res.getRow)), qr, ttl)
              results += qr // place it in our result buffer
            }
          })
      }
    }

    results.toSeq // DONE!
  }

  def executeMap(tableName: String = table.tableName, ttl: Int = 30, skipCache: Boolean = true)(implicit c: ByteConverter[R]): Map[R, QueryResult[T, R]] = {
    if (keys.isEmpty) return Map.empty[R, QueryResult[T, R]] // don't get all started with nothing to do

    // init our result map and give it a hint of the # of keys we have
    val resultMap = mutable.Map[R, QueryResult[T, R]]()
    resultMap.sizeHint(keys.size) // perf optimization

    // if we are utilizing cache, we'll need to be able to recall the `Get' later to use as the cache key
    val getsByKey = if (skipCache) mutable.Map.empty[String, Get] else mutable.Map[String, Get]()

    if (!skipCache) getsByKey.sizeHint(keys.size) // perf optimization

    // buffer for all `Get's that really need to be gotten
    val cacheMisses = Buffer[Get]()

    val gets = buildGetsAndCheckCache(skipCache) {
      case (get: Get, key: Array[Byte]) => if (!skipCache) getsByKey.put(new String(key), get)
    } {
      case (qropt: Option[QueryResult[T, R]], get: Get) => if (!skipCache) qropt match {
        case Some(result) => resultMap.put(result.rowid, result) // got it! place it in our result map
        case None => cacheMisses += get // missed it! place the get in the buffer
      }
    }

    // identify what still needs to be `Get'ed ;-}
    val hbaseGets = if (skipCache) gets else cacheMisses

    if (!hbaseGets.isEmpty) { // only do this if we have something to do
      table.withTable(tableName) {
        htable =>
          htable.get(hbaseGets).foreach(res => {
            if (res != null && !res.isEmpty) { // ignore empty results
              val qr = new QueryResult[T, R](res, table, tableName) // construct query result

              // now is where we need to retrive the 'get' used for this result so that we can
              // pass this 'get' as the key for our local cache
              if (!skipCache) table.cache.putResult(getsByKey(new String(res.getRow)), qr, ttl)
              resultMap(qr.rowid) = qr // place it in our result map
            }
          })
      }
    }

    resultMap // DONE!
  }

  private def buildGetsAndCheckCache(skipCache: Boolean)(receiveGetAndKey: (Get,Array[Byte]) => Unit = (get, key) => {})(receiveCachedResult: (Option[QueryResult[T, R]], Get) => Unit = (qr, get) => {}): Seq[Get] = {
    if (keys.isEmpty) return Seq.empty[Get] // no keys..? nothing to see here... move along... move along.

    val gets = Buffer[Get]() // buffer for the raw `Get's

    for (key <- keys) {
      val get = new Get(key)
      gets += get
      receiveGetAndKey(get, key)
    }

    // since the families and columns will be identical for all `Get's, only build them once
    val firstGet = gets(0)

    // add all families to the first `Get'
    for (family <- families) {
      firstGet.addFamily(family)
    }
    // add all columns to the first `Get'
    for ((columnFamily, column) <- columns) {
      firstGet.addColumn(columnFamily, column)
    }

    var pastFirst = false
    for (get <- gets) {
      if (pastFirst) { // we want to skip the first `Get' as it already has families/columns

        // for all subsequent `Get's, we will build their familyMap from the first `Get'
        firstGet.getFamilyMap.foreach((kv: (Array[Byte], NavigableSet[Array[Byte]])) => {
          get.getFamilyMap.put(kv._1, kv._2)
        })
      } else pastFirst = true

      // try the cache with this filled in get
      if (!skipCache) receiveCachedResult(table.cache.getResult(get), get)
    }

    gets
  }

}

/**
* Represents the specification of a Column Family
*/
class ColumnFamily[T <: HbaseTable[T, R], R, F, K, V](val table: HbaseTable[T, R], val familyName: F, val compressed: Boolean = false, val versions: Int = 1)(implicit c: ByteConverter[F]) {
  val familyBytes = c.toBytes(familyName)
}

/**
* Represents the specification of a Column.
*/
class Column[T <: HbaseTable[T, R], R, F, K, V](table: HbaseTable[T, R], columnFamily: F, columnName: K)(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) {
  val columnBytes = kc.toBytes(columnName)
  val familyBytes = fc.toBytes(columnFamily)

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
  val tables = scala.collection.mutable.Set[HbaseTable[_, _]]()

  def table[T <: HbaseTable[T, _], _](table: T) = {
    tables += table
    table
  }

}

/**
* Represents a Table.  Expects an instance of HBaseConfiguration to be present.
* A parameter-type T should be the actual table that is implementing this one (this is to allow syntactic sugar for easily specifying columns during
* queries).
* A parameter-type R should be the type of the key for the table.  
*/
class HbaseTable[T <: HbaseTable[T, R], R](val tableName: String, var cache: QueryResultCache[T, R] = new NoOpCache[T, R]())(implicit conf: Configuration) {

  def pops = this.asInstanceOf[T]

  val tablePool = new HTablePool(conf, 50)
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

  def column[F, K, V](columnFamily: ColumnFamily[T, R, F, K, _], columnName: K, valueClass: Class[V])(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) = {
    val c = new Column[T, R, F, K, V](this, columnFamily.familyName, columnName)
    columns += c
    c
  }

  def family[F, K, V](familyName: F, compressed: Boolean = false, versions: Int = 1)(implicit c: ByteConverter[F]) = {
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
      table foreach (_.flushCommits())
      table foreach (tbl => tablePool.putTable(tbl))
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

  def put(key: R)(implicit c: ByteConverter[R]) = new PutOp(this, c.toBytes(key))

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

  def apply(item: String): CommaSet = CommaSet(Set(item))
}

class DataInputWrapper(input: DataInputStream) {
  def readObj[T](implicit c: ComplexByteConverter[T]) = {
    c.read(input)
  }
}

class DataOutputWrapper(output: DataOutputStream) {
  def writeObj[T](obj: T)(implicit c: ByteConverter[T]) {
    output.write(c.toBytes(obj))
  }
}