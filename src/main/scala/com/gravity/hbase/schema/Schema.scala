package com.gravity.hbase.schema

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.TableNotFoundException
import org.apache.hadoop.conf.Configuration
import scala.collection._
import mutable.Buffer
import java.io._
import org.apache.hadoop.io.{BytesWritable, Writable}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{Filter, FilterList, SingleColumnValueFilter}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

case class ScanCachePolicy(ttlMinutes:Int)

trait QueryResultCache[T <: HbaseTable[T,R],R] {
  def get(key:R)(implicit c:ByteConverter[R]) : Option[QueryResult[T,R]]
  def put(key:R, value:QueryResult[T,R])(implicit c:ByteConverter[R])

  def getScanResult(key:Scan) : Option[Seq[QueryResult[T,R]]]
  def putScanResult(key:Scan, value:Seq[QueryResult[T,R]])
}

class NoOpCache[T <: HbaseTable[T,R],R] extends QueryResultCache[T,R] {
  override def get(key:R)(implicit c:ByteConverter[R]) : Option[QueryResult[T,R]] = None
  override def put(key:R, value:QueryResult[T,R])(implicit c:ByteConverter[R]) {}

  override def getScanResult(key:Scan) : Option[Seq[QueryResult[T,R]]] = None
  override def putScanResult(key:Scan, value:Seq[QueryResult[T,R]]) {}
}

/**
* Class to be implemented by custom converters
*/
abstract class ByteConverter[T] {
  def toBytes(t: T): Array[Byte]

  def fromBytes(bytes: Array[Byte]): T

  def toBytesWritable(t:T) : BytesWritable = {
    new BytesWritable(toBytes(t))
  }

  def fromBytesWritable(bytes: BytesWritable) : T = {
    fromBytes(bytes.getBytes)
  }
}

/**
* Simple high performance conversions from complex types to bytes
*/
abstract class ComplexByteConverter[T] extends ByteConverter[T] {
  override def toBytes(t: T) : Array[Byte] = {
    val bos = new ByteArrayOutputStream()

    val dout = new DataOutputStream(bos)
    write(t, dout)

    bos.toByteArray
  }

  def write(data: T, output:DataOutputStream)

  override def fromBytes(bytes:Array[Byte]):T = {
    val din = new DataInputStream(new ByteArrayInputStream(bytes))
    read(din)
  }

  def read(input:DataInputStream) : T
}

class MapConverter[K, V, MP <: mutable.Map[K,V]](implicit c:ByteConverter[K], d:ByteConverter[V]) extends ComplexByteConverter[MP] {
  override def write(map:MP, output:DataOutputStream) {
    val length = map.size
    output.writeInt(length)

    for((k,v) <- map) {
      val keyBytes = c.toBytes(k)
      val valBytes = d.toBytes(v)
      output.writeInt(keyBytes.size)
      output.write(keyBytes)
      output.writeInt(valBytes.size)
      output.write(valBytes)
    }
  }
  
  override def read(input:DataInputStream) = {
    val length = input.readInt()
    val map = mutable.Map[K,V]()
    for(i <- 0 until length) {
      val keyLength = input.readInt
      val keyArr = new Array[Byte](keyLength)
      input.read(keyArr)
      val key = c.fromBytes(keyArr)

      val valLength = input.readInt
      val valArr = new Array[Byte](valLength)
      input.read(valArr)
      val value = d.fromBytes(valArr)

      map.put(key,value)
    }
    map.asInstanceOf[MP]
  }
}

class SetConverter[T, ST <: Set[T]](implicit c:ByteConverter[T]) extends ComplexByteConverter[ST] {
  override def write(seq:ST, output:DataOutputStream) {
    writeSeq(seq,output)
  }

  def writeSeq(seq:ST, output:DataOutputStream)  {
      val length = seq.size
    output.writeInt(length)

    for(t <- seq){
      val bytes = c.toBytes(t)
      output.writeInt(bytes.size)
      output.write(bytes)
    }
  }

  override def read(input:DataInputStream) = readSeq(input)

  def readSeq(input:DataInputStream)= {
    val length = input.readInt()
    val buffer = Buffer[T]()
    for(i <- 0 until length) {
      val arrLength = input.readInt()
      val arr = new Array[Byte](arrLength)
      input.read(arr)
      buffer += c.fromBytes(arr)
    }
    buffer.toSet.asInstanceOf[ST]
  }

}

class SeqConverter[T, ST <: Seq[T]](implicit c:ByteConverter[T]) extends ComplexByteConverter[ST] {
  override def write(seq:ST, output:DataOutputStream) {
    writeSeq(seq,output)
  }

  def writeSeq(seq:ST, output:DataOutputStream)  {
      val length = seq.size
    output.writeInt(length)

    for(t <- seq){
      val bytes = c.toBytes(t)
      output.writeInt(bytes.size)
      output.write(bytes)
    }
  }

  override def read(input:DataInputStream) = readSeq(input)

  def readSeq(input:DataInputStream)= {
    val length = input.readInt()
    val buffer = Buffer[T]()
    for(i <- 0 until length) {
      val arrLength = input.readInt()
      val arr = new Array[Byte](arrLength)
      input.read(arr)
      buffer += c.fromBytes(arr)
    }
    buffer.toIndexedSeq.asInstanceOf[ST]
  }

}




/**
* When a query comes back, there are a bucket of column families and columns to retrieve.  This class retrieves them.
*/
class QueryResult[T <: HbaseTable[T,R],R](val result: Result, table: HbaseTable[T,R], val tableName: String) extends Serializable{
  def column[F, K, V](column: (T) => Column[T, R, F, K, V])(implicit c: ByteConverter[V]): Option[V] = {
    val co = column(table.pops)
    val col = result.getColumnLatest(co.familyBytes, co.columnBytes)
    if(col != null) {
      Some(c.fromBytes(col.getValue))
    }else {
      None
    }
  }

  def family[F, K, V](family:(T)=>ColumnFamily[T, R, F, K, V])(implicit c:ByteConverter[F], d:ByteConverter[K], e:ByteConverter[V]): Map[K, V] = {

    val familyMap = result.getFamilyMap(family(table.pops).familyBytes)
    if(familyMap != null) {
      familyMap.map{case (column: Array[Byte], value: Array[Byte]) =>
        d.fromBytes(column) -> e.fromBytes(value)
      }
    }else {
      val mt = Map.empty[K,V]
      mt
    }
  }

  def rowid(implicit c:ByteConverter[R]) = c.fromBytes(result.getRow)

  def getTableName = tableName
}

/**
* A query for setting up a scanner across the whole table or key subsets.
* There is a lot of room for expansion in this class -- caching parameters, scanner specs, key-only, etc.
*/
class ScanQuery[T <: HbaseTable[T,R],R](table: HbaseTable[T,R]) {
  val scan = new Scan()
  scan.setCaching(100)

  val filterBuffer = Buffer[Filter]()

  def executeWithCaching(cachePolicy:ScanCachePolicy,operator:FilterList.Operator=FilterList.Operator.MUST_PASS_ALL) : Seq[QueryResult[T,R]] = {
      completeScanner(operator)
      val results = table.cache.getScanResult(scan) match {
        case Some(result) => result
        case None => {
          val results = Buffer[QueryResult[T,R]]()
          table.withTable() { htable=>
            val scanner = htable.getScanner(scan)
            try {
              for(result <- scanner) {
                results += new QueryResult[T,R](result,table,table.tableName)
              }
              table.cache.putScanResult(scan,results.toSeq)
              results
            }finally {
              scanner.close()
            }
          }
        }
      }

    results
  }

  def execute(handler: (QueryResult[T,R]) => Unit,operator:FilterList.Operator = FilterList.Operator.MUST_PASS_ALL) {
    table.withTable() {
      htable =>
             completeScanner(operator)
          val scanner = htable.getScanner(scan)

          try {
          for (result <- scanner) {
            handler(new QueryResult[T,R](result, table, table.tableName))
          }
        }finally {
            scanner.close()
          }
    }
  }

  /*
  Prepares the scanner for use by chaining the filters together.  Should be called immediately before passing the scanner to the table.
   */
  def completeScanner(operator:FilterList.Operator = FilterList.Operator.MUST_PASS_ALL) {
    if(filterBuffer.size > 0) {
      val filterList = new FilterList(operator)
      filterBuffer.foreach{filter=>filterList.addFilter(filter)}
      scan.setFilter(filterList)
    }
  }

  def executeToSeq[I](handler: (QueryResult[T,R]) => I, operator:FilterList.Operator = FilterList.Operator.MUST_PASS_ALL): Seq[I] = {
    val results = Buffer[I]()

    table.withTable() {
      htable =>
          completeScanner(operator)
          val scanner = htable.getScanner(scan)

          try {
          for (result <- scanner; if (result != null)) {
            results += handler(new QueryResult[T,R](result, table, table.tableName))
          }
        }finally {
            scanner.close()
          }
    }

    results.toSeq
  }

  def withFamily[F,K,V](family:FamilyExtractor[T,R,F,K,V]) = {
    val fam = family(table.pops)
    scan.addFamily(fam.familyBytes)
    this
  }

  def withColumnOp[F,K,V](column:ColumnExtractor[T,R,F,K,V], compareOp:CompareOp, value:Option[V], excludeIfNull:Boolean)(implicit c:ByteConverter[V]) = {
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

  def withCaching(rowsToCache:Int) = {scan.setCaching(rowsToCache);this;}
}

/**
* An individual data modification operation (put, increment, or delete usually)
* These operations are chained together by the client, and then executed in bulk.
*/
class OpBase[T <: HbaseTable[T,R],R](table:HbaseTable[T,R], key:Array[Byte], previous: Buffer[OpBase[T,R]] = Buffer[OpBase[T,R]]()) {

  previous += this

  def put(key:R)(implicit c:ByteConverter[R]) = {
    val po = new PutOp(table,c.toBytes(key),previous)
    po
  }

  def increment(key:R)(implicit c:ByteConverter[R]) = {
    val inc = new IncrementOp(table,c.toBytes(key),previous)
    inc
  }

  def delete(key:R)(implicit c:ByteConverter[R]) = {
    val del = new DeleteOp(table,c.toBytes(key),previous)
    del
  }

  def size = previous.size

  def getOperations : Iterable[Writable] = {
    val puts = Buffer[Put]()
    val deletes = Buffer[Delete]()
    val increments = Buffer[Increment]()
    previous.foreach{
      case put:PutOp[T,R] => {
        puts += put.put
      }
      case delete:DeleteOp[T,R] => {
        deletes += delete.delete
      }
      case increment:IncrementOp[T,R] => {
        increments += increment.increment
      }
    }

    deletes ++ puts ++ increments

  }

  def execute(tableName:String = table.tableName) = {
    val puts = Buffer[Put]()
    val deletes = Buffer[Delete]()
    val increments = Buffer[Increment]()
    table.withTable(tableName){table=>

      previous.foreach{
        case put:PutOp[T,R] => {
          puts += put.put
        }
        case delete:DeleteOp[T,R] => {
          deletes += delete.delete
        }
        case increment:IncrementOp[T,R] => {
          increments += increment.increment
        }
      }

      if(deletes.size > 0 || puts.size > 0) {
        //IN THEORY, the operations will happen in order.  If not, break this into two different batched calls for deletes and puts
        table.batch(deletes ++ puts)
      }
      if(increments.size > 0) {
        increments.foreach(increment=>table.increment(increment))
      }
    }

    OpsResult(deletes.size, puts.size, increments.size)
  }
}

case class OpsResult(numDeletes: Int, numPuts: Int, numIncrements: Int)

/**
* An increment operation -- can increment multiple columns in a single go.
*/
class IncrementOp[T <: HbaseTable[T,R],R](table:HbaseTable[T,R], key:Array[Byte], previous: Buffer[OpBase[T,R]] = Buffer[OpBase[T,R]]()) extends OpBase[T,R](table,key,previous) {
  val increment = new Increment(key)

  def value[F, K, Long](column:(T)=> Column[T, R, F, K, Long], value: java.lang.Long)(implicit c: ByteConverter[F], d: ByteConverter[K]) = {
    val col = column(table.pops)
    increment.addColumn(col.familyBytes, col.columnBytes, value)
    this
  }

  def valueMap[F, K, Long](family:(T)=> ColumnFamily[T, R, F, K, Long], values: Map[K,Long])(implicit c: ByteConverter[F], d:ByteConverter[K]) = {
    val fam = family(table.pops)
    for((key,value) <- values) {
      increment.addColumn(fam.familyBytes, d.toBytes(key),value.asInstanceOf[java.lang.Long])
    }
    this
  }
}

/**
* A Put operation.  Can work across multiple columns or entire column families treated as Maps.
*/
class PutOp[T <: HbaseTable[T,R],R](table:HbaseTable[T,R], key:Array[Byte], previous: Buffer[OpBase[T,R]] = Buffer[OpBase[T,R]]()) extends OpBase[T,R](table,key,previous) {
  val put = new Put(key)

  def value[F, K, V](column:(T) => Column[T,R, F, K, V], value: V)(implicit c: ByteConverter[F], d: ByteConverter[K], e: ByteConverter[V]) = {
    val col = column(table.asInstanceOf[T])
    put.add(col.familyBytes, col.columnBytes, e.toBytes(value))
    this
  }

  def valueMap[F,K,V](family:(T)=> ColumnFamily[T,R, F,K,V], values: Map[K,V])(implicit c: ByteConverter[K], d: ByteConverter[V]) = {
    val fam = family(table.pops)
    for((key,value) <- values) {
      put.add(fam.familyBytes, c.toBytes(key), d.toBytes(value))
    }
    this
  }
}

/**
* A deletion operation.  If nothing is specified but a key, will delete the whole row.  If a family is specified, will just delete the values in
* that family.
*/
class DeleteOp[T <: HbaseTable[T,R],R](table:HbaseTable[T,R], key:Array[Byte], previous: Buffer[OpBase[T,R]] = Buffer[OpBase[T,R]]()) extends OpBase[T,R](table,key,previous) {
  val delete = new Delete(key)

  def family[F, K, V](family :(T)=> ColumnFamily[T,R, F,K,V]) = {
    val fam = family(table.pops)
    delete.deleteFamily(fam.familyBytes)
    this
  }

  def values[F, K, V](family : (T) => ColumnFamily[T,R,F,K,V], values:Set[K])(implicit vc:ByteConverter[K]) = {
    for(value <- values) {
      val fam = family(table.pops)
      delete.deleteColumns(fam.familyBytes,vc.toBytes(value))
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
class Query[T <: HbaseTable[T,R],R](table: HbaseTable[T,R]) {

  val keys = Buffer[Array[Byte]]()
  val families = Buffer[Array[Byte]]()
  val columns = Buffer[(Array[Byte], Array[Byte])]()


  def withKey(key: R)(implicit c: ByteConverter[R]) = {
    keys += c.toBytes(key)
    this
  }

  def withKeys(keys: Set[R])(implicit c: ByteConverter[R]) = {
    for (key <- keys) withKey(key)(c)
    this
  }

  def withColumnFamily[F, K, V](family: (T)=>ColumnFamily[T, R, F, K, V])(implicit c: ByteConverter[F]): Query[T,R] = {
    val fam = family(table.pops)
    families += c.toBytes(fam.familyName)
    this
  }

  def withColumn[F, K, V](family: (T)=>ColumnFamily[T,R, F, K, V], columnName: K)(implicit c: ByteConverter[F], d: ByteConverter[K]): Query[T,R] = {
    val fam = family(table.pops)
    columns += (fam.familyBytes -> d.toBytes(columnName))
    this
  }

  def withColumn[F, K, V](column: (T)=>Column[T,R, F, K, V])(implicit c: ByteConverter[K]): Query[T,R] = {
    val col = column(table.pops)
    columns += (col.familyBytes -> col.columnBytes)
    this
  }

  def single(tableName:String = table.tableName, cacheResults:Boolean=false) = {
    require(keys.size == 1, "Calling single() with more than one key")
    

    val get = new Get(keys(0))

    for (family <- families) {
      get.addFamily(family)
    }
    for ((columnFamily, column) <- columns) {
      get.addColumn(columnFamily, column)
    }

    table.withTable(tableName) {htable => new QueryResult(htable.get(get), table, tableName)}
  }

  def singleOption(tableName: String = table.tableName): Option[QueryResult[T,R]] = {
    require(keys.size == 1, "Calling single() with more than one key")

    table.withTableOption(tableName) {
      case Some(htable) => {
        val get = new Get(keys(0))
        for (family <- families) {
          get.addFamily(family)
        }
        for ((columnFamily, column) <- columns) {
          get.addColumn(columnFamily, column)
        }
        return Some(new QueryResult(htable.get(get), table, tableName))
      }
      case None => return None
    }
  }

  def execute(tableName:String = table.tableName) = {

    val gets = for (key <- keys) yield {
      new Get(key)
    }
    for (family <- families; get <- gets) {
      get.addFamily(family)
    }
    for ((columnFamily, column) <- columns; get <- gets) {
      get.addColumn(columnFamily, column)
    }

    table.withTable(tableName) {
      htable =>
        val results = htable.get(gets)
        results.map(res => new QueryResult(res, table, tableName))
    }
  }

  def executeMap(tableName:String = table.tableName)(implicit c:ByteConverter[R]) = {
    val gets = for (key <- keys) yield {
      new Get(key)
    }
    for (family <- families; get <- gets) {
      get.addFamily(family)
    }
    for ((columnFamily, column) <- columns; get <- gets) {
      get.addColumn(columnFamily, column)
    }

    table.withTable(tableName) {
      htable =>
        val results = htable.get(gets)
        results.flatMap(res => {
          if(res != null && !res.isEmpty) {
            val qr = new QueryResult[T,R](res, table, tableName)
            Some(qr.rowid -> qr)
          }else {
            None
          }
        })
    }.toMap
  }

}

/**
* Represents the specification of a Column Family
*/
class ColumnFamily[T <: HbaseTable[T,R],R, F, K, V](val table: HbaseTable[T,R], val familyName: F, val compressed: Boolean = false, val versions: Int = 1)(implicit c: ByteConverter[F]) {
  val familyBytes = c.toBytes(familyName)
}

/**
* Represents the specification of a Column.
*/
class Column[T <: HbaseTable[T,R], R, F, K, V](table:HbaseTable[T,R], columnFamily: F, columnName: K)(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) {
  val columnBytes = kc.toBytes(columnName)
  val familyBytes = fc.toBytes(columnFamily)

  def getValue(res: QueryResult[T,R]) = {
    kv.fromBytes(res.result.getColumnLatest(familyBytes, columnBytes).getValue)
  }

  def apply(res: QueryResult[T,R]): Option[V] = {
    val rawValue = res.result.getColumnLatest(familyBytes, columnBytes)
    if (rawValue == null) None else {
      Some(kv.fromBytes(rawValue.getValue))
    }
  }

  def getOrDefault(res: QueryResult[T,R], default: V): V = apply(res) match {
    case Some(value) => value
    case None => default
  }
}

trait Schema {
  val tables = mutable.Set[HbaseTable[_,_]]()

  def table[T <: HbaseTable[T,_],_](table: T)= {
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
class HbaseTable[T <: HbaseTable[T,R],R](val tableName: String, var cache : QueryResultCache[T,R] = new NoOpCache[T,R]())(implicit conf: Configuration) {

  def pops = this.asInstanceOf[T]

  val tablePool = new HTablePool(conf,50)
  private val columns = Buffer[Column[T, R, _, _,_]]()
  val families = Buffer[ColumnFamily[T, R,_, _, _]]()

  def familyBytes = families.map(family => family.familyBytes)

  val meta = family[String,String,Any]("meta")

  //alter 'articles', NAME => 'html', VERSIONS =>1, COMPRESSION=>'lzo'

  /*
  WARNING - Currently assumes the family names are strings (which is probably a best practice, but we support byte families)
   */
  def createScript(tableNameOverride : String = tableName) = {
    val create = "create '" + tableNameOverride + "', "
    create + (for (family <- families) yield {
      familyDef(family)
    }).mkString(",")
  }

  def deleteScript(tableNameOverride : String = tableName) = {
    val delete = "disable '" + tableNameOverride + "'\n"

    delete + "delete '" + tableNameOverride + "'"
  }

  def alterScript(tableNameOverride : String = tableName, families: Seq[ColumnFamily[T,_,_,_,_]] = families) = {
    var alter = "disable '" + tableNameOverride + "'\n"
    alter += "alter '" + tableNameOverride + "', "
    alter += (for(family <- families) yield {
      familyDef(family)
    }).mkString(",")
    alter += "\nenable '" + tableNameOverride + "'"
    alter
  }

  def familyDef(family:ColumnFamily[T,_,_,_,_]) = {
    val compression = if (family.compressed) ", COMPRESSION=>'lzo'" else ""
    "{NAME => '%s', VERSIONS => %d%s}".format(Bytes.toString(family.familyBytes), family.versions, compression)
  }


  def getTable(name: String) = tablePool.getTable(name)

  def column[F, K, V](columnFamily: ColumnFamily[T, R, F, K, _], columnName: K, valueClass: Class[V])(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) = {
    val c = new Column[T, R,F, K, V](this, columnFamily.familyName, columnName)
    columns += c
    c
  }

  def family[F, K, V](familyName: F, compressed: Boolean = false, versions: Int = 1)(implicit c: ByteConverter[F]) = {
    val family = new ColumnFamily[T,R,F,K,V](this, familyName, compressed, versions)
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


  def withTableOption[Q](name: String)(work: (Option[HTableInterface]) => Q) : Q = {
    val table = getTableOption(name)
    try {
      work(table)
    } finally {
      table foreach (_.flushCommits())
      table foreach (tbl=>tablePool.putTable(tbl))
    }
  }


  def withTable[Q](mytableName:String = tableName)(funct: (HTableInterface) => Q) : Q = {
    withTableOption(mytableName) {
      case Some(table) => {
        funct(table)
      }
      case None => throw new RuntimeException("Table " + tableName + " does not exist")
    }
  }

  def scan = new ScanQuery(this)

  def query = new Query(this)

  def put(key:R)(implicit c:ByteConverter[R]) = new PutOp(this,c.toBytes(key))
  def delete(key:R)(implicit c:ByteConverter[R]) = new DeleteOp(this, c.toBytes(key))
  def increment(key:R)(implicit c:ByteConverter[R]) = new IncrementOp(this, c.toBytes(key))
}

case class YearDay(year: Int, day: Int)

case class CommaSet(items: Set[String])
object CommaSet {
  val empty = CommaSet(Set.empty[String])

  def apply(item: String): CommaSet = CommaSet(Set(item))
}


