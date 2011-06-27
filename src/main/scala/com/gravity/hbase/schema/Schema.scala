package com.gravity.hbase.schema

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.TableNotFoundException
import org.apache.hadoop.conf.Configuration
import scala.collection._
import mutable.Buffer

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/**
* Class to be implemented by custom converters
*/
abstract class ByteConverter[T] {
  def toBytes(t: T): Array[Byte]

  def fromBytes(bytes: Array[Byte]): T
}


/**
* When a query comes back, there are a bucket of column families and columns to retrieve.  This class retrieves them.
*/
class QueryResult[T](val result: Result, table: HbaseTable[T]) {
  def column[F, K, V](column: (T) => Column[T, F, K, V])(implicit c: ByteConverter[V]): Option[V] = {
    val co = column(table.pops)
    val col = result.getColumnLatest(co.familyBytes, co.columnBytes)
    if(col != null) {
      Some(c.fromBytes(col.getValue))
    }else {
      None
    }
  }

  def family[F, K, V](family:(T)=>ColumnFamily[T, F, K, V])(implicit c:ByteConverter[F], d:ByteConverter[K], e:ByteConverter[V]) = {

    val familyMap = result.getFamilyMap(family(table.pops).familyBytes)
    if(familyMap != null) {
      familyMap.map{case (column: Array[Byte], value: Array[Byte]) =>
        d.fromBytes(column) -> e.fromBytes(value)
      }
    }else {
      Map[K,V]()
    }
  }

}

/**
* A query for setting up a scanner across the whole table or key subsets.
* There is a lot of room for expansion in this class -- caching parameters, scanner specs, key-only, etc.
*/
class ScanQuery[T](table: HbaseTable[T]) {
  val scan = new Scan()

  def execute(handler: (QueryResult[T]) => Unit) {
    table.withTable {
      htable =>
        val scanner = htable.getScanner(scan)
        for (result <- scanner) {
          handler(new QueryResult[T](result, table))
        }
    }
  }

  def withStartKey[K](key: K)(implicit c: ByteConverter[K]) = {scan.setStartRow(c.toBytes(key)); this}

  def withEndKey[K](key: K)(implicit c: ByteConverter[K]) = {scan.setStopRow(c.toBytes(key)); this}

}

/**
* An individual data modification operation (put, increment, or delete usually)
* These operations are chained together by the client, and then executed in bulk.
*/
class OpBase[T](table:HbaseTable[T], key:Array[Byte], previous: Buffer[OpBase[T]] = Buffer[OpBase[T]]()) {

  previous += this

  def put[K](key:K)(implicit c:ByteConverter[K]) = {
    val po = new PutOp(table,c.toBytes(key),previous)
    po
  }

  def increment[K](key:K)(implicit c:ByteConverter[K]) = {
    val inc = new IncrementOp(table,c.toBytes(key),previous)
    inc
  }

  def delete[K](key:K)(implicit c:ByteConverter[K]) = {
    val del = new DeleteOp(table,c.toBytes(key),previous)
    del
  }

  def execute() {
    val rows = Buffer[Row]()
    val increments = Buffer[Increment]()
    table.withTable{table=>
      previous.foreach{
        case put:PutOp[_] => {
          rows += put.put
        }
        case delete:DeleteOp[_] => {
          rows += delete.delete
        }
        case increment:IncrementOp[_] => {
          increments += increment.increment
        }
      }

      if(rows.size > 0) {
        table.batch(rows)
      }
      if(increments.size > 0) {
        increments.foreach(increment=>table.increment(increment))
      }
    }
  }
}

/**
* An increment operation -- can increment multiple columns in a single go.
*/
class IncrementOp[T](table:HbaseTable[T], key:Array[Byte], previous: Buffer[OpBase[T]] = Buffer[OpBase[T]]()) extends OpBase[T](table,key,previous) {
  val increment = new Increment(key)

  def value[F, K, Long](column:(T)=> Column[T, F, K, Long], value: java.lang.Long)(implicit c: ByteConverter[F], d: ByteConverter[K]) = {
    val col = column(table.pops)
    increment.addColumn(col.familyBytes, col.columnBytes, value)
    this
  }

  def valueMap[F, K, Long](family:(T)=> ColumnFamily[T, F, K, Long], values: Map[K,Long])(implicit c: ByteConverter[F], d:ByteConverter[K]) = {
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
class PutOp[T](table:HbaseTable[T], key:Array[Byte], previous: Buffer[OpBase[T]] = Buffer[OpBase[T]]()) extends OpBase[T](table,key,previous) {
  val put = new Put(key)

  def value[F, K, V](column:(T) => Column[T, F, K, V], value: V)(implicit c: ByteConverter[F], d: ByteConverter[K], e: ByteConverter[V]) = {
    val col = column(table.asInstanceOf[T])
    put.add(col.familyBytes, col.columnBytes, e.toBytes(value))
    this
  }

  def valueMap[F,K,V](family:(T)=> ColumnFamily[T, F,K,V], values: Map[K,V])(implicit c: ByteConverter[K], d: ByteConverter[V]) = {
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
class DeleteOp[T](table:HbaseTable[T], key:Array[Byte], previous: Buffer[OpBase[T]] = Buffer[OpBase[T]]()) extends OpBase[T](table,key,previous) {
  val delete = new Delete(key)

  def family[F, K, V](family :(T)=> ColumnFamily[T, F,K,V]) = {
    val fam = family(table.pops)
    delete.deleteFamily(fam.familyBytes)
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
class Query[T](table: HbaseTable[T]) {

  val keys = Buffer[Array[Byte]]()
  val families = Buffer[Array[Byte]]()
  val columns = Buffer[(Array[Byte], Array[Byte])]()


  def withKey[K](key: K)(implicit c: ByteConverter[K]) = {
    keys += c.toBytes(key)
    this
  }

  def withColumnFamily[F, K, V](family: (T)=>ColumnFamily[T, F, K, V])(implicit c: ByteConverter[F]): Query[T] = {
    val fam = family(table.pops)
    families += c.toBytes(fam.familyName)
    this
  }

  def withColumn[F, K, V](family: (T)=>ColumnFamily[T, F, K, V], columnName: K)(implicit c: ByteConverter[F], d: ByteConverter[K]): Query[T] = {
    val fam = family(table.pops)
    columns += (fam.familyBytes -> d.toBytes(columnName))
    this
  }

  def withColumn[F, K, V](column: (T)=>Column[T, F, K, V])(implicit c: ByteConverter[K]): Query[T] = {
    val col = column(table.pops)
    columns += (col.familyBytes -> col.columnBytes)
    this
  }

  def single() = {
    require(keys.size == 1, "Calling single() with more than one key")
    val get = new Get(keys(0))

    for (family <- families) {
      get.addFamily(family)
    }
    for ((columnFamily, column) <- columns) {
      get.addColumn(columnFamily, column)
    }

    table.withTable {htable => new QueryResult(htable.get(get), table)}
  }

  def execute() = {

    val gets = for (key <- keys) yield {
      new Get(key)
    }
    for (family <- families; get <- gets) {
      get.addFamily(family)
    }
    for ((columnFamily, column) <- columns; get <- gets) {
      get.addColumn(columnFamily, column)
    }

    table.withTable {
      htable =>
        val results = htable.get(gets)
        results.map(res => new QueryResult(res, table))
    }
  }

}

/**
* Represents the specification of a Column Family
*/
class ColumnFamily[T, F, K, V](val table: HbaseTable[T], val familyName: F, val compressed: Boolean = false, val versions: Int = 1)(implicit c: ByteConverter[F]) {
  val familyBytes = c.toBytes(familyName)
}

/**
* Represents the specification of a Column.
*/
class Column[T, F, K, V](table:HbaseTable[T], columnFamily: F, columnName: K)(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) {
  val columnBytes = kc.toBytes(columnName)
  val familyBytes = fc.toBytes(columnFamily)

  def getValue(res: QueryResult[T]) = {
    kv.fromBytes(res.result.getColumnLatest(familyBytes, columnBytes).getValue)
  }
}

trait Schema {

}

/**
* Represents a Table.  Expects an instance of HBaseConfiguration to be present.
*/
class HbaseTable[T](tableName: String)(implicit conf: Configuration) {

  def pops = this.asInstanceOf[T]

  /*
  WARNING - Currently assumes the family names are strings (which is probably a best practice, but we support byte families)
   */
  def createScript() = {
    val create = "create '" + tableName + "', "


    create + (for (family <- families) yield {
      "{NAME => '" + Bytes.toString(family.familyBytes) + "', VERSIONS => " + family.versions +
              (if (family.compressed) ", COMPRESSION=>'lzo'" else "") +
              "}"
    }).mkString(",")
  }

  private val columns = Buffer[Column[_, _, _, _]]()
  private val families = Buffer[ColumnFamily[_, _, _, _]]()

  def getTable(name: String) = new HTable(conf, name)

  def column[F, K, V](columnFamily: ColumnFamily[T, F, K, _], columnName: K, valueClass: Class[V])(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) = {
    val c = new Column[T, F, K, V](this, columnFamily.familyName, columnName)
    columns += c
    c
  }

  def family[F, K, V](familyName: F, compressed: Boolean = false, versions: Int = 1)(implicit c: ByteConverter[F]) = {
    val family = new ColumnFamily[T, F, K, V](this, familyName, compressed, versions)
    families += family
    family
  }

  def getTableOption(name: String) = {
    try {
      Some(getTable(name))
    } catch {
      case e: TableNotFoundException => None
    }
  }


  def withTableOption[R](name: String)(work: (Option[HTable]) => R) : R = {
    val table = getTableOption(name)
    try {
      work(table)
    } finally {
      table foreach (_.flushCommits())
    }
  }


  def withTable[R](funct: (HTable) => R) : R = {
    withTableOption(tableName) {
      case Some(table) => {
        funct(table)
      }
      case None => throw new RuntimeException("Table " + tableName + " does not exist")
    }
  }

  def scan = new ScanQuery(this)

  def query = new Query(this)

  def put[K](key:K)(implicit c:ByteConverter[K]) = new PutOp[T](this,c.toBytes(key))
  def delete[K](key:K)(implicit c:ByteConverter[K]) = new DeleteOp[T](this, c.toBytes(key))
  def increment[K](key:K)(implicit c:ByteConverter[K]) = new IncrementOp[T](this, c.toBytes(key))
}

case class YearDay(year: Int, day: Int)

case class CommaSet(items: Set[String])


