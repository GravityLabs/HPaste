package com.gravity.hbase.schema

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.{TableNotFoundException, HBaseConfiguration}
import org.joda.time.DateTime
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


class QueryResult(val result: Result, table: HbaseTable) {
  def column[F, K, V](column: Column[F, K, V])(implicit c: ByteConverter[V]): V = c.fromBytes(result.getColumnLatest(column.familyBytes, column.columnBytes).getValue)

  //  def familySet[F,K,V](family:ColumnFamily[F,K,V])
}

class ScanQuery(table: HbaseTable) {
  val scan = new Scan()

  def execute[T](handler:(QueryResult)=>T) {
    table.withTable{htable=>
      val scanner = htable.getScanner(scan)
      for(result <- scanner) {
        handler(new QueryResult(result,table))
      }
    }
  }

  def withStartKey[K](key:K)(implicit c:ByteConverter[K]) = {scan.setStartRow(c.toBytes(key));this}
  def withEndKey[K](key:K)(implicit c:ByteConverter[K]) = {scan.setStopRow(c.toBytes(key));this}

}

class Query(table: HbaseTable) {

  val keys = Buffer[Array[Byte]]()
  val families = Buffer[Array[Byte]]()
  val columns = Buffer[(Array[Byte], Array[Byte])]()


  def withKey[K](key: K)(implicit c: ByteConverter[K]) = {
    keys += c.toBytes(key)
    this
  }

  def withColumnFamily[T](name: T)(implicit c: ByteConverter[T]): Query = {
    families += c.toBytes(name)
    this
  }

  def withColumnFamily[F, K, V](family: ColumnFamily[F, K, V])(implicit c: ByteConverter[F]): Query = {
    families += c.toBytes(family.familyName)
    this
  }

  def withColumn[F, K, V](family: ColumnFamily[F, K, V], columnName: K)(implicit c: ByteConverter[F], d: ByteConverter[K]): Query = {
    columns += (family.familyBytes -> d.toBytes(columnName))
    this
  }

  def withColumn[F, K, V](column: Column[F, K, V])(implicit c: ByteConverter[K]): Query = {
    columns += (column.familyBytes -> column.columnBytes)
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

    table.withTable {htable =>
      val results = htable.get(gets)
      results.map(res=> new QueryResult(res,table))
    }
  }

}

class ColumnFamily[F, K, V](val familyName: F, val compressed:Boolean=false, val versions:Int=1)(implicit c: ByteConverter[F]) {
  val familyBytes = c.toBytes(familyName)
}

class Column[F, K, V](columnFamily: F, columnName: K)(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) {
  val columnBytes = kc.toBytes(columnName)
  val familyBytes = fc.toBytes(columnFamily)



  def getValue(res: QueryResult) = {
    kv.fromBytes(res.result.getColumnLatest(familyBytes, columnBytes).getValue)
  }
}


class HbaseTable(tableName: String)(implicit conf:Configuration) {
  /*
  WARNING - Currently assumes the family names are strings (which is probably a best practice, but we support byte families)
   */
  def createScript() = {
    /*
   * create 'articles', {NAME => 'meta', VERSIONS => 1}, {NAME=> 'counts', VERSIONS => 1}, {NAME => 'text', VERSIONS => 1, COMPRESSION=>'lzo'}, {NAME => 'attributes', VERSIONS => 1}
   * create 'daily_2011_180', {NAME => 'rollups', VERSIONS => 1}, {NAME=>'topicViews', VERSIONS=>1},{NAME=>'topicArticles',VERSIONS=>1},{NAME=>'topicSocialReferrerCounts',VERSIONS=>1},{NAME=>'topicSearchReferrerCounts',VERSIONS=>1}
   * alter 'articles', NAME => 'html', VERSIONS =>1, COMPRESSION=>'lzo'
   * alter 'articles', NAME => 'topics', VERSIONS =>1
     */
    val create = "create '"+tableName+"', "


    create + (for(family <- families) yield {
      "{NAME => '" + Bytes.toString(family.familyBytes) + "', VERSIONS => "+family.versions +
      (if(family.compressed) ", COMPRESSION=>'lzo'" else "") +
      "}"
    }).mkString(",")
  }

  private val columns = Buffer[Column[_,_,_]]()
  private val families = Buffer[ColumnFamily[_,_,_]]()

  def getTable(name: String) = new HTable(conf, name)

  def column[F,K,V](columnFamily: ColumnFamily[F,K,_], columnName : K, valueClass: Class[V])(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) = {
    val c = new Column[F,K,V](columnFamily.familyName,columnName)
    columns += c
    c
  }

  def family[F,K,V](familyName:F, compressed:Boolean=false, versions:Int=1)(implicit c:ByteConverter[F]) = {
    val family = new ColumnFamily[F,K,V](familyName, compressed,versions)
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


  def withTableOption[T](name: String)(work: (Option[HTable]) => T): T = {
    val table = getTableOption(name)
    try {
      work(table)
    } finally {
      table foreach (_.flushCommits())
    }
  }


  def withTable[T](funct: (HTable) => T): T = {
    withTableOption(tableName) {
      case Some(table) => {
        funct(table)
      }
      case None => throw new RuntimeException("Table " + tableName + " does not exist")
    }
  }

  def scan = new ScanQuery(this)

  def query = new Query(this)

}

case class YearDay(year:Int, day:Int)

case class CommaSet(items:Set[String])


