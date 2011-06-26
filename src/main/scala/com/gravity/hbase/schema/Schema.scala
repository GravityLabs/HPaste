package com.gravity.hbase.schema

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.{TableNotFoundException, HBaseConfiguration}
import org.joda.time.DateTime
import org.apache.hadoop.conf.Configuration

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


abstract class ByteConverter[T] {
  def toBytes(t: T): Array[Byte]

  def fromBytes(bytes: Array[Byte]): T
}



class ColumnFamily[F, K, V](val familyName: F)(implicit c: ByteConverter[F]) {
  val familyBytes = c.toBytes(familyName)
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

  val keys = ListBuffer[Array[Byte]]()
  val families = ListBuffer[Array[Byte]]()
  val columns = ListBuffer[(Array[Byte], Array[Byte])]()


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

class Column[F, K, V](columnFamily: F, columnName: K)(implicit fc: ByteConverter[F], kc: ByteConverter[K], kv: ByteConverter[V]) {
  val columnBytes = kc.toBytes(columnName)
  val familyBytes = fc.toBytes(columnFamily)

  def getValue(res: QueryResult) = {
    kv.fromBytes(res.result.getColumnLatest(familyBytes, columnBytes).getValue)
  }
}

class HbaseTable(tableName: String)(implicit conf:Configuration) {
  def getTable(name: String) = new HTable(conf, name)

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


package object schema {
  implicit object StringConverter extends ByteConverter[String] {
    override def toBytes(t: String) = Bytes.toBytes(t)

    override def fromBytes(bytes: Array[Byte]) = Bytes.toString(bytes)
  }

  implicit object LongConverter extends ByteConverter[Long] {
    override def toBytes(t: Long) = Bytes.toBytes(t)

    override def fromBytes(bytes: Array[Byte]) = Bytes.toLong(bytes)
  }

  implicit object DateTimeConverter extends ByteConverter[DateTime] {
    override def toBytes(t:DateTime) = Bytes.toBytes(t.getMillis)
    override def fromBytes(bytes:Array[Byte]) = new DateTime(Bytes.toLong(bytes))
  }

  implicit object CommaSetConverter extends ByteConverter[CommaSet] {
    override def toBytes(t:CommaSet) = Bytes.toBytes(t.items.mkString(","))
    override def fromBytes(bytes:Array[Byte]) = new CommaSet(Bytes.toString(bytes).split(",").toSet)
  }

  implicit object YearDayConverter extends ByteConverter[YearDay] {
    override def toBytes(t:YearDay) = Bytes.toBytes(t.year.toString + "_" + t.day.toString)
    override def fromBytes(bytes:Array[Byte]) = {
      val strRep = Bytes.toString(bytes)
      val strRepSpl = strRep.split("_")
      val year = strRepSpl(0).toInt
      val day = strRepSpl(1).toInt
      YearDay(year,day)
    }
  }
}