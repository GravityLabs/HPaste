package com.gravity.hbase

import org.apache.hadoop.hbase.util.Bytes
import scala.collection._
import org.joda.time.{DateMidnight, DateTime}
import org.apache.hadoop.io.BytesWritable
import java.io._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/**
* This is the standard set of types that can be auto converted into hbase values (they work as families, columns, and values)
*/
package object schema {

  implicit def dow(output:DataOutputStream) = new DataOutputWrapper(output)
  implicit def diw(input:DataInputStream) = new DataInputWrapper(input)

  type FamilyExtractor[T <: HbaseTable[T,R],R,F,K,V] = (T) => ColumnFamily[T,R,F,K,V]
  type ColumnExtractor[T <: HbaseTable[T,R],R,F,K,V] = (T) => Column[T, R, F, K, V]


  implicit object StringConverter extends ByteConverter[String] {
    override def toBytes(t: String) = Bytes.toBytes(t)
    override def fromBytes(bytes: Array[Byte]) = Bytes.toString(bytes)
  }

  implicit object StringSeqConverter extends SeqConverter[String,Seq[String]]
  implicit object StringSetConverter extends SetConverter[String,Set[String]]


  implicit object IntConverter extends ByteConverter[Int] {
    override def toBytes(t:Int) = Bytes.toBytes(t)
    override def fromBytes(bytes: Array[Byte]) = Bytes.toInt(bytes)
  }
  implicit object IntSeqConverter extends SeqConverter[Int,Seq[Int]]
  implicit object IntSetConverter extends SetConverter[Int,Set[Int]]

  implicit object ShortConverter extends ByteConverter[Short] {
    override def toBytes(t:Short) = Bytes.toBytes(t)
    override def fromBytes(bytes: Array[Byte]) = Bytes.toShort(bytes)
  }
  implicit object ShortSeqConverter extends SeqConverter[Short,Seq[Short]]
  implicit object ShortSetConverter extends SetConverter[Short,Set[Short]]

  implicit object BooleanConverter extends ByteConverter[Boolean] {
    override def toBytes(t:Boolean) = Bytes.toBytes(t)
    override def fromBytes(bytes: Array[Byte]) = Bytes.toBoolean(bytes)
  }
  implicit object BooleanSeqConverter extends SeqConverter[Boolean,Seq[Boolean]]
  implicit object BooleanSetConverter extends SetConverter[Boolean,Set[Boolean]]

  implicit object LongConverter extends ByteConverter[Long] {
    override def toBytes(t: Long) = Bytes.toBytes(t)
    override def fromBytes(bytes: Array[Byte]) = Bytes.toLong(bytes)
  }
  implicit object LongSeqConverter extends SeqConverter[Long,Seq[Long]]
  implicit object LongSetConverter extends SetConverter[Long,Set[Long]]

  implicit object DoubleConverter extends ByteConverter[Double] {
    override def toBytes(t: Double) = Bytes.toBytes(t)
    override def fromBytes(bytes: Array[Byte]) = Bytes.toDouble(bytes)
  }
  implicit object DoubleSeqConverter extends SeqConverter[Double,Seq[Double]]
  implicit object DoubleSetConverter extends SetConverter[Double,Set[Double]]


  implicit object FloatConverter extends ByteConverter[Float] {
    override def toBytes(t: Float) = Bytes.toBytes(t)
    override def fromBytes(bytes: Array[Byte]) = Bytes.toFloat(bytes)
  }
  implicit object FloatSeqConverter extends SeqConverter[Float,Seq[Float]]
  implicit object FloatSetConverter extends SetConverter[Float,Set[Float]]

  implicit object DateTimeConverter extends ByteConverter[DateTime] {
    override def toBytes(t:DateTime) = Bytes.toBytes(t.getMillis)
    override def fromBytes(bytes:Array[Byte]) = new DateTime(Bytes.toLong(bytes))
  }
  implicit object DateTimeSeqConverter extends SeqConverter[DateTime,Seq[DateTime]]
  implicit object DateTimeSetConverter extends SetConverter[DateTime,Set[DateTime]]

  implicit object CommaSetConverter extends ByteConverter[CommaSet] {
    val SPLITTER = ",".r
    override def toBytes(t:CommaSet) = Bytes.toBytes(t.items.mkString(","))
    override def fromBytes(bytes:Array[Byte]) = new CommaSet(SPLITTER.split(Bytes.toString(bytes)).toSet)
  }
  implicit object CommaSetSeqConverter extends SeqConverter[CommaSet,Seq[CommaSet]]
  implicit object CommaSetSetConverter extends SetConverter[CommaSet,Set[CommaSet]]


  implicit object YearDayConverter extends ByteConverter[YearDay] {
    val SPLITTER = "_".r
    override def toBytes(t:YearDay) = Bytes.toBytes(t.year.toString + "_" + t.day.toString)
    override def fromBytes(bytes:Array[Byte]) = {
      val strRep = Bytes.toString(bytes)
      val strRepSpl = SPLITTER.split(strRep)
      val year = strRepSpl(0).toInt
      val day = strRepSpl(1).toInt
      YearDay(year,day)
    }
  }
  implicit object YearDaySeqConverter extends SeqConverter[YearDay,Seq[YearDay]]
  implicit object YearDaySetConverter extends SetConverter[YearDay,Set[YearDay]]

  implicit object DateMidnightConverter extends ComplexByteConverter[DateMidnight] {
    override def write(dm:DateMidnight,output:DataOutputStream) {
      output.writeLong(dm.getMillis)
    }
    override def read(input:DataInputStream) = new DateMidnight(input.readLong())


    def apply(year:Int, day:Int) = new DateMidnight().withYear(year).withDayOfYear(day)
  }

  implicit object DateMidnightSeqConverter extends SeqConverter[DateMidnight,Seq[DateMidnight]]
  implicit object DateMidnightSetConverter extends SetConverter[DateMidnight,Set[DateMidnight]]

  implicit object StringLongMap extends MapConverter[String,Long,mutable.Map[String,Long]]

  /*
  Helper function to make byte arrays out of arbitrary values.
   */
  def makeBytes(writer:(DataOutputStream)=>Unit) : Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val dataOutput = new DataOutputStream(bos)
    writer(dataOutput)
    bos.toByteArray  
  }

  def makeWritable(writer:(DataOutputStream)=>Unit) : BytesWritable = new BytesWritable(makeBytes(writer))

  def readBytes[T](bytes:Array[Byte])(reader:(DataInputStream)=>T) : T = {
    val bis = new ByteArrayInputStream(bytes)
    val dis = new DataInputStream(bis)
    val results = reader(dis)
    dis.close()
    results
  }

  def readWritable[T](bytesWritable:BytesWritable)(reader:(DataInputStream)=>T) : T = readBytes(bytesWritable.getBytes)(reader)

}