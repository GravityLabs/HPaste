package com.gravity.hbase

import org.apache.hadoop.hbase.util.Bytes
import org.joda.time.DateTime

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


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