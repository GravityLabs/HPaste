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
import org.apache.hadoop.io.{BytesWritable, Writable}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{Filter, FilterList, SingleColumnValueFilter}
import scala.collection._
import org.joda.time.DateTime
import scala.collection.mutable.{ListBuffer, Buffer}
import java.util.{HashMap, NavigableSet}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/** Expresses an input stream that can read ordered primitives from a binary input, and can also use the ByteConverter[T] interface to read serializable objects.
  *
  */
class PrimitiveInputStream(input: InputStream) extends DataInputStream(input) {
  /**
    * Read an object, assuming the existence of a ComplexByteConverter[T] implementation
    * The byte converter is stateless and should be therefore defined somewhere as an implicit object
    */
  def readObj[T](implicit c: ComplexByteConverter[T]) = {
    c.read(this)
  }

  def skipLong() {this.skipBytes(8)}

  //WORK IN PROGRESS
  def readRow[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR]) = {
    val rowBytesLength = readInt()
    val rowBytes = new Array[Byte](rowBytesLength)
    read(rowBytes)
    val rowId = table.rowKeyConverter.fromBytes(rowBytes)
    val ds = DeserializedResult(rowId.asInstanceOf[AnyRef], table.families.length)

    val famCount = readInt()

    for (i <- 0 until famCount) {
      val fam = table.familyByIndex(i)
      val kvLength = readInt()

      for (ii <- 0 until kvLength) {
        val isTypedColumn = readBoolean
        val converter = if (isTypedColumn) {
          val colIdx = readInt
          val col = table.columnByIndex(colIdx)
          col
        } else {
          fam
        }

        val keyLength = readInt
        val keyBytes = new Array[Byte](keyLength)
        read(keyBytes)
        val valueLength = readInt
        val valueBytes = new Array[Byte](valueLength)
        read(valueBytes)
        val key = converter.keyFromBytesUnsafe(keyBytes)
        val value = converter.valueFromBytesUnsafe(valueBytes)
        ds.add(fam,key,value,0l)
      }
    }
    table.rowBuilder(ds)

  }
}

/** Expresses an output stream that can write ordered primitives into a binary output, and can also use the ByteConverter[T] interface to write serializable objects.
  */
class PrimitiveOutputStream(output: OutputStream) extends DataOutputStream(output) {

  //WORK IN PROGRESS
  def writeRow[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T,R,RR],row: RR) {

    //Serialize row id
    val rowIdBytes = row.table.rowKeyConverter.toBytes(row.rowid)
    writeInt(rowIdBytes.length)
    write(rowIdBytes)

    //Write number of families
    writeInt(row.result.values.length)

    var idx = 0
    while (idx < row.result.values.length) {
      val family = row.result.values(idx)
      val colFam = row.table.familyByIndex(idx)
      if(family == null) {
        writeInt(0)
      }else {
        writeInt(family.size())
        family.foreach {
          case (colKey: AnyRef, colVal: AnyRef) =>
            //See if it's a strongly typed column
            val converters: KeyValueConvertible[_, _, _] = row.table.columnsByName.get(colKey) match {
              case Some(col) if col.family.index == colFam.index =>
                writeBoolean(true)
                writeInt(col.columnIndex)
                col
              case _ =>
                writeBoolean(false)
                colFam
            }

            val keyBytes = converters.keyToBytesUnsafe(colKey)
            writeInt(keyBytes.length)
            write(keyBytes)
            val valBytes = converters.valueToBytesUnsafe(colVal)
            writeInt(valBytes.length)
            write(valBytes)
        }

      }
      idx += 1
    }
  }


  /**
    * Write an object, assuming the existence of a ComplexByteConverter[T] implementation.
    * The byte converter is stateless and should be therefore defined somewhere as an implicit object
    */
  def writeObj[T](obj: T)(implicit c: ComplexByteConverter[T]) {
    c.write(obj, this)
  }
}

/**
  * Class to be implemented by custom converters
  */
abstract class ByteConverter[T] {
  def toBytes(t: T): Array[Byte]


  def fromBytes(bytes: Array[Byte]): T = fromBytes(bytes, 0, bytes.length)

  def fromBytes(bytes: Array[Byte], offset: Int, length: Int): T

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

    val dout = new PrimitiveOutputStream(bos)
    write(t, dout)

    bos.toByteArray
  }

  def write(data: T, output: PrimitiveOutputStream)

  override def fromBytes(bytes: Array[Byte], offset: Int, length: Int): T = {
    val din = new PrimitiveInputStream(new ByteArrayInputStream(bytes, offset, length))
    read(din)
  }

  override def fromBytes(bytes: Array[Byte]): T = {
    val din = new PrimitiveInputStream(new ByteArrayInputStream(bytes))
    read(din)
  }

  def read(input: PrimitiveInputStream): T

  def safeReadField[A](input: PrimitiveInputStream)(readField: (PrimitiveInputStream)=>A, valueOnFail: A): A = {
    if (input.available() < 1) return valueOnFail

    try {
      readField(input)
    }
    catch {
      case _: IOException => valueOnFail
    }
  }
}

trait MapStream[K,V] {
  val c : ByteConverter[K]
  val d : ByteConverter[V]

  def writeMap(map:Map[K,V], output: PrimitiveOutputStream) {
    val length = map.size
    output.writeInt(length)

    for ((k, v) <- map) {
      val keyBytes = c.toBytes(k)
      val valBytes = d.toBytes(v)
      output.writeInt(keyBytes.length)
      output.write(keyBytes)
      output.writeInt(valBytes.length)
      output.write(valBytes)
    }

  }

  def readMap(input:PrimitiveInputStream) : Array[(K,V)] = {
    val length = input.readInt()
    val kvarr = Array.ofDim[(K, V)](length)

    var i = 0
    while (i < length) {
      val keyLength = input.readInt
      val keyArr = new Array[Byte](keyLength)
      input.read(keyArr)
      val key = c.fromBytes(keyArr)

      val valLength = input.readInt
      val valArr = new Array[Byte](valLength)
      input.read(valArr)
      val value = d.fromBytes(valArr)

      kvarr(i) = (key -> value)
      i = i + 1
    }
    kvarr
  }
}

class ImmutableMapConverter[K, V](implicit val c: ByteConverter[K],val  d: ByteConverter[V]) extends ComplexByteConverter[scala.collection.immutable.Map[K, V]] with MapStream[K,V] {
  override def write(map: scala.collection.immutable.Map[K, V], output: PrimitiveOutputStream) {
    writeMap(map,output)
  }

  override def read(input: PrimitiveInputStream) = {
    val kvarr = readMap(input)
    scala.collection.immutable.Map[K, V](kvarr: _*)
  }
}

class MutableMapConverter[K, V](implicit val c: ByteConverter[K],val  d: ByteConverter[V]) extends ComplexByteConverter[scala.collection.mutable.Map[K, V]] with MapStream[K,V] {
  override def write(map: scala.collection.mutable.Map[K, V], output: PrimitiveOutputStream) {
    writeMap(map,output)
  }

  override def read(input: PrimitiveInputStream) = {
    val kvarr = readMap(input)
    scala.collection.mutable.Map[K, V](kvarr: _*)
  }
}


class MapConverter[K, V](implicit val c: ByteConverter[K],val  d: ByteConverter[V]) extends ComplexByteConverter[Map[K, V]] with MapStream[K,V] {
  override def write(map: Map[K, V], output: PrimitiveOutputStream) {
    writeMap(map,output)
  }

  override def read(input: PrimitiveInputStream) = {
    val kvarr = readMap(input)
    Map[K, V](kvarr: _*)
  }
}


class MutableSetConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[scala.collection.mutable.Set[T]] with CollStream[T] {

  override def write(set: scala.collection.mutable.Set[T], output: PrimitiveOutputStream) {
    writeColl(set, set.size, output, c)
  }

  override def read(input: PrimitiveInputStream): scala.collection.mutable.Set[T] = {
   scala.collection.mutable.Set(readColl(input, c):_*)
  }
}


class ImmutableSetConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[scala.collection.immutable.Set[T]] with CollStream[T] {

  override def write(set: scala.collection.immutable.Set[T], output: PrimitiveOutputStream) {
    writeColl(set, set.size, output, c)
  }

  override def read(input: PrimitiveInputStream): scala.collection.immutable.Set[T] = {
    readColl(input, c).toSet
  }
}


class SetConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[Set[T]] with CollStream[T] {

  override def write(set: Set[T], output: PrimitiveOutputStream) {
    writeColl(set, set.size, output, c)
  }

  override def read(input: PrimitiveInputStream): Set[T] = {
    readColl(input, c).toSet
  }
}


class SeqConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[Seq[T]] with CollStream[T] {
  override def write(seq: Seq[T], output: PrimitiveOutputStream) {
    writeColl(seq, seq.length, output, c)
  }

  override def read(input: PrimitiveInputStream) = readColl(input, c).toSeq
}

class BufferConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[Buffer[T]] with CollStream[T] {
  override def write(buf: Buffer[T], output: PrimitiveOutputStream) {
    writeBuf(buf, output)
  }

  def writeBuf(buf: Buffer[T], output: PrimitiveOutputStream) {
    writeColl(buf, buf.length, output, c)
  }

  override def read(input: PrimitiveInputStream) = readColl(input, c)
}

trait CollStream[T] {

  def writeColl(items: Iterable[T], length: Int, output: PrimitiveOutputStream, c: ByteConverter[T]) {

    output.writeInt(length)

    val iter = items.iterator
    while (iter.hasNext) {
      val t = iter.next()
      val bytes = c.toBytes(t)
      output.writeInt(bytes.length)
      output.write(bytes)
    }
  }

  def readColl(input: PrimitiveInputStream, c: ByteConverter[T]): Buffer[T] = {
    val length = input.readInt()
    val cpx = if (c.isInstanceOf[ComplexByteConverter[T]]) c.asInstanceOf[ComplexByteConverter[T]] else null

    var i = 0
    val buff = Buffer[T]()
    while (i < length) {
      val arrLength = input.readInt()
      if (cpx != null) {
        buff += cpx.read(input)
      } else {
        val arr = new Array[Byte](arrLength)
        input.read(arr)
        buff += c.fromBytes(arr)
      }
      i = i + 1
    }

    buff
  }
}

