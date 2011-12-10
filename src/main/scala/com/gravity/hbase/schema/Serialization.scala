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
import java.util.NavigableSet
import scala.collection.mutable.Buffer
import org.joda.time.DateTime

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
}

/** Expresses an output stream that can write ordered primitives into a binary output, and can also use the ByteConverter[T] interface to write serializable objects.
  */
class PrimitiveOutputStream(output: OutputStream) extends DataOutputStream(output) {

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

    val dout = new PrimitiveOutputStream(bos)
    write(t, dout)

    bos.toByteArray
  }

  def write(data: T, output: PrimitiveOutputStream)

  override def fromBytes(bytes: Array[Byte]): T = {
    val din = new PrimitiveInputStream(new ByteArrayInputStream(bytes))
    read(din)
  }

  def read(input: PrimitiveInputStream): T
}


class MapConverter[K, V](implicit c: ByteConverter[K], d: ByteConverter[V]) extends ComplexByteConverter[Map[K, V]] {
  override def write(map: Map[K, V], output: PrimitiveOutputStream) {
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

  override def read(input: PrimitiveInputStream) = {
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

  override def write(set: Set[T], output: PrimitiveOutputStream) {
    val length = set.size
    output.writeInt(length)

    set.foreach {
      itm =>
        val bytes = c.toBytes(itm)
        output.writeInt(bytes.size)
        output.write(bytes)
    }
  }

  override def read(input: PrimitiveInputStream): Set[T] = {
    val length = input.readInt()
    Set((for (i <- 0 until length) yield {
      val arrLength = input.readInt()
      val arr = new Array[Byte](arrLength)
      input.read(arr)
      c.fromBytes(arr)
    }): _*)
  }
}


class SeqConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[Seq[T]] {
  override def write(seq: Seq[T], output: PrimitiveOutputStream) {
    writeSeq(seq, output)
  }

  def writeSeq(seq: Seq[T], output: PrimitiveOutputStream) {
    val length = seq.size
    output.writeInt(length)

    for (t <- seq) {
      val bytes = c.toBytes(t)
      output.writeInt(bytes.size)
      output.write(bytes)
    }
  }

  override def read(input: PrimitiveInputStream) = readSeq(input)

  def readSeq(input: PrimitiveInputStream) = {
    val length = input.readInt()

    Seq((for (i <- 0 until length) yield {
      val arrLength = input.readInt()
      val arr = new Array[Byte](arrLength)
      input.read(arr)
      c.fromBytes(arr)
    }): _*)
  }

}

class BufferConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[Buffer[T]] {
  override def write(buf: Buffer[T], output: PrimitiveOutputStream) {
    writeBuf(buf, output)
  }

  def writeBuf(buf: Buffer[T], output: PrimitiveOutputStream) {
    val length = buf.size
    output.writeInt(length)

    for (t <- buf) {
      val bytes = c.toBytes(t)
      output.writeInt(bytes.size)
      output.write(bytes)
    }
  }

  override def read(input: PrimitiveInputStream) = readBuf(input)

  def readBuf(input: PrimitiveInputStream) = {
    val length = input.readInt()

    Buffer((for (i <- 0 until length) yield {
      val arrLength = input.readInt()
      val arr = new Array[Byte](arrLength)
      input.read(arr)
      c.fromBytes(arr)
    }): _*)
  }

}


