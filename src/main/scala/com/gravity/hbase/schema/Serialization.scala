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

    set.foreach {
      itm =>
        val bytes = c.toBytes(itm)
        output.writeInt(bytes.size)
        output.write(bytes)
    }
  }

  override def read(input: DataInputStream): Set[T] = {
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
    }): _*)
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
    }): _*)
  }

}


