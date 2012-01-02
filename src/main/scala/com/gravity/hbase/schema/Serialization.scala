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
import org.joda.time.DateTime
import scala.collection.mutable.{ListBuffer, Buffer}

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

  def fromBytes(bytes: Array[Byte]): T = fromBytes(bytes, 0, bytes.length)

  def fromBytes(bytes: Array[Byte], offset:Int, length:Int) : T

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

  override def fromBytes(bytes: Array[Byte], offset:Int, length:Int) : T = {
    val din = new PrimitiveInputStream(new ByteArrayInputStream(bytes,offset,length))
    read(din)
  }

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
      output.writeInt(keyBytes.length)
      output.write(keyBytes)
      output.writeInt(valBytes.length)
      output.write(valBytes)
    }
  }

  override def read(input: PrimitiveInputStream) = {
    val length = input.readInt()
    val kvarr = Array.ofDim[(K,V)](length)

    var i = 0
    while(i < length) {
      val keyLength = input.readInt
      val keyArr = new Array[Byte](keyLength)
      input.read(keyArr)
      val key = c.fromBytes(keyArr)

      val valLength = input.readInt
      val valArr = new Array[Byte](valLength)
      input.read(valArr)
      val value = d.fromBytes(valArr)

      kvarr(i) = (key -> value)
      i = i+1
    }
    Map[K,V](kvarr:_*)
  }
}

//TODO: T is not available at runtime, and Arrays are not generic.  Figure out classmanifest workaround
//class ArrayConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[Array[T]] {
//  override def write(set: Array[T], output: PrimitiveOutputStream) {
//    val length = set.length
//    output.writeInt(length)
//    var i = 0
//    while(i < length) {
//      val itm = set(i)
//      val bytes = c.toBytes(itm)
//      output.writeInt(bytes.length)
//      output.write(bytes)
//      i = i+1
//    }
//  }
//
//  override def read(input: PrimitiveInputStream) : Array[T] = {
//    val length = input.readInt()
//    val arr = Array.ofDim[T](length)
//    var i = 0
//    while(i < length) {
//      val byteLength = input.readInt()
//      val itmArr = new Array[Byte](byteLength)
//      val itm = c.fromBytes(itmArr)
//      arr(i) = itm
//      i = i + 1
//    }
//    arr
//  }
//}

class SetConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[Set[T]] with CollStream[T] {

  override def write(set: Set[T], output: PrimitiveOutputStream) {
    writeColl(set,set.size,output,c)
  }

  override def read(input: PrimitiveInputStream): Set[T] = {
    readColl(input, c).toSet
  }
}


class SeqConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[Seq[T]] with CollStream[T]{
  override def write(seq: Seq[T], output: PrimitiveOutputStream) {
    writeColl(seq,seq.length,output,c)
  }

  override def read(input: PrimitiveInputStream) = readColl(input,c).toSeq
}

class BufferConverter[T](implicit c: ByteConverter[T]) extends ComplexByteConverter[Buffer[T]] with CollStream[T]{
  override def write(buf: Buffer[T], output: PrimitiveOutputStream) {
    writeBuf(buf, output)
  }

  def writeBuf(buf: Buffer[T], output: PrimitiveOutputStream) {
    writeColl(buf,buf.length,output,c)
  }

  override def read(input: PrimitiveInputStream) =readColl(input,c)
}

trait CollStream[T] {

  def writeColl(items:Iterable[T], length:Int, output:PrimitiveOutputStream, c:ByteConverter[T]) {

    output.writeInt(length)

    val iter = items.iterator
    while(iter.hasNext) {
      val t = iter.next()
      val bytes = c.toBytes(t)
      output.writeInt(bytes.length)
      output.write(bytes)
    }
  }

  def readColl(input:PrimitiveInputStream, c:ByteConverter[T]) : Buffer[T] = {
    val length = input.readInt()
    val cpx = c.asInstanceOf[ComplexByteConverter[T]]

    var i = 0
    val buff = Buffer[T]()
    while(i < length) {
      val arrLength = input.readInt()
      if(cpx != null) {
        buff += cpx.read(input)
      }else {
        val arr = new Array[Byte](arrLength)
        input.read(arr)
        buff += c.fromBytes(arr)
      }
      i = i + 1
    }

    buff
  }
}

