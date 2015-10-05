/**Licensed to Gravity.com under one
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
import org.apache.hadoop.io.Writable
import scala.collection._
import mutable.{ArrayBuffer, Buffer}
import org.joda.time.DateTime
import com.gravity.hbase.schema._
import java.math.BigInteger
import java.nio.ByteBuffer
import org.apache.commons.lang.ArrayUtils
import java.util.{Arrays, HashMap}
import org.apache.hadoop.hbase.util.Bytes.ByteArrayComparator
import org.apache.hadoop.hbase.{HColumnDescriptor, KeyValue}
import com.gravity.hbase.{AnyConverterSignal, AnyNotSupportedException}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */



trait Schema {
  val tables = scala.collection.mutable.Set[HbaseTable[_, _, _]]()

  def table[T <: HbaseTable[T, _, _]](table: T) = {
    tables += table
    table.init()
    table
  }


}

case class YearDay(year: Int, day: Int)

case class CommaSet(items: Set[String]) {
  def mkString: String = items.mkString

  def mkString(sep: String): String = items.mkString(sep)

  def mkString(start: String, sep: String, end: String): String = items.mkString(start, sep, end)
}

object CommaSet {
  val empty = CommaSet(Set.empty[String])

  def apply(items: String*): CommaSet = CommaSet(items.toSet)
}

