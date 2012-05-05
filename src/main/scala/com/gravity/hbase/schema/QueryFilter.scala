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
import scala.collection._
import java.util.NavigableSet
import scala.collection.mutable.Buffer
import org.joda.time.DateTime
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.filter.FilterList.Operator
/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

@deprecated("Use query2 and filter()")
class QueryFilter[T <: HbaseTable[T,R,_],R](table:HbaseTable[T,R,_]) {
  var currentFilter : FilterList = new FilterList(Operator.MUST_PASS_ALL)

  def and = {
    if(currentFilter == null) {
      currentFilter = new FilterList(Operator.MUST_PASS_ALL)
    }else {
      val encompassingFilter = new FilterList(Operator.MUST_PASS_ALL)
      encompassingFilter.addFilter(currentFilter)
      currentFilter = encompassingFilter
    }
    this
  }

  def or = {
    if(currentFilter == null) {
      currentFilter = new FilterList(Operator.MUST_PASS_ONE)
    }else {
      val encompassingFilter = new FilterList(Operator.MUST_PASS_ONE)
      encompassingFilter.addFilter(currentFilter)
      currentFilter = encompassingFilter
    }
    this
  }

  def lessThanColumnKey[F,K,V](family: (T) => ColumnFamily[T,R,F,K,V], value:K)(implicit k:ByteConverter[K]) = {
    val valueFilter = new QualifierFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(k.toBytes(value)))
    val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(family(table.pops).familyBytes))
    val andFilter = new FilterList(Operator.MUST_PASS_ALL)
    andFilter.addFilter(familyFilter)
    andFilter.addFilter(valueFilter)
    currentFilter.addFilter(familyFilter)
    this
  }

  def greaterThanColumnKey[F,K,V](family: (T) => ColumnFamily[T,R,F,K,V], value:K)(implicit k:ByteConverter[K]) = {
    val andFilter = new FilterList(Operator.MUST_PASS_ALL)
    val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(family(table.pops).familyBytes))
    val valueFilter = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(k.toBytes(value)))
    andFilter.addFilter(familyFilter)
    andFilter.addFilter(valueFilter)
    currentFilter.addFilter(familyFilter)
    this
  }


  def betweenColumnKeys[F,K,V](family: (T) => ColumnFamily[T,R,F,K,V], lower: K, upper: K)(implicit f:ByteConverter[K]) = {
    val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(family(table.pops).familyBytes))
    val begin = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(f.toBytes(lower)))
    val end = new QualifierFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(f.toBytes(upper)))
    val filterList = new FilterList(Operator.MUST_PASS_ALL)
    filterList.addFilter(familyFilter)
    filterList.addFilter(begin)
    filterList.addFilter(end)
    currentFilter.addFilter(filterList)

    this
  }

  def allOfFamilies[F](familyList: ((T) => ColumnFamily[T,R,F,_,_])*) = {
    val filterList = new FilterList(Operator.MUST_PASS_ONE)
    for(family <- familyList) {
      val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(family(table.pops).familyBytes))
      filterList.addFilter(familyFilter)
    }
    currentFilter.addFilter(filterList)
    this
  }



}


