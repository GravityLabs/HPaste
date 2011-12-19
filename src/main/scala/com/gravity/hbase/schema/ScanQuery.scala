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

class ScanQuery[T <: HbaseTable[T, R,RR], R, RR <: HRow[T,R]](table: HbaseTable[T, R,RR]) {
  val scan = new Scan()
  scan.setCaching(100)
  scan.setMaxVersions(1)

  val filterBuffer = scala.collection.mutable.Buffer[Filter]()

  def executeWithCaching(operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL, ttl: Int = 30): Seq[RR] = {
    completeScanner(operator)
    val results = table.cache.getScanResult(scan) match {
      case Some(result) => {
        println("cache hit against key " + scan.toString)
        result
      }
      case None => {
        println("cache miss against key " + scan.toString)
        val results = scala.collection.mutable.Buffer[RR]()
        table.withTable() {
          htable =>
            val scanner = htable.getScanner(scan)
            try {
              for (result <- scanner) {
                results += table.buildRow(result)
              }
              table.cache.putScanResult(scan, results.toSeq, ttl)
              results
            } finally {
              scanner.close()
            }
        }
      }
    }

    results
  }

  def execute(handler: (RR) => Unit, operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL) {
    table.withTable() {
      htable =>
        completeScanner(operator)
        val scanner = htable.getScanner(scan)

        try {
          for (result <- scanner) {
            handler(table.buildRow(result))
          }
        } finally {
          scanner.close()
        }
    }
  }

  /*
  Prepares the scanner for use by chaining the filters together.  Should be called immediately before passing the scanner to the table.
   */
  def completeScanner(operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL) {
    if (filterBuffer.size > 0) {
      val filterList = new FilterList(operator)
      filterBuffer.foreach {filter => filterList.addFilter(filter)}
      scan.setFilter(filterList)
    }
  }

  def executeToSeq[I](handler: (RR) => I, operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL): Seq[I] = {
    val results = Buffer[I]()

    table.withTable() {
      htable =>
        completeScanner(operator)
        val scanner = htable.getScanner(scan)

        try {
          for (result <- scanner; if (result != null)) {
            results += handler(table.buildRow(result))
          }
        } finally {
          scanner.close()
        }
    }

    results.toSeq
  }

  def withFamily[F, K, V](family: FamilyExtractor[T, R, F, K, V]) = {
    val fam = family(table.pops)
    scan.addFamily(fam.familyBytes)
    this
  }

  def withColumn[F, K, V](column: ColumnExtractor[T, R, F, K, V]) = {
    val col = column(table.pops)
    scan.addColumn(col.familyBytes, col.columnBytes)

    this
  }

  def withFilter(f: () => Filter) = {
    filterBuffer.add(f())
    this
  }

  def addFilter(filter: Filter) = withFilter(() => filter)

  def withColumnOp[F, K, V](column: ColumnExtractor[T, R, F, K, V], compareOp: CompareOp, value: Option[V], excludeIfNull: Boolean)(implicit c: ByteConverter[V]) = {
    val col = column(table.pops)
    val filter = new SingleColumnValueFilter(
      col.familyBytes,
      col.columnBytes,
      compareOp,
      value match {case Some(v) => c.toBytes(v); case None => new Array[Byte](0)}
    )
    filter.setFilterIfMissing(excludeIfNull)
    filterBuffer += filter
    this
  }

  def withStartKey[R](key: R)(implicit c: ByteConverter[R]) = {scan.setStartRow(c.toBytes(key)); this}

  def withEndKey[R](key: R)(implicit c: ByteConverter[R]) = {scan.setStopRow(c.toBytes(key)); this}

  def withCaching(rowsToCache: Int) = {scan.setCaching(rowsToCache); this;}
}


