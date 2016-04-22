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

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util._
import org.joda.time.ReadableInstant

import scala.collection.JavaConversions._
import scala.collection._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/**
 * Expresses a scan, get, or batched get against hbase.  Which one it becomes depends on what
 * calls you make.  If you specify withKey() it will
 * become a Get, withKeys() will make into a batched get, and no keys at all will make it a Scan.
 *
 * @tparam T the table to work with
 * @tparam R the row key type
 * @tparam RR the row result type
 */
trait BaseQuery[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]] {

  val table: HbaseTable[T, R, RR]
  val keys: mutable.Buffer[Array[Byte]] = mutable.Buffer[Array[Byte]]()
  val families: mutable.Buffer[Array[Byte]] = mutable.Buffer[Array[Byte]]()
  val columns: mutable.Buffer[(Array[Byte], Array[Byte])] = mutable.Buffer[(Array[Byte], Array[Byte])]()
  var currentFilter: Filter = _
  // new FilterList(Operator.MUST_PASS_ALL)
  var startRowBytes: Array[Byte] = null
  var endRowBytes: Array[Byte] = null
  var batchSize: Int = -1
  var startTime: Long = Long.MinValue
  var endTime: Long = Long.MaxValue

  def filter(filterFx: ((FilterBuilder) => Unit)*): this.type = {
    val fb = new FilterBuilder(true)
    for (fx <- filterFx) {
      fx(fb)
    }
    currentFilter = fb.coreList
    this
  }

  def filterOr(filterFx: ((FilterBuilder) => Unit)*): this.type = {
    val fb = new FilterBuilder(false)
    for (fx <- filterFx) {
      fx(fb)
    }
    currentFilter = fb.coreList
    this

  }

  /**
   * Lets you make your own filter, for experimentation
   * @param builder the builder what builds a filter
   */
  def withFilter(builder : => Filter) {
    currentFilter = builder
  }

  class FilterBuilder(and: Boolean) {
    var coreList: FilterList = if (and) new FilterList(Operator.MUST_PASS_ALL) else new FilterList(Operator.MUST_PASS_ONE)
    val clauseBuilder: ClauseBuilder = new ClauseBuilder()

    private def addFilter(filter: FilterList) {
      //coreList = filter
      coreList.addFilter(filter)
    }


    def or(clauses: ((ClauseBuilder) => Option[Filter])*): FilterBuilder = {
      val orFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE)
      for (ctx <- clauses) {
        val filter = ctx(clauseBuilder)
        if (filter.isDefined) {
          orFilter.addFilter(filter.get)
        }
      }
      if (orFilter.getFilters.size() > 0) {
        addFilter(orFilter)
      }
      this
    }

    def and(clauses: ((ClauseBuilder) => Option[Filter])*): FilterBuilder = {
      val andFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL)
      for (cfx <- clauses) {
        val filter = cfx(clauseBuilder)
        if (filter.isDefined) {
          andFilter.addFilter(filter.get)
        }
      }
      if (andFilter.getFilters.size() > 0) {
        addFilter(andFilter)
      }
      this
    }

  }

  class ClauseBuilder() {

    def table: T = BaseQuery.this.table.asInstanceOf[T]

    def columnValueMustNotEqual[F, K, V](column: (T) => Column[T, R, F, K, V], value: V): Some[SingleColumnValueFilter] = {
      val c = column(table.pops)
      val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.NOT_EQUAL, c.valueConverter.toBytes(value))
      vc.setFilterIfMissing(true)
      vc.setLatestVersionOnly(true)
      Some(vc)
    }

    def columnValueMustStartWith[F, K, V](column: (T) => Column[T, R, F, K, String], prefix: String): Some[SingleColumnValueFilter] = {
      val c = column(table.pops)
      val prefixFilter = new BinaryPrefixComparator(Bytes.toBytes(prefix))
      val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.EQUAL, prefixFilter)
      Some(vc)
    }


    def noClause: None.type = None

    def columnValueMustContain[F, K, V](column: (T) => Column[T, R, F, K, String], substr: String): Some[SingleColumnValueFilter] = {
      val c = column(table.pops)
      val substrFilter = new SubstringComparator(substr)
      val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.EQUAL, substrFilter)
      Some(vc)
    }

    /**
     * Untested
     */
    def whereFamilyHasKeyGreaterThan[F, K](family: (T) => ColumnFamily[T, R, F, K, _], key: K): Some[SkipFilter] = {
      val f = family(table.pops)
      val fl = new FilterList(Operator.MUST_PASS_ALL)
      val ts = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(f.keyConverter.toBytes(key)))
      val ff = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(f.familyBytes))
      fl.addFilter(ts)
      fl.addFilter(ff)
      val sk = new SkipFilter(fl)
      Some(sk)
    }

    def columnValueMustPassRegex[F, K, V](column: (T) => Column[T, R, F, K, String], regex: String): Some[SingleColumnValueFilter] = {
      val c = column(table.pops)
      val regexFilter = new RegexStringComparator(regex)
      val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.EQUAL, regexFilter)
      Some(vc)
    }


    def columnValueMustNotContain[F, K, V](column: (T) => Column[T, R, F, K, String], substr: String): Some[SingleColumnValueFilter] = {
      val c = column(table.pops)
      val substrFilter = new SubstringComparator(substr)
      val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.NOT_EQUAL, substrFilter)
      Some(vc)
    }


    def maxRowsPerServer(rowsize: Int): Option[Filter] = {
      val pageFilter = new PageFilter(rowsize)
      Some(pageFilter)
    }

    def columnValueMustEqual[F, K, V](column: (T) => Column[T, R, F, K, V], value: V): Some[SingleColumnValueFilter] = {
      val c = column(table.pops)
      val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.EQUAL, c.valueConverter.toBytes(value))
      vc.setFilterIfMissing(true)
      vc.setLatestVersionOnly(true)
      Some(vc)
    }

    def columnValueMustBeIn[F,K,V](column: (T) => Column[T,R,F,K,V], values: Set[V]): Some[FilterList] = {
      val c = column(table.pops)
      val fl = new FilterList(FilterList.Operator.MUST_PASS_ONE)
      values.foreach{value=>
        val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.EQUAL, c.valueConverter.toBytes(value))
        vc.setFilterIfMissing(true)
        vc.setLatestVersionOnly(true)
        fl.addFilter(vc)
      }

      Some(fl)
    }

    def columnValueMustBeGreaterThan[F, K, V](column: (T) => Column[T, R, F, K, V], value: V): Some[SingleColumnValueFilter] = {
      val c = column(table.pops)
      val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.GREATER, c.valueConverter.toBytes(value))
      vc.setFilterIfMissing(true)
      vc.setLatestVersionOnly(true)
      Some(vc)
    }

    def columnValueMustBeLessThan[F, K, V](column: (T) => Column[T, R, F, K, V], value: V): Some[SingleColumnValueFilter] = {
      val c = column(table.pops)
      val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.LESS, c.valueConverter.toBytes(value))
      vc.setFilterIfMissing(true)
      vc.setLatestVersionOnly(true)
      Some(vc)
    }

    def columnValueMustBePresent[F, K, V](column: (T) => Column[T, R, F, K, V]): Some[SingleColumnValueFilter] = {
      val c = column(table.pops)
      val vc = new SingleColumnValueFilter(c.familyBytes, c.columnBytes, CompareOp.NOT_EQUAL, Bytes.toBytes(0))
      vc.setFilterIfMissing(true)
      vc.setLatestVersionOnly(true)
      Some(vc)
    }

    def lessThanColumnKey[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], value: K): Some[FilterList] = {
      val fam = family(table.pops)
      val valueFilter = new QualifierFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(fam.keyConverter.toBytes(value)))
      val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(family(table.pops).familyBytes))
      val andFilter = new FilterList(Operator.MUST_PASS_ALL)
      andFilter.addFilter(familyFilter)
      andFilter.addFilter(valueFilter)
      Some(andFilter)
    }

    def greaterThanColumnKey[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], value: K): Some[FilterList] = {
      val fam = family(table.pops)
      val andFilter = new FilterList(Operator.MUST_PASS_ALL)
      val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(fam.familyBytes))
      val valueFilter = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(fam.keyConverter.toBytes(value)))
      andFilter.addFilter(familyFilter)
      andFilter.addFilter(valueFilter)
      Some(andFilter)
    }

    //  def columnFamily[F,K,V](family: (T) => ColumnFamily[T,R,F,K,V])(implicit c: ByteConverter[F]): Query[T,R] = {
    //    val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(family(table.pops).familyBytes))
    //    currentFilter.addFilter(familyFilter)
    //    this
    //  }

    /**
     * This filter only returns rows that have the desired column (as expected),
     * but does NOT actually return the column value with the results (which is behavior that you probably didn't expect).
     *
     * That's pretty surprising behavior, so it's now deprecated in favor of columnValueMustBePresent,
     * which filters out rows that don't contain the column and also does actually return the column value with the results.
     */
    @deprecated def whereColumnMustExist[F, K, _](column: (T) => Column[T, R, F, K, _]): Some[SingleColumnValueExcludeFilter] = {
      val c = column(table.pops)
      val valFilter = new SingleColumnValueExcludeFilter(c.familyBytes, c.columnBytes, CompareOp.NOT_EQUAL, new Array[Byte](0))
      valFilter.setFilterIfMissing(true)
      Some(valFilter)
    }

    def betweenColumnKeys[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], lower: K, upper: K): Some[FilterList] = {
      val fam = family(table.pops)
      val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(fam.familyBytes))
      val begin = new QualifierFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(fam.keyConverter.toBytes(lower)))
      val end = new QualifierFilter(CompareOp.LESS_OR_EQUAL, new BinaryComparator(fam.keyConverter.toBytes(upper)))

      val filterList = new FilterList(Operator.MUST_PASS_ALL)
      filterList.addFilter(familyFilter)
      filterList.addFilter(begin)
      filterList.addFilter(end)
      Some(filterList)
    }

    def inFamily[F](family: (T) => ColumnFamily[T, R, F, _, _]): Some[FamilyFilter] = {
      val fam = family(table.pops)
      val ff = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(fam.familyBytes))
      Some(ff)
    }

    def allInFamilies[F](familyList: ((T) => ColumnFamily[T, R, F, _, _])*): Some[FilterList] = {
      val filterList = new FilterList(Operator.MUST_PASS_ONE)
      for (family <- familyList) {
        val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(family(table.pops).familyBytes))
        filterList.addFilter(familyFilter)
      }
      Some(filterList)
    }

    /**
     * Limits the amount of columns returned within a single family where `pageSize` is the number of columns desired and `pageOffset` is the starting column index
     * @param family the family which contains the columns desired to be paged
     * @param pageSize the maximum number of columns to be returned for this family
     * @param pageOffset the offset that is to represent the first column of this page to return
     * @tparam F the family's type
     * @tparam K the family's key type
     * @tparam V the family's value type
     */
    def withPaginationForFamily[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], pageSize: Int, pageOffset: Int): Some[FilterList] = {
      val fam = family(table.pops)
      val familyFilter = new FamilyFilter(CompareOp.EQUAL, new BinaryComparator(fam.familyBytes))
      val paging = new ColumnPaginationFilter(pageSize, pageOffset)

      val filterList = new FilterList(Operator.MUST_PASS_ALL)
      filterList.addFilter(familyFilter)
      filterList.addFilter(paging)

      Some(filterList)
    }
  }

  /**The key to fetch (this makes it into a Get request against hbase) */
  def withKey(key: R): this.type = {
    keys += table.rowKeyConverter.toBytes(key)
    this
  }

  /**Multiple keys to fetch (this makes it into a multi-Get request against hbase) */
  def withKeys(keys: Set[R]): this.type = {
    for (key <- keys) {
      withKey(key)
    }
    this
  }



  def betweenDates(start: ReadableInstant, end: ReadableInstant): this.type = {
    startTime = start.getMillis
    endTime = end.getMillis
    this
  }

  def afterDate(start: ReadableInstant): this.type = {
    startTime = start.getMillis
    this
  }

  def untilDate(end: ReadableInstant): this.type = {
    endTime = end.getMillis
    this
  }

  def withStartRow(row: R): this.type = {
    startRowBytes = table.rowKeyConverter.toBytes(row)
    this
  }

  def withEndRow(row: R): this.type = {
    endRowBytes = table.rowKeyConverter.toBytes(row)
    this
  }

  def withBatchSize(size: Int): this.type = {
    batchSize = size
    this
  }

}

trait MinimumFiltersToExecute[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]] {

  this: BaseQuery[T, R, RR] =>

  def withFamilies[F](firstFamily: (T) => ColumnFamily[T, R, F, _, _], familyList: ((T) => ColumnFamily[T, R, F, _, _])*): Query2[T, R, RR]

  def withColumnsInFamily[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], firstColumn: K, columnList: K*): Query2[T, R, RR]

  @deprecated("withColumnsInFamily can select one or more columns from a single family")
  def withColumn[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], columnName: K): Query2[T, R, RR]

  @deprecated("withColumns can select one or more columns")
  def withColumn[F, K, V](column: (T) => Column[T, R, F, K, V]): Query2[T, R, RR]

  def withColumns[F, K, V](firstColumn: (T) => Column[T, R, F, _, _], columnList: ((T) => Column[T, R, F, _, _])*): Query2[T, R, RR]

}

class Query2[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]] private(
                                                                                override val table: HbaseTable[T, R, RR],
                                                                                override val keys: mutable.Buffer[Array[Byte]],
                                                                                override val families: mutable.Buffer[Array[Byte]],
                                                                                override val columns: mutable.Buffer[(Array[Byte], Array[Byte])]) extends BaseQuery[T, R, RR] with MinimumFiltersToExecute[T, R, RR] {

  private[schema] def this(
                              table: HbaseTable[T, R, RR],
                              keys: mutable.Buffer[Array[Byte]] = mutable.Buffer[Array[Byte]](),
                              families: mutable.Buffer[Array[Byte]] = mutable.Buffer[Array[Byte]](),
                              columns: mutable.Buffer[(Array[Byte], Array[Byte])] = mutable.Buffer[(Array[Byte], Array[Byte])](),
                              currentFilter: Filter,
                              startRowBytes: Array[Byte],
                              endRowBytes: Array[Byte],
                              batchSize: Int,
                              startTime: Long,
                              endTime: Long) = {
    this(table, keys, families, columns)
    this.currentFilter = currentFilter
    this.startRowBytes = startRowBytes
    this.endRowBytes = endRowBytes
    this.batchSize = batchSize
    this.startTime = startTime
    this.endTime = endTime
  }

  override def withFamilies[F](firstFamily: (T) => ColumnFamily[T, R, F, _, _], familyList: ((T) => ColumnFamily[T, R, F, _, _])*): Query2[T, R, RR] = {
    for (family <- firstFamily +: familyList) {
      val fam = family(table.pops)
      families += fam.familyBytes
    }
    this
  }

  override def withColumnsInFamily[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], firstColumn: K, columnList: K*): Query2[T, R, RR] = {
    val fam = family(table.pops)
    for (column <- firstColumn +: columnList) {
      columns += (fam.familyBytes -> fam.keyConverter.toBytes(column))
    }
    this
  }

  override def withColumn[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], columnName: K): Query2[T, R, RR] = {
    val fam = family(table.pops)
    columns += (fam.familyBytes -> fam.keyConverter.toBytes(columnName))
    this
  }

  override def withColumn[F, K, V](column: (T) => Column[T, R, F, K, V]): Query2[T, R, RR] = {
    val col = column(table.pops)
    columns += (col.familyBytes -> col.columnBytes)
    this
  }

  override def withColumns[F, K, V](firstColumn: (T) => Column[T, R, F, _, _], columnList: ((T) => Column[T, R, F, _, _])*): Query2[T, R, RR] = {
    for (column <- firstColumn +: columnList) {
      val col = column(table.pops)
      columns += (col.familyBytes -> col.columnBytes)
    }
    this
  }

  def single(tableName: String = table.tableName, ttl: Int = 30, skipCache: Boolean = true, timeOutMs: Int = 0)(implicit conf: Configuration): RR = singleOption(tableName, ttl, skipCache, noneOnEmpty = false, timeOutMs).get

  /**
   *
   * @param tableName The Name of the table
   * @param ttl The time to live for any cached results
   * @param skipCache Whether to skip any cacher defined for this table
   * @param noneOnEmpty If true, will return None if the result is empty.  If false, will return an empty row.  If caching is true, will cache the empty row.
   * @return
   */
  def singleOption(tableName: String = table.tableName, ttl: Int = 30, skipCache: Boolean = true, noneOnEmpty: Boolean = true, timeOutMs: Int = 0)(implicit conf: Configuration): Option[RR] = {
    require(keys.size == 1, "Calling single() with more than one key")
    require(keys.nonEmpty, "Calling a Get operation with no keys specified")
    val get = new Get(keys.head)
    get.setMaxVersions(1)

    if (startTime != Long.MinValue || endTime != Long.MaxValue) {
      get.setTimeRange(startTime, endTime)
    }

    for (family <- families) {
      get.addFamily(family)
    }
    for ((columnFamily, column) <- columns) {
      get.addColumn(columnFamily, column)
    }
    if (currentFilter != null) {
      get.setFilter(currentFilter)
    }

    if (skipCache || table.cache.isInstanceOf[NoOpCache[T, R, RR]])
      table.withTableOption(tableName, conf, timeOutMs) {
        case Some(htable) =>
          val result = htable.get(get)
          if (noneOnEmpty && result.isEmpty) {
            None
          }
          else if (!noneOnEmpty && result.isEmpty) {
            val qr = table.emptyRow(keys.head)
            Some(qr)
          }
          else {
            val qr = table.buildRow(result)
            Some(qr)
          }
        case None => None
      }
    else {
      val cacheKey = table.cache.getKeyFromGet(get)
      table.cache.getLocalResult(cacheKey) match {
        case Found(result) =>
          table.cache.instrumentRequest(1, 1, 0, 0, 0)
          Some(result)
        case FoundEmpty =>
          table.cache.instrumentRequest(1, 1, 0, 0, 0)
          if (noneOnEmpty)
            None
          else {
            val qr = table.emptyRow(keys.head)
            Some(qr)
          }
        case NotFound =>
          table.cache.getRemoteResult(cacheKey) match {
            case Found(result) =>
              table.cache.putResultLocal(cacheKey, Some(result), ttl)
              table.cache.instrumentRequest(1, 0, 1, 1, 0)
              Some(result)
            case FoundEmpty =>
              table.cache.instrumentRequest(1, 0, 1, 1, 0)
              if (noneOnEmpty) //then also don't cache.
                None
              else {
                table.cache.putResultLocal(cacheKey, None, ttl) //just cache None, so the cache will return "FoundEmpty", but RETURN the empty row to keep behavior consistent
                val qr = table.emptyRow(keys.head)
                Some(qr)
              }
            case NotFound =>
              val fromTable = table.withTableOption(tableName, conf, timeOutMs) {
                case Some(htable) =>
                  val result = htable.get(get)
                  if (noneOnEmpty && result.isEmpty) {
                    //this means DO NOT CACHE EMPTY. But return None
                    None
                  }
                  else if (!noneOnEmpty && result.isEmpty) {
                    //this means - return empty row, cache None. believe it or not this is easier than the alternative, because of the logic in multi-get
                    table.cache.putResultLocal(cacheKey, None, ttl)
                    table.cache.putResultRemote(cacheKey, None, ttl)
                    val qr = table.emptyRow(keys.head)
                    Some(qr)
                  }
                  else {
                    //the result isn't empty so we don't care about the noneOnEmpty settings
                    val qr = table.buildRow(result)
                    val ret = Some(qr)
                    table.cache.putResultLocal(cacheKey, ret, ttl)
                    table.cache.putResultRemote(cacheKey, ret, ttl)
                    ret
                  }
                case None => None //the table doesn't even exist. let's just go on our merry way, because it's probably something bigger than us gone wrong
              }
              table.cache.instrumentRequest(1, 0, 1, 0, 1)
              fromTable
            case Error(message, exceptionOption) => //don't save back to remote if there was an error - it's likely overloaded and this just creates a cascading failure
              val fromTable = table.withTableOption(tableName, conf, timeOutMs) {
                case Some(htable) =>
                  val result = htable.get(get)
                  if (noneOnEmpty && result.isEmpty) {
                    //this means DO NOT CACHE EMPTY. But return None
                    None
                  }
                  else if (!noneOnEmpty && result.isEmpty) {
                    //this means - return empty row, cache None. believe it or not this is easier than the alternative, because of the logic in multi-get
                    table.cache.putResultLocal(cacheKey, None, ttl)
                    val qr = table.emptyRow(keys.head)
                    Some(qr)
                  }
                  else {
                    //the result isn't empty so we don't care about the noneOnEmpty settings
                    val qr = table.buildRow(result)
                    val ret = Some(qr)
                    table.cache.putResultLocal(cacheKey, ret, ttl)
                    ret
                  }
                case None => None //the table doesn't even exist. let's just go on our merry way, because it's probably something bigger than us gone wrong
              }

              table.cache.instrumentRequest(1, 0, 1, 0, 1)
              fromTable
          }
        case Error(message, exceptionOption) => //don't save back to the local cache if there was an error retrieving
          table.cache.getRemoteResult(cacheKey) match {
            case Found(result) =>
              table.cache.instrumentRequest(1, 0, 1, 1, 0)
              Some(result)
            case FoundEmpty =>
              table.cache.instrumentRequest(1, 0, 1, 1, 0)
              if (noneOnEmpty)
                None
              else {
                val qr = table.emptyRow(keys.head)
                Some(qr)
              }
            case NotFound =>
              val fromTable = table.withTableOption(tableName, conf, timeOutMs) {
                case Some(htable) =>
                  val result = htable.get(get)
                  if (noneOnEmpty && result.isEmpty) {
                    None
                  }
                  else if (!noneOnEmpty && result.isEmpty) {
                    val qr = table.emptyRow(keys.head)
                    Some(qr)
                  }
                  else {
                    //the result isn't empty so we don't care about the noneOnEmpty settings
                    val qr = table.buildRow(result)
                    val ret = Some(qr)
                    ret
                  }
                case None => None //the table doesn't even exist. let's just go on our merry way, because it's probably something bigger than us gone wrong
              }
              table.cache.instrumentRequest(1, 0, 1, 0, 1)
              fromTable
            case Error(remoteMessage, remoteExceptionOption) => //don't save back to remote if there was an error - it's likely overloaded and this just creates a cascading failure
              table.cache.instrumentRequest(1, 0, 1, 0, 1)
              table.withTableOption(tableName, conf, timeOutMs) {
                case Some(htable) =>
                  val result = htable.get(get)
                  if (noneOnEmpty && result.isEmpty) {
                    None
                  }
                  else if (!noneOnEmpty && result.isEmpty) {
                    val qr = table.emptyRow(keys.head)
                    Some(qr)
                  }
                  else {
                    val qr = table.buildRow(result)
                    val ret = Some(qr)
                    ret
                  }
                case None => None //the table doesn't even exist. let's just go on our merry way, because it's probably something bigger than us gone wrong
              }
          }
      }
    }
  }

  /**
   * Kept for binary compatibility.  Will be deprecated for multi()
   * @param tableName The Name of the table
   * @param ttl The Time to Live for any cached results
   * @param skipCache Whether to skip any defined cache
   * @return
   */
  def executeMap(tableName: String = table.tableName, ttl: Int = 30, skipCache:Boolean=true, timeOutMs: Int = 0)(implicit conf: Configuration) : Map[R,RR] = multiMap(tableName, ttl, skipCache, returnEmptyRows = false, timeOutMs)

  /**
   * @param tableName The Name of the table
   * @param ttl The Time to Live for any cached results
   * @param skipCache Whether to skip any defined cache
   * @param returnEmptyRows If this is on, then empty rows will be returned and cached.  Often times empty rows are not considered in caching situations
   *                        so this is off by default to keep the mental model simple.
   * @return
   */
  def multiMap(tableName: String = table.tableName, ttl: Int = 30, skipCache: Boolean = true, returnEmptyRows:Boolean=false, timeOutMs: Int = 0)(implicit conf: Configuration): Map[R, RR] = {
    if (keys.isEmpty) return Map.empty[R, RR] // don't get all started with nothing to do

    // init our result map and give it a hint of the # of keys we have
    val resultMap = mutable.Map[R, RR]()
    resultMap.sizeHint(keys.size) // perf optimization

    val localKeysToGetsAndCacheKeys: Map[Int, (Get, String)] = buildKeysToGetsAndCacheKeys()
    val gets = localKeysToGetsAndCacheKeys.values.map(_._1).toList

    if(returnEmptyRows) {
      for (key <- keys) {
        resultMap(table.rowKeyConverter.fromBytes(key)) = table.emptyRow(key)
      }
    }

    if(skipCache || table.cache.isInstanceOf[NoOpCache[T, R, RR]]) {
      if (gets.nonEmpty) {
        table.withTable(tableName, conf, timeOutMs) {
          htable =>
            htable.get(gets).foreach(res => {
              if (res != null && !res.isEmpty) {
                val qr = table.buildRow(res) // construct query result
                resultMap(qr.rowid) = qr // place it in our result map
              }
            })
        }
      }
    }
    else {
      val resultBuffer = mutable.Map[String, Option[RR]]() //the results, built from local cache, remote cache, and finally the
      var localhits = 0
      var localmisses = 0
      var remotehits = 0
      var remotemisses = 0

      val remoteCacheMisses = mutable.Buffer[String]()
      val cacheKeysToGets = localKeysToGetsAndCacheKeys.values.map(tuple => tuple._2 -> tuple._1).toMap //gets.map(get => table.cache.getKeyFromGet(get) -> get).toMap
      val cacheKeysRemaining = mutable.Set[String](localKeysToGetsAndCacheKeys.values.map(_._2).toSeq: _*)

      val localCacheResults = table.cache.getLocalResults(cacheKeysRemaining)

      localCacheResults.foreach { case (key, localCacheResult) =>
        localCacheResult match {
          case Found(result) =>
            cacheKeysRemaining.remove(key)
            localhits += 1
            resultBuffer.update(key, Some(result))
          case FoundEmpty =>
            cacheKeysRemaining.remove(key)
            localhits += 1
            if(returnEmptyRows) resultBuffer.update(key, None)
          case NotFound =>
            localmisses += 1
          case Error(message, exceptionOption) =>
            localmisses += 1
        }
      }

      val remoteCacheResults = table.cache.getRemoteResults(cacheKeysRemaining)

      remoteCacheResults.foreach { case (key, remoteCacheResult) =>
        remoteCacheResult match {
          case Found(result) =>
            resultBuffer.update(key, Some(result))
            cacheKeysRemaining.remove(key)
            remotehits += 1
            table.cache.putResultLocal(key, Some(result), ttl)
          case FoundEmpty =>
            cacheKeysRemaining.remove(key)
            remotehits += 1
            if(returnEmptyRows) {
              //return empty rows also means cache empty rows
              resultBuffer.update(key, None)
              table.cache.putResultLocal(key, None, ttl)
            }
          case NotFound =>
            remoteCacheMisses += key
            remotemisses += 1
          case Error(message, exceptionOption) =>
            //don't record the miss key here, to prevent a saveback
            remotemisses += 1
        }
      }

      //anything in remaining keys that remote cache does not have, add to missing
      cacheKeysRemaining.filter(key => !remoteCacheResults.contains(key)).foreach{ missingKey =>
        remoteCacheMisses += missingKey
        remotemisses += 1
      }

      val allCacheMissGets = cacheKeysRemaining.map(key => cacheKeysToGets(key)).toList

      if (allCacheMissGets.nonEmpty) {
        table.withTable(tableName, conf, timeOutMs) {
          htable =>
            htable.get(allCacheMissGets).foreach(res => {
              if (res != null && !res.isEmpty) {
                val localKey = getIntFromBytes(res.getRow)
                val cacheKey = localKeysToGetsAndCacheKeys(localKey)._2
                val theThingWeWant = Some(table.buildRow(res))
                //put it in local cache now, and the resultBuffer will be used to send to remote in bulk later
                table.cache.putResultLocal(cacheKey, theThingWeWant, ttl)
                resultBuffer.update(cacheKey, theThingWeWant)
              }
            })
        }

        if (returnEmptyRows) {
          //then we need also need to cache Nones for everything that came back empty. while processing each result, we didn't have the associated cache keys,
          //so we have to derive from other data which to associate with None.
          //results.keySet is everything we've gotten back, one way or another
          //cacheKeysToGets.keySet is everything, total.
          //so everything minus things we've already gotten back is things that were missing from the table fetch
          val keyForThingsThatDontExist = cacheKeysToGets.keySet.diff(resultBuffer.keySet) //result buffer will have nones from remote cache and local cache, but the ones from remote will already be in local
          keyForThingsThatDontExist.foreach(keyForThingThatDoesntExist => {
            //and each of those should be cached as None. put it in local cache now, and the resultBuffer will be used to send to remote in bulk later
            table.cache.putResultLocal(keyForThingThatDoesntExist, None, ttl)
            resultBuffer.update(keyForThingThatDoesntExist, None)
          })
        }
      }

      val remoteCacheMissesSet = remoteCacheMisses.toSet
      val resultsToSaveRemote = resultBuffer.filterKeys(key => remoteCacheMissesSet.contains(key)) //we only want to save Nones if we are returning empty rows, but there will only be Nones in the results if that is true
      table.cache.putResultsRemote(resultsToSaveRemote, ttl)
      table.cache.instrumentRequest(keys.size, localhits, localmisses, remotehits, remotemisses)

      resultBuffer.values.foreach {
        case Some(value) => resultMap(value.rowid) = value
        case None => //if we want to return empty rows, they are already there. if we don't... they aren't, and we don't have to change anything.
      }
    }

    resultMap // DONE!
  }

  private def getIntFromBytes(bytes: Array[Byte]) : Int = {
    scala.util.hashing.MurmurHash3.arrayHash(bytes)
  }

  private def buildKeysToGetsAndCacheKeys(): Map[Int, (Get, String)] = {
    if (keys.isEmpty) return Map.empty[Int,(Get, String)] // no keys..? nothing to see here... move along... move along.

    val gets = mutable.Buffer[(Int,Get)]() // buffer for the raw `Get's

    for (key <- keys) {
      val get = new Get(key)
      if (startTime != Long.MinValue || endTime != Long.MaxValue) {
        get.setTimeRange(startTime, endTime)
      }

      gets += Tuple2(getIntFromBytes(key), get)
    }

    // since the families and columns will be identical for all `Get's, only build them once
    val firstGet = gets.head._2

    // add all families to the first `Get'
    for (family <- families) {
      firstGet.addFamily(family)
    }
    // add all columns to the first `Get'
    for ((columnFamily, column) <- columns) {
      firstGet.addColumn(columnFamily, column)
    }
    if (currentFilter != null) {
      firstGet.setFilter(currentFilter)
    }


    var pastFirst = false
    for (get <- gets) {
      if (pastFirst) {
        // we want to skip the first `Get' as it already has families/columns

        // for all subsequent `Get's, we will build their familyMap from the first `Get'
        firstGet.getFamilyMap.foreach((kv: (Array[Byte], util.NavigableSet[Array[Byte]])) => {
          get._2.getFamilyMap.put(kv._1, kv._2)
        })
        if (currentFilter != null) {
          get._2.setFilter(currentFilter)
        }
      }
      else {
        pastFirst = true
      }
    }

    gets.map{case (key, get) => key -> Tuple2(get, table.cache.getKeyFromGet(get))}.toMap //I feel like there must be a way to do this with just one toMap but i'm not sure what it is.
  }

  def makeScanner(maxVersions: Int = 1, cacheBlocks: Boolean = true, cacheSize: Int = 100): Scan = {
    require(keys.isEmpty, "A scanner should not specify keys, use singleOption or execute or executeMap")
    val scan = new Scan()
    scan.setMaxVersions(maxVersions)
    scan.setCaching(cacheSize)
    scan.setCacheBlocks(cacheBlocks)

    if (batchSize > -1) {
      scan.setBatch(batchSize)
    }

    if (startTime != Long.MinValue || endTime != Long.MaxValue) {
      scan.setTimeRange(startTime, endTime)
    }

    if (startRowBytes != null) {
      scan.setStartRow(startRowBytes)
    }
    if (endRowBytes != null) {
      scan.setStopRow(endRowBytes)
    }

    for (family <- families) {
      scan.addFamily(family)
    }
    for (column <- columns) {
      scan.addColumn(column._1, column._2)
    }

    if (currentFilter != null) {
      scan.setFilter(currentFilter)
    }

    scan
  }

  def scan(handler: (RR) => Unit, maxVersions: Int = 1, cacheBlocks: Boolean = true, cacheSize: Int = 100, useLocalCache: Boolean = false, localTTL: Int = 30, timeOutMs: Int = 0)(implicit conf: Configuration) {
    val scan = makeScanner(maxVersions, cacheBlocks, cacheSize)

    val results = if (useLocalCache) mutable.Buffer[RR]() else mutable.Buffer.empty[RR]

    def cacheHandler(rr: RR) {
      if (useLocalCache) results += rr
    }

    def cacheComplete() {
      if (useLocalCache && results.nonEmpty) table.cache.putScanResult(scan, results, localTTL)
    }

    val whatWeGetFromCache = if (useLocalCache) table.cache.getScanResult(scan) else None

    whatWeGetFromCache match {
      case Some(result) =>
        println("cache hit against key " + scan.toString)
        result.foreach(handler)
      case None =>

        table.withTable(table.tableName, conf, timeOutMs) {
          htable =>

            val scanner = htable.getScanner(scan)

            try {
              var done = false
              while (!done) {
                val result = scanner.next()
                if (result != null) {
                  val rr = table.buildRow(result)
                  cacheHandler(rr)
                  handler(rr)
                } else {
                  done = true
                }
              }
            } finally {
              cacheComplete()
              scanner.close()
            }
        }
    }

  }

  def scanToIterable[I](handler: (RR) => I, maxVersions: Int = 1, cacheBlocks: Boolean = true, cacheSize: Int = 100, useLocalCache: Boolean = false, localTTL: Int = 30, timeOutMs: Int = 0)(implicit conf: Configuration): scala.Iterable[I] = {
    val scan = makeScanner(maxVersions, cacheBlocks, cacheSize)

    val results = if (useLocalCache) mutable.Buffer[RR]() else mutable.Buffer.empty[RR]

    def cacheHandler(rr: RR) {
      if (useLocalCache) results += rr
    }

    def cacheComplete() {
      if (useLocalCache && results.nonEmpty) table.cache.putScanResult(scan, results, localTTL)
    }

    val whatWeGetFromCache = if (useLocalCache) table.cache.getScanResult(scan) else None

    val results2 = whatWeGetFromCache match {
      case Some(rrs) => rrs.map(rr => handler(rr))
      case None =>
        val runResults = table.withTable(table.tableName, conf, timeOutMs) {
          htable =>
            val scanner = htable.getScanner(scan)
            try {
              for (result <- scanner; if result != null) yield {
                val rr = table.buildRow(result)
                cacheHandler(rr)
                handler(rr)
              }
            } finally {
              cacheComplete()
              scanner.close()
            }
        }

        runResults
    }
    results2
  }

  trait Stopable extends Throwable

  object YouCanStopNow extends Stopable

  /**Similar to the scan method but if your handler returns false, it will stop scanning.
   *
   */
  def scanUntil(handler: (RR) => Boolean, maxVersions: Int = 1, cacheBlocks: Boolean = true, cacheSize: Int = 100, timeOutMs: Int = 0)(implicit conf: Configuration) {
    table.withTable(table.tableName, conf, timeOutMs) {
      htable =>
        val scan = makeScanner(maxVersions, cacheBlocks, cacheSize)

        val scanner = htable.getScanner(scan)

        try {
          for (result <- scanner) {
            if (!handler(table.buildRow(result))) throw YouCanStopNow
          }
        } catch {
          case _: Stopable => // nothing to see here... move along. move along.
        } finally {
          scanner.close()
        }
    }
  }

}

object Query2 {
  def p(depth: Int = 1, msg: Any) {
    println(("\t" * depth) + msg)
  }

  def printFilter(depth: Int, f: Filter) {
    p(depth, "Filter All Remaining: " + f.filterAllRemaining())
    p(depth, "Has Filter Row: " + f.hasFilterRow)
    p(depth, "To String: " + f.toString)
    f match {
      case fl: FilterList =>
        p(depth, "Operator: " + fl.getOperator)
        fl.getFilters.foreach(sf => printFilter(depth + 1, sf))
      case _ =>
    }
  }

}
