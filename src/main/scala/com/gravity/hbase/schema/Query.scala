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

@deprecated
class Query[T <: HbaseTable[T, R,RR], R, RR <: HRow[T,R]](table: HbaseTable[T, R, RR]) {

  val keys = Buffer[Array[Byte]]()
  val families = Buffer[Array[Byte]]()
  val columns = Buffer[(Array[Byte], Array[Byte])]()


  def withKey(key: R)= {
    keys += table.rowKeyConverter.toBytes(key)
    this
  }

  def withKeys(keys: Set[R]) = {
    for (key <- keys) {
      withKey(key)
    }
    this
  }

  def withColumnFamily[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V]): Query[T, R, RR] = {
    val fam = family(table.pops)
    families += fam.familyBytes
    this
  }

  def withColumn[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], columnName: K): Query[T, R, RR] = {
    val fam = family(table.pops)
    columns += (fam.familyBytes -> fam.keyConverter.toBytes(columnName))
    this
  }

  def withColumn[F, K, V](column: (T) => Column[T, R, F, K, V]): Query[T, R, RR] = {
    val col = column(table.pops)
    columns += (col.familyBytes -> col.columnBytes)
    this
  }

  def single(tableName: String = table.tableName, ttl: Int = 30, skipCache: Boolean = true) = singleOption(tableName, ttl, skipCache, false).get


  def singleOption(tableName: String = table.tableName, ttl: Int = 30, skipCache: Boolean = true, noneOnEmpty: Boolean = true): Option[RR] = {
    require(keys.size == 1, "Calling single() with more than one key")
    val get = new Get(keys.head)
    get.setMaxVersions(1)
    for (family <- families) {
      get.addFamily(family)
    }
    for ((columnFamily, column) <- columns) {
      get.addColumn(columnFamily, column)
    }

    val fromCache = if (skipCache) None else table.cache.getResult(get)

    fromCache match {
      case Some(result) => Some(result)
      case None => {
        table.withTableOption(tableName) {
          case Some(htable) => {
            val result = htable.get(get)
            if (noneOnEmpty && result.isEmpty) {
              None
            } else {
              val qr = table.buildRow(result)
              if (!skipCache && !result.isEmpty) table.cache.putResult(get, qr, ttl)
              Some(qr)
            }
          }
          case None => None
        }
      }
    }

  }

  def execute(tableName: String = table.tableName, ttl: Int = 30, skipCache: Boolean = true): Seq[RR] = {
    if (keys.isEmpty) return Seq.empty[RR] // no keys..? nothing to see here... move along... move along.

    val results = Buffer[RR]() // buffer for storing all results retrieved

    // if we are utilizing cache, we'll need to be able to recall the `Get' later to use as the cache key
    val getsByKey = if (skipCache) mutable.Map.empty[String, Get] else mutable.Map[String, Get]()

    if (!skipCache) getsByKey.sizeHint(keys.size) // perf optimization

    // buffer for all `Get's that really need to be gotten
    val cacheMisses = Buffer[Get]()

    val gets = buildGetsAndCheckCache(skipCache) {
      case (get: Get, key: Array[Byte]) => if (!skipCache) getsByKey.put(new String(key), get)
    } {
      case (qropt: Option[RR], get: Get) => if (!skipCache) {
        qropt match {
          case Some(result) => results += result // got it! place it in our result buffer
          case None => cacheMisses += get // missed it! place the get in the buffer
        }
      }
    }

    // identify what still needs to be `Get'ed ;-}
    val hbaseGets = if (skipCache) gets else cacheMisses

    if (!hbaseGets.isEmpty) {
      // only do this if we have something to do
      table.withTable(tableName) {
        htable =>
          htable.get(hbaseGets).foreach(res => {
            if (res != null && !res.isEmpty) {
              // ignore empty results
              val qr = table.buildRow(res) // construct query result

              // now is where we need to retrive the 'get' used for this result so that we can
              // pass this 'get' as the key for our local cache
              if (!skipCache) table.cache.putResult(getsByKey(new String(res.getRow)), qr, ttl)
              results += qr // place it in our result buffer
            }
          })
      }
    }

    results.toSeq // DONE!
  }

  def executeMap(tableName: String = table.tableName, ttl: Int = 30, skipCache: Boolean = true): Map[R, RR] = {
    if (keys.isEmpty) return Map.empty[R, RR] // don't get all started with nothing to do

    // init our result map and give it a hint of the # of keys we have
    val resultMap = mutable.Map[R, RR]()
    resultMap.sizeHint(keys.size) // perf optimization

    // if we are utilizing cache, we'll need to be able to recall the `Get' later to use as the cache key
    val getsByKey = if (skipCache) mutable.Map.empty[String, Get] else mutable.Map[String, Get]()

    if (!skipCache) getsByKey.sizeHint(keys.size) // perf optimization

    // buffer for all `Get's that really need to be gotten
    val cacheMisses = Buffer[Get]()

    val gets = buildGetsAndCheckCache(skipCache) {
      case (get: Get, key: Array[Byte]) => if (!skipCache) getsByKey.put(new String(key), get)
    } {
      case (qropt: Option[RR], get: Get) => if (!skipCache) {
        qropt match {
          case Some(result) => resultMap.put(result.rowid, result) // got it! place it in our result map
          case None => cacheMisses += get // missed it! place the get in the buffer
        }
      }
    }

    // identify what still needs to be `Get'ed ;-}
    val hbaseGets = if (skipCache) gets else cacheMisses

    if (!hbaseGets.isEmpty) {
      // only do this if we have something to do
      table.withTable(tableName) {
        htable =>
          htable.get(hbaseGets).foreach(res => {
            if (res != null && !res.isEmpty) {
              // ignore empty results
              val qr = table.buildRow(res) // construct query result

              // now is where we need to retrive the 'get' used for this result so that we can
              // pass this 'get' as the key for our local cache
              if (!skipCache) table.cache.putResult(getsByKey(new String(res.getRow)), qr, ttl)
              resultMap(qr.rowid) = qr // place it in our result map
            }
          })
      }
    }

    resultMap // DONE!
  }

  private def buildGetsAndCheckCache(skipCache: Boolean)
                                    (receiveGetAndKey: (Get, Array[Byte]) => Unit = (get, key) => {})
                                    (receiveCachedResult: (Option[RR], Get) => Unit = (qr, get) => {}): Seq[Get] = {
    if (keys.isEmpty) return Seq.empty[Get] // no keys..? nothing to see here... move along... move along.

    val gets = Buffer[Get]() // buffer for the raw `Get's

    for (key <- keys) {
      val get = new Get(key)
      gets += get
      receiveGetAndKey(get, key)
    }

    // since the families and columns will be identical for all `Get's, only build them once
    val firstGet = gets(0)

    // add all families to the first `Get'
    for (family <- families) {
      firstGet.addFamily(family)
    }
    // add all columns to the first `Get'
    for ((columnFamily, column) <- columns) {
      firstGet.addColumn(columnFamily, column)
    }

    var pastFirst = false
    for (get <- gets) {
      if (pastFirst) {
        // we want to skip the first `Get' as it already has families/columns

        // for all subsequent `Get's, we will build their familyMap from the first `Get'
        firstGet.getFamilyMap.foreach((kv: (Array[Byte], NavigableSet[Array[Byte]])) => {
          get.getFamilyMap.put(kv._1, kv._2)
        })
      } else {
        pastFirst = true
      }

      // try the cache with this filled in get
//      if (!skipCache) receiveCachedResult(table.cache.getResult(get), get)
    }

    if(!skipCache) {
      val cacheResults = table.cache.getResults(gets)

      cacheResults.foreach {case (get, rowOpt) =>
        receiveCachedResult(rowOpt,get)
      }
    }


    gets
  }

}
