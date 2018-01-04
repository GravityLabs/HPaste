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
import scala.collection.Map
import org.apache.hadoop.hbase.client.{Get, Scan}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


case class ScanCachePolicy(ttlMinutes: Int)

/**
 * An interface that can be injected into an HbaseTable implementation that supports caching
 * of queries.
 * @tparam T
 * @tparam R
 * @tparam RR
 */
trait QueryResultCache[T <: HbaseTable[T, R, RR], R, RR <: HRow[T,R]] {

  def getScanResult(key: Scan): Option[Seq[RR]]

  def putScanResult(key: Scan, value: Seq[RR], ttl: Int)

  def getKeyFromGet(get:Get) : String

  def getLocalResult(key: String): CacheRequestResult[RR]

  def getLocalResults(keys:Iterable[String]) : Map[String, CacheRequestResult[RR]]

  def getRemoteResult(key: String): CacheRequestResult[RR]

  def getRemoteResults(keys: Iterable[String]) : Map[String, CacheRequestResult[RR]]

  def putResultLocal(key: String, value: Option[RR], ttl: Int)

  def putResultRemote(key: String, value: Option[RR], ttl: Int)

  def putResultsRemote(keysToValues: Map[String, Option[RR]], ttl: Int)

  def instrumentRequest(requestSize: Int, localHits: Int, localMisses: Int, remoteHits: Int, remoteMisses: Int)
}

/**
 * The default implementation of QueryResultCache.  Will do nothing.
 * @tparam T
 * @tparam R
 * @tparam RR
 */
class NoOpCache[T <: HbaseTable[T, R,RR], R, RR <: HRow[T,R]] extends QueryResultCache[T, R, RR] {

  def getScanResult(key: Scan): Option[Seq[RR]] = None

  def putScanResult(key: Scan, value: Seq[RR], ttl: Int) {}

  def getKeyFromGet(get:Get) : String = ""

  def getLocalResult(key: String): CacheRequestResult[RR] = NotFound

  def getLocalResults(keys:Iterable[String]) : Map[String, CacheRequestResult[RR]] = Map.empty[String, CacheRequestResult[RR]]

  def getRemoteResult(key: String): CacheRequestResult[RR] = NotFound

  def getRemoteResults(keys: Iterable[String]) : Map[String, CacheRequestResult[RR]] = Map.empty[String, CacheRequestResult[RR]]

  def putResultLocal(key: String, value: Option[RR], ttl: Int) {}

  def putResultRemote(key: String, value: Option[RR], ttl: Int) {}

  def putResultsRemote(keysToValues: Map[String, Option[RR]], ttl: Int) {}

  def instrumentRequest(requestSize: Int, localHits: Int, localMisses: Int, remoteHits: Int, remoteMisses: Int) {}
}

private[schema] class TestCache[T <: HbaseTable[T, R,RR], R, RR <: HRow[T,R]] extends QueryResultCache[T, R, RR] {
  private val local = new java.util.concurrent.ConcurrentHashMap[String, Option[RR]]()
  private val remote = new java.util.concurrent.ConcurrentHashMap[String, Option[RR]]()
  def getScanResult(key: Scan): Option[Seq[RR]] = None

  def putScanResult(key: Scan, value: Seq[RR], ttl: Int) {}

  def getKeyFromGet(get:Get) : String = java.util.Arrays.hashCode(get.getRow).toString()

  private def getResultFrom(cache: java.util.concurrent.ConcurrentHashMap[String, Option[RR]], key: String) = {
    try {
      cache.get(key) match {
        case null => NotFound
        case Some(value) => Found(value)
        case None => FoundEmpty
      }
    }
    catch {
      case e:Exception => Error("Error retrieving " + key, Some(e))
    }
  }

  private def getResultsFrom(cache: java.util.concurrent.ConcurrentHashMap[String, Option[RR]], keys: Iterable[String]) = {
    val results = for(key <- keys) yield key -> getResultFrom(cache, key)
    results.toMap
  }

  def getLocalResult(key: String): CacheRequestResult[RR] = {
    getResultFrom(local, key)
  }

  def getLocalResults(keys:Iterable[String]) : Map[String, CacheRequestResult[RR]] = {
    getResultsFrom(local, keys)
  }

  def getRemoteResult(key: String): CacheRequestResult[RR] = {
    getResultFrom(remote, key)
  }

  def getRemoteResults(keys: Iterable[String]) : Map[String, CacheRequestResult[RR]] = {
    getResultsFrom(remote, keys)
  }

  def putResultLocal(key: String, value: Option[RR], ttl: Int) {
    local.put(key, value)
  }

  def putResultRemote(key: String, value: Option[RR], ttl: Int) {
    remote.put(key, value)
  }

  def putResultsRemote(keysToValues: Map[String, Option[RR]], ttl: Int) {
    keysToValues.map { kv =>
      remote.put(kv._1, kv._2)
    }
  }

  def instrumentRequest(requestSize: Int, localHits: Int, localMisses: Int, remoteHits: Int, remoteMisses: Int) {
    println("instrumented " + requestSize + " requests, " + localHits + " local hits, " + localMisses + " localMisses, " + remoteHits + " remoteHits, and " + remoteMisses + " remoteMisses")
  }
}