package com.gravity.hbase.schema

import org.apache.hadoop.hbase.client.{Get, Scan}

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


case class ScanCachePolicy(ttlMinutes: Int)

trait QueryResultCache[T <: HbaseTable[T, R], R] {

  def getScanResult(key: Scan): Option[Seq[QueryResult[T, R]]]

  def putScanResult(key: Scan, value: Seq[QueryResult[T, R]], ttl: Int)

  def getResult(key: Get): Option[QueryResult[T, R]]

  def putResult(key: Get, value: QueryResult[T, R], ttl: Int)
}

class NoOpCache[T <: HbaseTable[T, R], R] extends QueryResultCache[T, R] {

  override def getScanResult(key: Scan): Option[Seq[QueryResult[T, R]]] = None

  override def putScanResult(key: Scan, value: Seq[QueryResult[T, R]], ttl: Int) {}

  override def getResult(key: Get): Option[QueryResult[T, R]] = None


  override def putResult(key: Get, value: QueryResult[T, R], ttl: Int) {}
}
