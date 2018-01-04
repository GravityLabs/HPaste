package com.gravity.hbase.schema

import java.util

import gnu.trove.map.TObjectLongMap
import org.joda.time.DateTime
import scala.collection.mutable.Buffer

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * The container for the result values deserialized from Hbase.
 * @param rowid
 * @param famCount
 */
case class DeserializedResult(rowid: AnyRef, famCount: Int) {

  def isEmpty: Boolean = values.size == 0

  def isEmptyRow: Boolean = ! values.exists(family=>family != null && family.size() > 0)

  def getRow[R](): R = rowid.asInstanceOf[R]


  def familyValueMap[K, V](fam: ColumnFamily[_, _, _, _, _]): util.Map[K, V] = {
    val famMap = family(fam)
    if (famMap != null) {
      famMap.asInstanceOf[java.util.Map[K, V]]
    } else {
      new gnu.trove.map.hash.THashMap[K, V]()
    }
  }

  def familyKeySet[K](fam: ColumnFamily[_, _, _, _, _]): util.Set[K] = {
    val famMap = family(fam)
    if (famMap != null) {
      famMap.keySet.asInstanceOf[java.util.Set[K]]
    } else {
      new gnu.trove.set.hash.THashSet[K]()
    }
  }

  def family(family: ColumnFamily[_, _, _, _, _]): util.Map[AnyRef, AnyRef] = {
    values(family.index)
  }

  def familyOf(column: Column[_, _, _, _, _]): util.Map[AnyRef, AnyRef] = family(column.family)

  def familyMap(fam: ColumnFamily[_, _, _, _, _]): util.Map[AnyRef, AnyRef] = family(fam)

  def hasColumn(column: Column[_, _, _, _, _]): Boolean = {
    val valueMap = familyOf(column)
    if (valueMap == null || valueMap.size == 0) false else true
  }

  def columnValue(fam: ColumnFamily[_, _, _, _, _], columnName: AnyRef): AnyRef = {
    val valueMap = family(fam)
    if (valueMap == null) {
      null
    } else {
      valueMap.get(columnName)
    }
  }

  def columnTimestamp(fam: ColumnFamily[_, _, _, _, _], columnName: AnyRef): Long = {
    val res = timestampLookaside(fam.index)
    if (res != null) {
      val colRes = res.get(columnName)
      colRes
    }
    else {
      0l
    }
  }


  def columnTimestampAsDate(column: Column[_, _, _, _, _]): DateTime = {
    val cts = columnTimestamp(column.family, column.columnNameRef)
    if (cts > 0) {
      new DateTime(cts)
    } else {
      null
    }
  }

  def columnTimestampByName(fam: ColumnFamily[_, _, _, _, _], columnName: AnyRef): Long = {
    val cts = columnTimestamp(fam, columnName)
    cts
  }

  def columnTimestampByNameAsDate(fam: ColumnFamily[_, _, _, _, _], columnName: AnyRef): DateTime = {
    val cts = columnTimestamp(fam, columnName)
    if (cts > 0) {
      new DateTime(cts)
    }
    else {
      null
    }
  }


  def columnValueSpecific(column: Column[_, _, _, _, _]): AnyRef = {
    columnValue(column.family, column.columnNameRef)
  }


 var values: Array[util.Map[AnyRef, AnyRef]] = new Array[java.util.Map[AnyRef, AnyRef]](famCount)

  private val timestampLookaside: Array[TObjectLongMap[AnyRef]] = new Array[gnu.trove.map.TObjectLongMap[AnyRef]](famCount)



  /**This is a map whose key is the family type, and whose values are maps of column keys to columnvalues paired with their timestamps */
  //  val values = new java.util.HashMap[ColumnFamily[_, _, _, _, _], java.util.HashMap[AnyRef, AnyRef]]()

  //  val timestampLookaside = new java.util.HashMap[ColumnFamily[_, _, _, _, _], java.util.HashMap[AnyRef, Long]]()

  def add(family: ColumnFamily[_, _, _, _, _], qualifier: AnyRef, value: AnyRef, timeStamp: Long) {
    var map = values(family.index)
    if (map == null) {
      map = new gnu.trove.map.hash.THashMap[AnyRef, AnyRef]()
      values(family.index) = map
    }
    map.put(qualifier, value)

    var tsMap = timestampLookaside(family.index)
    if (tsMap == null) {
      tsMap = new gnu.trove.map.hash.TObjectLongHashMap[AnyRef]()
      timestampLookaside(family.index) = tsMap
    }
    tsMap.put(qualifier, timeStamp)
    //Add timestamp lookaside
  }

  var errorBuffer: Buffer[(Array[Byte], Array[Byte], Array[Byte], Long)] = _

  def addErrorBuffer(family: Array[Byte], qualifier: Array[Byte], value: Array[Byte], timestamp: Long) {
    if (errorBuffer == null) {
      errorBuffer = Buffer()
    }
    errorBuffer.append((family, qualifier, value, timestamp))
  }

  def hasErrors: Boolean = (errorBuffer != null)
}
