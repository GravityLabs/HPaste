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

class Query2Builder[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]] private[schema](override val table: HbaseTable[T, R, RR]) extends BaseQuery[T, R, RR] with MinimumFiltersToExecute[T, R, RR] {

  def toQuery2 = new Query2(table, keys, families, columns, currentFilter, startRowBytes, endRowBytes, batchSize, startTime, endTime)

  def withAllColumns = toQuery2

  override def withFamilies[F](firstFamily: (T) => ColumnFamily[T, R, F, _, _], familyList: ((T) => ColumnFamily[T, R, F, _, _])*) = {
    for (family <- firstFamily +: familyList) {
      val fam = family(table.pops)
      families += fam.familyBytes
    }
    toQuery2
  }

  override def withColumnsInFamily[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], firstColumn: K, columnList: K*) = {
    val fam = family(table.pops)
    for (column <- firstColumn +: columnList) {
      columns += (fam.familyBytes -> fam.keyConverter.toBytes(column))
    }
    toQuery2
  }

  override def withColumn[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], columnName: K) = {
    val fam = family(table.pops)
    columns += (fam.familyBytes -> fam.keyConverter.toBytes(columnName))
    toQuery2
  }

  override def withColumn[F, K, V](column: (T) => Column[T, R, F, K, V]) = {
    val col = column(table.pops)
    columns += (col.familyBytes -> col.columnBytes)
    toQuery2
  }

  override def withColumns[F, K, V](firstColumn: (T) => Column[T, R, F, _, _], columnList: ((T) => Column[T, R, F, _, _])*) = {
    for (column <- firstColumn +: columnList) {
      val col = column(table.pops)
      columns += (col.familyBytes -> col.columnBytes)
    }
    toQuery2
  }

}