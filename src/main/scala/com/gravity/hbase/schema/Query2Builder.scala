package com.gravity.hbase.schema

class Query2Builder[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]] private[schema](override val table: HbaseTable[T, R, RR]) extends BaseQuery[T, R, RR] with MinimumFiltersToExecute[T, R, RR] {

  def withAllColumns = new Query2(table, keys, families, columns, currentFilter, startRowBytes, endRowBytes, batchSize, startTime, endTime)

  override def withFamilies[F](firstFamily: (T) => ColumnFamily[T, R, F, _, _], familyList: ((T) => ColumnFamily[T, R, F, _, _])*) = {
    withAllColumns.withFamilies(firstFamily, familyList: _*)
  }

  override def withColumn[F, K, V](family: (T) => ColumnFamily[T, R, F, K, V], columnName: K) = {
    withAllColumns.withColumn(family, columnName)
  }

  override def withColumn[F, K, V](column: (T) => Column[T, R, F, K, V]) = {
    withAllColumns.withColumn(column)
  }

  override def withColumns[F, K, V](firstColumn: (T) => Column[T, R, F, _, _], columnList: ((T) => Column[T, R, F, _, _])*) = {
    withAllColumns.withColumns(firstColumn, columnList: _*)
  }

}