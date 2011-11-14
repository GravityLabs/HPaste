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

class ScanQuery[T <: HbaseTable[T, R], R](table: HbaseTable[T, R]) {
  val scan = new Scan()
  scan.setCaching(100)
  scan.setMaxVersions(1)

  val filterBuffer = scala.collection.mutable.Buffer[Filter]()

  def executeWithCaching(operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL, ttl: Int = 30): Seq[QueryResult[T, R]] = {
    completeScanner(operator)
    val results = table.cache.getScanResult(scan) match {
      case Some(result) => {
        println("cache hit against key " + scan.toString)
        result
      }
      case None => {
        println("cache miss against key " + scan.toString)
        val results = scala.collection.mutable.Buffer[QueryResult[T, R]]()
        table.withTable() {
          htable =>
            val scanner = htable.getScanner(scan)
            try {
              for (result <- scanner) {
                results += new QueryResult[T, R](result, table, table.tableName)
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

  def execute(handler: (QueryResult[T, R]) => Unit, operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL) {
    table.withTable() {
      htable =>
        completeScanner(operator)
        val scanner = htable.getScanner(scan)

        try {
          for (result <- scanner) {
            handler(new QueryResult[T, R](result, table, table.tableName))
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

  def executeToSeq[I](handler: (QueryResult[T, R]) => I, operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL): Seq[I] = {
    val results = Buffer[I]()

    table.withTable() {
      htable =>
        completeScanner(operator)
        val scanner = htable.getScanner(scan)

        try {
          for (result <- scanner; if (result != null)) {
            results += handler(new QueryResult[T, R](result, table, table.tableName))
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





/**
* Class to be implemented by custom converters
*/


/**
* Simple high performance conversions from complex types to bytes
*/














/**
* When a query comes back, there are a bucket of column families and columns to retrieve.  This class retrieves them.
*/


/**
* A query for setting up a scanner across the whole table or key subsets.
* There is a lot of room for expansion in this class -- caching parameters, scanner specs, key-only, etc.
*/


/**
* An individual data modification operation (put, increment, or delete usually)
* These operations are chained together by the client, and then executed in bulk.
*/




/**
* An increment operation -- can increment multiple columns in a single go.
*/


/**
* A Put operation.  Can work across multiple columns or entire column families treated as Maps.
*/


/**
* A deletion operation.  If nothing is specified but a key, will delete the whole row.  If a family is specified, will just delete the values in
* that family.
*/


/**
* A query for retrieving values.  It works somewhat differently than the data modification operations, in that you do the following:
* 1. Specify one or more keys
* 2. Specify columns and families to scan in for ALL the specified keys
*
* In other words there's no concept of having multiple rows fetched with different columns for each row (that seems to be a rare use-case and
* would make the API very complex).
*/


/**
* Represents the specification of a Column Family
*/


/**
* Represents the specification of a Column.
*/




/**
* Represents a Table.  Expects an instance of HBaseConfiguration to be present.
* A parameter-type T should be the actual table that is implementing this one (this is to allow syntactic sugar for easily specifying columns during
* queries).
* A parameter-type R should be the type of the key for the table.  
*/










