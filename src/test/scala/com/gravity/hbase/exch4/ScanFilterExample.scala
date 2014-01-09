package com.gravity.hbase.exch4

import scala.collection.mutable.HashSet
import scala.collection.mutable.SynchronizedSet

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.BinaryComparator;

import com.gravity.hbase.schema.HbaseTable
import com.gravity.hbase.schema.HRow
import com.gravity.hbase.schema.Column
import com.gravity.hbase.schema.ColumnFamily
import com.gravity.hbase.schema.Schema
import com.gravity.hbase.schema.HbaseTableConfig
import com.gravity.hbase.schema.DeserializedResult

import util.HBaseHelper;


/**
 * "HBase: the definitive guide" examples translated to HPaste in this file
 * 
 * RowFilterExample
 * FamilyFilterExample
 * QualifierFilterExample
 * ValueFilterExample
 * DependentColumnFilterExample
 * SingleColumnValueFilterExample
 * PrefixFilterExample
 * ColumnPaginationFilterExample
 * 
 * NOTE: a running hbase is expected to exist; see book for details
 * 
 * Query2 is HPaste query facility.  It consolidates scan operations
 * and much more.  This example is limited to scanning with filters; in
 * particular, it covers how to use vanilla Filter(s) with HPaste, as
 * opposed to taking advantage of the richer filtering API that HPaste
 * provides. Scanning with filters is similar to scanning, and available
 * for all three possible scanning styles: scan, scanToIterable, scanUntil.
 * Two of the three possible scanning styles are represented in this
 * example. 
 * 
 */

object ScanFilterExample extends Schema {
  
  implicit val conf : Configuration  = HBaseConfiguration.create();
  val n4table : String = "simpleScanFilterTable"
  val n4fam1 : String = "fam1"
  val n4fam2 : String = "fam2"
    
  // the following MUST match what names are generated in
  // helper.fillTable(n4table, 1, 10, 5, n4fam1, n4fam2)
  // which is used later in this example for bulk fill.
  //
  // If not, def buildRow(result: Result): RR in 
  // HbaseTable.scala will operate incorrectly, resulting
  // in a scan retrieval that appears empty-ish, sometimes
  // in a bizarre way (e.g. families yet no cols retrieved)

  val n4col1 : String = "col-0"
  val n4col2 : String = "col-1"
  val n4col3 : String = "col-2"
  val n4col4 : String = "col-3"
  val n4col5 : String = "col-4"
  val n4row1 : String = "row-1"
  val n4row2 : String = "row-2"
  val n4row3 : String = "row-3"
  val n4row4 : String = "row-4"
  val n4row5 : String = "row-5"
  val n4row6 : String = "row-7"
  val n4row7 : String = "row-7"
  val n4row8 : String = "row-8"
  val n4row9 : String = "row-9"
  val n4row10 : String = "row-10"
    
  class SimpleHbaseTable (val tName : String) extends HbaseTable[SimpleHbaseTable,String,SimpleHbaseTableRow] (tName,rowKeyClass=classOf[String], tableConfig = HbaseTableConfig(maxFileSizeInBytes=1073741824)) {
    
    def rowBuilder(result:DeserializedResult) = new SimpleHbaseTableRow(this,result)

    // Column family definition, with 2 columns
    val fam1 = family[String, String, Any](n4fam1)
    // assume a column title whose value is a string; e.g. col is fam1:colName1
    val cf1cn1 = column(fam1, n4col1, classOf[String])
    val cf1cn2 = column(fam1, n4col2, classOf[String])
    val cf1cn3 = column(fam1, n4col3, classOf[String])
    val cf1cn4 = column(fam1, n4col4, classOf[String])
    val cf1cn5 = column(fam1, n4col5, classOf[String])

    // Another column family, with 1 column
    val fam2 = family[String, String, Any](n4fam2)
    // assume a column title whose value is a string
    val cf2cn1 = column(fam2, n4col1, classOf[String])
    val cf2cn2 = column(fam2, n4col2, classOf[String])
    val cf2cn3 = column(fam2, n4col3, classOf[String])
    val cf2cn4 = column(fam2, n4col4, classOf[String])
    val cf2cn5 = column(fam2, n4col5, classOf[String])

  }

  class SimpleHbaseTableRow(table:SimpleHbaseTable,result:DeserializedResult) extends HRow[SimpleHbaseTable,String](result,table) {
    def dataHint : String = {
      if (result.hasErrors) "errors found"
      else if (result.isEmpty) "nothing found"
      else {
    	  "[found] key:"+result.getRow[String]+ (if (result.isEmptyRow) " (empty row)" else "")
      }
    }
    override def toString : String = {
      val sb = new StringBuilder()
      val fam1map = family(_.fam1)
      val fam2map = family(_.fam2)
      val rowHint = result.getRow[String];
      fam1map.foreach{ case(k,v) => sb.append(rowHint+" "+n4fam1+":"+k+"="+v+"; ") }
      fam2map.foreach{ case(k,v) => sb.append(rowHint+" "+n4fam2+":"+k+"="+v+"; ") }
      sb.toString
    }
  }
  
  private val helper : HBaseHelper = HBaseHelper.getHelper(ScanFilterExample.conf);

  def main(args:Array[String]) {
    
    // in case you need it for troubleshooting or so,
    // this is how you can leak the underlying htable
    // val htable = SimpleTable.getTable(SimpleTable)

    // HOUSEKEEPING
    
    // register with the schema
    val SimpleTable = table(new SimpleHbaseTable(n4table))
    
    // prime hbase (drop and create) using the helper from the book
    ScanFilterExample.tables.foreach {
      table => {
        helper.dropTable(table.tableName)
        val famNames = table.familyBytes.toArray.map(new String(_))
        helper.createTable(table.tableName, famNames:_*)
      }
    }
    
    // FILL TABLE FOR LATER SCAN
    
    System.out.println("Adding rows to table in bulk...");
    helper.fillTable(n4table, 1, 10, 5, n4fam1, n4fam2);
    
    // REGULAR EXECUTION    
            
    // HPaste scan(rowHandler) 
    // ===================================
    
    val stringResultAcc = new StringBuilder()
    val rowToken = "scanned >> "
    	
    def rowHandler (resAcc: StringBuilder) (wholeRow: SimpleHbaseTableRow) = {  
      resAcc.append(rowToken+wholeRow.dataHint)
      val fam1map = wholeRow.family(_.fam1)
      val fam2map = wholeRow.family(_.fam2)
      fam1map.foreach{ case(k,v) => resAcc.append(" "+n4fam1+":"+k+"="+v+"; ") }
      fam2map.foreach{ case(k,v) => resAcc.append(" "+n4fam2+":"+k+"="+v+"; ") }
    }
    
    def sharedRowResultAcc = rowHandler(stringResultAcc) _
    
    // use a filter to select specific rows
    
    val yield_LE_n4row3 : Filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(n4row3)));
    
    SimpleTable.query2
        .withAllColumns
        .withOrFilters(yield_LE_n4row3)
        .scan(sharedRowResultAcc)
        
    val res1 = stringResultAcc.toString.split(rowToken)
    for( resItem <- res1 ) { println (resItem) }
    
    println

    // use multiple AND filters to select specific rows
    
    stringResultAcc.clear

    val yield_GT_n4row1 : Filter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(n4row1)));
    
    SimpleTable.query2
        .withAllColumns
        .withAndFilters(yield_GT_n4row1,yield_LE_n4row3)
        .scan(sharedRowResultAcc)
        
    val res2 = stringResultAcc.toString.split(rowToken)
    for( resItem <- res2 ) { println (resItem) }
    
    println
    
    // HPaste scanToIterable(rowHandler) 
    // ===================================
        
    // use a filter to select specific rows

    val yield_rowRegex : Filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*-3"));

    val scanIter1 = SimpleTable.query2
        .withAllColumns
        .withOrFilters(yield_rowRegex)
    	.scanToIterable(row=>row)

    scanIter1.foreach(println)
    
    println
    
    // use multiple OR filters to select specific rows
    
    val yield_rowSubstring = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("-2"));
        
    val scanIter2 = SimpleTable.query2
        .withAllColumns
        .withOrFilters(List(yield_rowRegex,yield_rowSubstring) : _*)
    	.scanToIterable(row=>row)

    scanIter2.foreach(println)
    
    println
    
    // use AND filter and limit result to selected family
    
    val yield_fam2only : Filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(n4fam2)));
    val andFilters1 = List(yield_GT_n4row1,yield_LE_n4row3,yield_fam2only)
    
    val scanIter3 = SimpleTable.query2
        .withAllColumns
        .withAndFilters(andFilters1 : _*)
    	.scanToIterable(row=>row)

    scanIter3.foreach(println)
    
    println
    
    // further limit result to selected column names

    val yield_LE_col3only : Filter = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes(n4col3)));
    
    val scanIter4 = SimpleTable.query2
        .withAllColumns
        .withAndFilters((yield_LE_col3only :: andFilters1) : _*)
    	.scanToIterable(row=>row)

    scanIter4.foreach(println)
    
    println
    
    // further limit results to selected values
    
    val yield_valContainsSub : Filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(".0"));

    val scanIter5 = SimpleTable.query2
        .withAllColumns
        .withAndFilters((yield_valContainsSub :: yield_LE_col3only :: andFilters1) : _*)
    	.scanToIterable(row=>row)

    scanIter5.foreach(println)
    
    println
    
    // use dependent columns
    
    val isDropRefCol = false;
    val yield_withDc1 : Filter = new DependentColumnFilter(Bytes.toBytes(n4fam1),  Bytes.toBytes(n4col3), 
        isDropRefCol, CompareFilter.CompareOp.EQUAL,new SubstringComparator("-2."));

    val scanIter6 = SimpleTable.query2
        .withAllColumns
        .withOrFilters(yield_withDc1)
    	.scanToIterable(row=>row)

    scanIter6.foreach(println)
    
    println
    
    // row selection by single column value
    
    val yield_byColVal : Filter = new SingleColumnValueFilter(
      Bytes.toBytes(n4fam2),
      Bytes.toBytes(n4col4),
      CompareFilter.CompareOp.EQUAL,
      new SubstringComparator("-3.3"));

    val scanIter7 = SimpleTable.query2
        .withAllColumns
        .withOrFilters(yield_byColVal)
    	.scanToIterable(row=>row)

    scanIter7.foreach(println)
    
    println
    
    // row selection by key prefix
    
    val yield_byPrefix : Filter = new PrefixFilter(Bytes.toBytes(n4row1))

    val scanIter8 = SimpleTable.query2
        .withAllColumns
        .withOrFilters(yield_byPrefix)
    	.scanToIterable(row=>row)

    scanIter8.foreach(println)
        
    println
    
    // column pagination filter
    
    val colOffset = 1
    val colPageSize = 2
    val yield_columnPagination : Filter = new ColumnPaginationFilter(colPageSize, colOffset);

    val scanIter9 = SimpleTable.query2
        .withAllColumns
        .withOrFilters(yield_columnPagination)
    	.scanToIterable(row=>row)

    scanIter9.foreach(println)
        
    println
    
  }
}
