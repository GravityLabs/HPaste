package com.gravity.hbase.exch3

import scala.collection.mutable.HashSet
import scala.collection.mutable.SynchronizedSet

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

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
 * ScanExample
 * ScanCacheBatchExample
 * 
 * NOTE: a running hbase is expected to exist; see book for details
 * 
 * Query2 is HPaste query facility.  It consolidates scan operations
 * and much more.  This example is limited to scanning (and some gyrations),
 * and works (similarly to PutOp) by composing the elements to be scanned,
 * the parameters used by scan for its operations, and then invoking one
 * of three possible scanning styles: scan, scanToIterable, scanUntil.
 * Each of the three possible scanning styles is represented in this
 * example.  Mapping between the Java API and the HPaste API seldom
 * has a 1:1 match:  The examples presented meander in the attempt
 * of providing a general feel of how HPaste likes to be used. 
 * 
 */

object Query2ScanExample extends Schema {
  
  implicit val conf : Configuration  = HBaseConfiguration.create();
  val n4table : String = "simpleScanTable"
  val n4fam1 : String = "fam1"
  val n4fam2 : String = "fam2"
    
  // the following MUST match what names are generated in
  // helper.fillTable(n4table, 1, 5, 5, n4fam1, n4fam2)
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
  
  private val helper : HBaseHelper = HBaseHelper.getHelper(Query2ScanExample.conf);

  def main(args:Array[String]) {
    
    // in case you need it for troubleshooting or so,
    // this is how you can leak the underlying htable
    // val htable = SimpleTable.getTable(SimpleTable)

    // HOUSEKEEPING
    
    // register with the schema
    val SimpleTable = table(new SimpleHbaseTable(n4table))
    
    // prime hbase (drop and create) using the helper from the book
    Query2ScanExample.tables.foreach {
      table => {
        helper.dropTable(table.tableName)
        val famNames = table.familyBytes.toArray.map(new String(_))
        helper.createTable(table.tableName, famNames:_*)
      }
    }
    
    // FILL TABLE FOR LATER SCAN
    
    System.out.println("Adding rows to table in bulk...");
    helper.fillTable(n4table, 1, 5, 5, n4fam1, n4fam2);
    
    // REGULAR EXECUTION    
        
    //
    // HPaste scan(rowHandler) 
    // ===================================
    // with scan, all the interesting work
    // must happen in the row handler, which
    // may optionally accumulate results.
    // All this happens while the scanner is
    // open, so better make sure the scanner
    // lease lasts longer than the time used
    // to carry out the handler's work.
    // The setup is good for a little 
    // processing on a small part of very
    // large rows.  If the rows are truly
    // big, it may be a good idea to reduce
    // scanner RPC caching size (which will 
    // in turn reduce the size of each single
    // network blob transfer) and reduce batch
    // size appropriately (which will increase
    // the number of RPC issued...)
    //
    
    val stringResultAcc = new StringBuilder()
    val rowToken = "scanned >> "
    	
    def rowHandler (resAcc: StringBuilder) (wholeRow: SimpleHbaseTableRow) = {  
      resAcc.append(rowToken+wholeRow.dataHint)
      val fam1map = wholeRow.family(_.fam1)
      val fam2map = wholeRow.family(_.fam2)
      fam1map.foreach{ case(k,v) => resAcc.append(" "+n4fam1+":"+k+"="+v+"; ") }
      fam2map.foreach{ case(k,v) => resAcc.append(" "+n4fam2+":"+k+"="+v+"; ") }
    }
    
    // stringResultAcc is shared among all
    // invocations of sharedRowResultAcc
    def sharedRowResultAcc = rowHandler(stringResultAcc) _
    
    // the simplest possible scan
    
    SimpleTable.query2
        .withAllColumns
        .scan(sharedRowResultAcc)
        
    val resByCol = stringResultAcc.toString.split(rowToken)
    for( resItem <- resByCol ) { println (resItem) }
    
    println

    // look what happens changing batch size!
    // And use a smaller RPC scan cache this time
    
    stringResultAcc.clear
    
    SimpleTable.query2
        .withAllColumns
        .withBatchSize(1)
        .scan(sharedRowResultAcc, cacheSize = 5)
        
    val resByRow = stringResultAcc.toString.split(rowToken)
    for( resItem <- resByRow ) { println (resItem) }
    
    println

    //
    // HPaste scanToIterable(rowHandler) 
    // ===================================
    // with scanToIterable, interesting work
    // may happen in the row handler; however
    // all rows are also returned for further
    // perusal or processing.  The rows
    // are returned after the scanner has
    // been closed, and exists independently
    // of the scanner: no more lease issues.
    // The setup is good if rows are relatively
    // small and can be held in a local buffer
    // with ease.  BTW, in HPaste scanner RPC 
    // caching is enabled by default--which may 
    // be relevant in such a case.  (See for
    // instance makeScanner in Query2.scala)
    //
    	
    def identityHandler(wholeRow: SimpleHbaseTableRow) : SimpleHbaseTableRow = {
      wholeRow
    }
    
    // the simplest possible scanToIterable
    
    val scanIter1 = SimpleTable.query2
        .withAllColumns
    	.scanToIterable(identityHandler)

    scanIter1.foreach(println)
    
    println
    
    // change batch size
   	
    val scanIter2 = SimpleTable.query2
        .withAllColumns
        .withBatchSize(1)
    	// idiomatic (same result)
        .scanToIterable(row=>row)

    scanIter2.foreach(println)
    
    println
    
    // only one family
   	
    val scanIter3 = SimpleTable.query2
        .withAllColumns
        .withFamilies(_.fam1)
        .withBatchSize(1)
        .scanToIterable(row=>row)

    scanIter3.foreach(println)
    
    println
    
    // only one column
   	
    val scanIter4 = SimpleTable.query2
        .withColumn(_.cf1cn1)
        .withBatchSize(1)
        .scanToIterable(row=>row)

    scanIter4.foreach(println)
    
    println
    
    // a few columns
   	
    val scanIter5 = SimpleTable.query2
        .withColumns(_.cf1cn1, _.cf1cn2, _.cf2cn3)
        .withBatchSize(1)
        .scanToIterable(row=>row)

    scanIter5.foreach(println)
    
    println
    
    // a few columns (alternate API) between start and end rows
   	
    val scanIter6 = SimpleTable.query2
        .withColumnsInFamily(_.fam1, n4col2, n4col3)
        .withStartRow(n4row2)
        .withEndRow(n4row4)
        .withBatchSize(1)
        .scanToIterable(row=>row)

    scanIter6.foreach(println)
    
    println
    
    //
    // scanUntil: conditional stop
    // ===================================
    // as in scan(rowHandler), all interesting
    // work happens in the row handler.
    // Differently from scan, if the handler 
    // returns false then scanning will stop
    // then and there.  Conversely, the handler 
    // must return true for scan to progress.
    // 

    
    def predicateHandler(wholeRow: SimpleHbaseTableRow) : Boolean = {
      // stop scanning the first time you see row 4
      if (n4row4.equals (wholeRow.result.getRow[String])) false 
      else {
        println ("[conditional] "+wholeRow)
        true
      }
    }
    
    SimpleTable.query2
        .withStartRow(n4row2)
        .withAllColumns
    	.scanUntil(predicateHandler, cacheSize = 12)
    
    println
    
  }
}
