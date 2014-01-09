package com.gravity.hbase.exch3

import scala.collection.mutable.HashSet
import scala.collection.mutable.SynchronizedSet

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException

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
 * PutExample
 * PutWriteBufferExample
 * PutListExample
 * PutListErrorExample1
 * PutListErrorExample2
 * 
 * NOTE: a running hbase is expected to exist; see book for details
 * 
 * Because of how PutOp manages its operation, this is equivalent to using 
 * (or not using) the client-side write buffer or a list of puts.  This 
 * example encompasses Put, PutWriteBuffer, PutList as the PutOp mechanism
 * is intrinsically poly-functional as implemented in the HPaste code.
 * execute() in OpBase takes care of the details of making it happen as
 * intended.  Depending on where and when execute() is called, put()
 * will be (or will not be) automatically buffered client-side.
 * This example includes also work-alike code for PutError1 and PutError2;
 * in the case of PutError2 (empty put) the behavior is tamer in HPaste
 * and transparent to handle.  PutError1 (bad family) is equivalent.
 * 
 */

object PutOpExample extends Schema {
  
  implicit val conf : Configuration  = HBaseConfiguration.create();

  class SimpleHbaseTable (val tName : String) extends HbaseTable[SimpleHbaseTable,String,SimpleHbaseTableRow] (tName,rowKeyClass=classOf[String], tableConfig = HbaseTableConfig(maxFileSizeInBytes=1073741824)) {
    
    def rowBuilder(result:DeserializedResult) = new SimpleHbaseTableRow(this,result)

    // Column family definition, with 2 columns
    val fam1 = family[String, String, Any]("fam1")
    // assume a column title whose value is a string; e.g. col is fam1:colName1
    val cf1cn1 = column(fam1, "colName1", classOf[String])
    // assume another column whose value is a string
    val cf1cn2 = column(fam1, "colName2", classOf[String])

    // Another column family, with 1 column
    val fam2 = family[String, String, Long]("fam2")
    // assume a column title whose value is a string
    val cf2cn1 = column(fam2, "colName1", classOf[String])

  }

  class SimpleHbaseTableRow(table:SimpleHbaseTable,result:DeserializedResult) extends HRow[SimpleHbaseTable,String](result,table)
  
  private val helper : HBaseHelper = HBaseHelper.getHelper(PutOpExample.conf);

  def main(args:Array[String]) {
    
    // HOUSEKEEPING
    
    // register with the schema
    val SimpleTable = table(new SimpleHbaseTable("simplePutTable"))
    
    // prime hbase (drop and create) using the helper from the book
    PutOpExample.tables.foreach {
      table => {
        helper.dropTable(table.tableName)
        val famNames = table.familyBytes.toArray.map(new String(_))
        helper.createTable(table.tableName, famNames:_*)
      }
    }
    
    // REGULAR EXECUTION

    // this is not buffered; use
    // scan 'simpleTable' , { VERSIONS => (an adequate value, 4-5 OK) }
    // to view in $./hbase shell since
    // values will be shadowed by
    // the buffered example following
    
    SimpleTable.put("myFirstRow")
        .value(_.cf1cn1,"f1colName1Val1")
        .value(_.cf1cn2,"f1colName2Val1")
        .execute()
        
    // this is buffered in PutOp.scala
    
    SimpleTable.put("myFirstRow")
        .value(_.cf1cn1,"f1colName1Val1b")
        .value(_.cf1cn2,"f1colName2Val1b")
        .value(_.cf2cn1,"f2colName1ValAb")
        .put("myNextRow")
        .value(_.cf1cn2,"f1colName2ValAb")
        .value(_.cf2cn1,"f2colName1ValBb")
        .execute()
        
    // An empty put is simply ignored
    // rather than creating an error;
    // no error handling necessary here...
    
    SimpleTable.put("myEmptyRow") // this empty put is ignored (will not appear in permanent storage)
        .put("myNextRow") // *this* put will appear in permanent storage notwithstanding the error
        .value(_.cf1cn2,"f1colName2ValAfterEmpty")
        .value(_.cf2cn1,"f2colName1ValAfterEmpty")
        .execute()

    // set breakpoint on next line and 
    // run debugger to understand why/how...
        
    SimpleTable.put("myLoneEmptyRow") // this empty put is ignored (will not appear in permanent storage)
        .execute()
        
        
    // ERROR CONDITION: bad column family
        
    // it takes a bit of effort to
    // create a bogus column family
        
    def bogusFamily[T,R,F,K,V] (arg : T) : Column[SimpleHbaseTable, String, String, String, String] = {
       val bogusFam = SimpleTable.family[String,String,Any]("bogus")
       SimpleTable.column(bogusFam, "(anything)", classOf[String])
    }
    
    // in general, it's not that easy
    // to trigger this unhappy path
    
    try {
      SimpleTable.put("myBogusRow") // this put will not appear in permanent storage
        .value(bogusFamily,"f1colName1Val1")
        .value(bogusFamily,"f1colName2Val1")
        .put("myNextRow") // *this* put will appear in permanent storage notwithstanding the error
        .value(_.cf1cn2,"f1colName2ValAfterBogus")
        .value(_.cf2cn1,"f2colName1ValAfterBogus")
        .execute()
        println("SHOULD NEVER GET HERE! Must throw instead")
    } catch {
      case e:RetriesExhaustedWithDetailsException => {
        println("(expected) properly thrown "+e)
      }
      case f:Throwable => {
        println("(almost expected) wrong exception thrown "+f)
      }
    }
    
  }
}
