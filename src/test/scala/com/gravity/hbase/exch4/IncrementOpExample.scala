package com.gravity.hbase.exch4

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
 * IncrementSingleExample
 * IncrementMultipleExample
 * 
 * NOTE: a running hbase is expected to exist; see book for details
 * 
 * Simple introduction to incrementing counters with HPaste.
 * 
 *   
 */

object IncrementOpExample extends Schema {
  
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
  
  private val helper : HBaseHelper = HBaseHelper.getHelper(IncrementOpExample.conf);

  def main(args:Array[String]) {
    
    // HOUSEKEEPING
    
    // register with the schema
    val SimpleTable = table(new SimpleHbaseTable("simpleIncrementTable"))
    
    // prime hbase (drop and create) using the helper from the book
    IncrementOpExample.tables.foreach {
      table => {
        helper.dropTable(table.tableName)
        val famNames = table.familyBytes.toArray.map(new String(_))
        helper.createTable(table.tableName, famNames:_*)
      }
    }
    
    // REGULAR EXECUTION

    // this is not buffered
    
    SimpleTable.increment("myFirstRow")
        .value(_.cf1cn1,1)
        .value(_.cf1cn2,100)
        .execute()
        
    // this is buffered in IncrementOp.scala
    
    SimpleTable.increment("myFirstRow")
        .value(_.cf1cn1,1)
        .value(_.cf1cn2,100)
        .value(_.cf2cn1,2)
        .increment("myNextRow")
        .value(_.cf1cn2,10)
        .value(_.cf2cn1,7)
        .execute()
        
    // An empty increment is simply ignored
    
    SimpleTable.increment("myEmptyRow") // this empty increment is ignored (will not appear in permanent storage)
        .increment("myNextRow") // this increment will also *not* appear in permanent storage! (Differs from put)
        .value(_.cf1cn2,10)
        .value(_.cf2cn1,7)
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
      SimpleTable.increment("myBogusRow") // this increment will not appear in permanent storage
        .value(bogusFamily,13)
        .value(bogusFamily,17)
        .increment("myNextRow") // this increment will also *not* appear in permanent storage! (Differs from put)
        .value(_.cf1cn2,10)
        .value(_.cf2cn1,7)
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
