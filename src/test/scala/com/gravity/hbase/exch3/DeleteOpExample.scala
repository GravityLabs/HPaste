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
 * DeleteExample
 * DeleteListExample
 * DeleteListErrorExample
 * 
 * NOTE: a running hbase is expected to exist; see book for details
 * 
 * HPaste DeleteOp is not as articulated as the HBase API when it comes
 * to delete operations, to the extent that HPaste does not have a
 * call equivalent to deleteColumn, nor it allows as many overloaded
 * parameter lists for the other operations.  Its invocation is
 * however similar to that of put.  Delete is built from its components,
 * (key, column family, column qualifier) and assembled incrementally
 * until execute() is invoked.  At that point, and only at that point,
 * delete fires.  This behavior encompasses in one unified model both
 * individual delete and delete lists.  Error handling is similar in
 * much of its outcome to the original API, except retrieving the failed
 * delete actions is not as straightforward.  (Execution happens
 * via Object[] batch(final List<? extends Row> actions) instead of 
 * void delete(List<Delete> deletes) in HTableInterface.)
 *  
 */

object DeleteOpExample extends Schema {
  
  implicit val conf : Configuration  = HBaseConfiguration.create();
  val n4table : String = "simpleDeleteTable"
  val n4fam1 : String = "fam1"
  val n4fam2 : String = "fam2"
  val n4col1 : String = "colName1"
  val n4col2 : String = "colName2"
  val n4col3 : String = "colName3"
  val n4row1 : String = "rowName1"
  val n4row2 : String = "rowName2"
  val n4row3 : String = "rowName3"
    
  class SimpleHbaseTable (val tName : String) extends HbaseTable[SimpleHbaseTable,String,SimpleHbaseTableRow] (tName,rowKeyClass=classOf[String], tableConfig = HbaseTableConfig(maxFileSizeInBytes=1073741824)) {
    
    def rowBuilder(result:DeserializedResult) = new SimpleHbaseTableRow(this,result)

    // Column family definition, with 2 columns
    val fam1 = family[String, String, Any](DeleteOpExample.n4fam1)
    // assume a column title whose value is a string; e.g. col is fam1:colName1
    val cf1cn1 = column(fam1, n4col1, classOf[String])
    // assume another column whose value is a string
    val cf1cn2 = column(fam1, n4col2, classOf[String])
    // assume another column whose value is a string
    val cf1cn3 = column(fam1, n4col3, classOf[String])

    // Another column family, with 1 column
    val fam2 = family[String, String, Any](DeleteOpExample.n4fam2)
    // assume a column title whose value is a string
    val cf2cn1 = column(fam2, n4col1, classOf[String])
    // assume a column title whose value is a string
    val cf2cn2 = column(fam2, n4col2, classOf[String])
    // assume a column title whose value is a string
    val cf2cn3 = column(fam2, n4col3, classOf[String])

  }

  class SimpleHbaseTableRow(table:SimpleHbaseTable,result:DeserializedResult) extends HRow[SimpleHbaseTable,String](result,table)
  
  private val helper : HBaseHelper = HBaseHelper.getHelper(DeleteOpExample.conf);

  def main(args:Array[String]) {
    
    // HOUSEKEEPING
    
    // register with the schema
    val SimpleTable = table(new SimpleHbaseTable(n4table))
    
    // *** INDIVIDUAL DELETE ***
    
    // prime hbase (drop and create) using the helper from the book
    DeleteOpExample.tables.foreach {
      table => {
        helper.dropTable(table.tableName)
        val famNames = table.familyBytes.toArray.map(new String(_))
        helper.createTable(table.tableName, famNames:_*)
      }
    }
    
    // FILL TABLE WITH 1 ROW FOR LATER INDIVIDUAL DELETE
    
    helper.put(n4table,
      Array(n4row1),
      Array(n4fam1,n4fam2),
      Array(n4col1, n4col1, n4col2, n4col2, n4col3, n4col3),
      Array[Long]( 1, 2, 3, 4, 5, 6 ),
      Array( "val1", "val2", "val3", "val4", "val5", "val6" ));
    println("Before individual delete calls...");
    helper.dump(n4table, Array(n4row1), null, null);
    println("(dump complete)")
    
    // REGULAR EXECUTION (INDIVIDUAL)

    // equivalent to deleteColumns(fam1,n4col1) && deleteColumns(fam1,n4col3)
    
    SimpleTable.delete(n4row1)
        .values(_.fam1,Set(n4col1,n4col3))
        .execute()
        
    println("After delete columns...");
    helper.dump(n4table, Array(n4row1), null, null);
    println("(dump complete)")

    // equivalent to deleteFamily(fam1)

    SimpleTable.delete(n4row1)
        .family(_.fam1)
        .execute()
        
    println("After delete family...");
    helper.dump(n4table, Array(n4row1), null, null);
    println("(dump complete)")
    
    // *** DELETE LIST ***
    
    // prime hbase (drop and create) using the helper from the book
    DeleteOpExample.tables.foreach {
      table => {
        helper.dropTable(table.tableName)
        val famNames = table.familyBytes.toArray.map(new String(_))
        helper.createTable(table.tableName, famNames:_*)
      }
    }
    
    // FILL TABLE WITH 3 ROWS FOR LATER DELETE LIST
    
    helper.put(n4table,
      Array(n4row1),
      Array(n4fam1,n4fam2),
      Array(n4col1, n4col1, n4col2, n4col2, n4col3, n4col3),
      Array[Long]( 1, 2, 3, 4, 5, 6 ),
      Array( "val1", "val2", "val3", "val4", "val5", "val6" ));
    helper.put(n4table,
      Array(n4row2),
      Array(n4fam1,n4fam2),
      Array(n4col1, n4col1, n4col2, n4col2, n4col3, n4col3),
      Array[Long]( 1, 2, 3, 4, 5, 6 ),
      Array( "val1", "val2", "val3", "val4", "val5", "val6" ));
    helper.put(n4table,
      Array(n4row3),
      Array(n4fam1,n4fam2),
      Array(n4col1, n4col1, n4col2, n4col2, n4col3, n4col3),
      Array[Long]( 1, 2, 3, 4, 5, 6 ),
      Array( "val1", "val2", "val3", "val4", "val5", "val6" ));
    println("Before delete list calls...");
    helper.dump(n4table, Array(n4row1,n4row2,n4row3), null, null);
    println("(dump complete)")
    
    // REGULAR EXECUTION (DELETE LIST)
    
    SimpleTable.delete(n4row1)
        .delete(n4row2)
        .values(_.fam1,Set(n4col1,n4col3))
        .delete(n4row3)
        .family(_.fam2)
        .execute()
        
    println("After delete list (no row1, short fam1 in row 2, no fam2 in row3)");
    helper.dump(n4table, Array(n4row1,n4row2,n4row3), null, null);
    println("(dump complete)")

    // ERROR HANDLING
    
    // prime hbase (drop and create) using the helper from the book
    DeleteOpExample.tables.foreach {
      table => {
        helper.dropTable(table.tableName)
        val famNames = table.familyBytes.toArray.map(new String(_))
        helper.createTable(table.tableName, famNames:_*)
      }
    }
    
    // FILL TABLE WITH 3 ROWS
    
    helper.put(n4table,
      Array(n4row1),
      Array(n4fam1,n4fam2),
      Array(n4col1, n4col1, n4col2, n4col2, n4col3, n4col3),
      Array[Long]( 1, 2, 3, 4, 5, 6 ),
      Array( "val1", "val2", "val3", "val4", "val5", "val6" ));
    helper.put(n4table,
      Array(n4row2),
      Array(n4fam1,n4fam2),
      Array(n4col1, n4col1, n4col2, n4col2, n4col3, n4col3),
      Array[Long]( 1, 2, 3, 4, 5, 6 ),
      Array( "val1", "val2", "val3", "val4", "val5", "val6" ));
    helper.put(n4table,
      Array(n4row3),
      Array(n4fam1,n4fam2),
      Array(n4col1, n4col1, n4col2, n4col2, n4col3, n4col3),
      Array[Long]( 1, 2, 3, 4, 5, 6 ),
      Array( "val1", "val2", "val3", "val4", "val5", "val6" ));
    println("Before faulty delete list calls...");
    helper.dump(n4table, Array(n4row1,n4row2,n4row3), null, null);
    println("(dump complete)")
    
    // ERROR CONDITION: bad column family
        
        
    def bogusFamily[T,R,F,K,V] (arg : T) : ColumnFamily[SimpleHbaseTable, String, String, String, Any] = {
       val bogusFam = SimpleTable.family[String,String,Any]("bogus")
       // SimpleTable.column(bogusFam, "(anything)", classOf[String])
       bogusFam
    }
    
    try {
      SimpleTable.delete(n4row1)
        .family(bogusFamily)
        .delete(n4row2)
        .values(_.fam1,Set(n4col1,n4col3))
        .delete(n4row3)
        .family(_.fam2)
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
    
    println("After faulty delete list ((row1 still there), short fam1 in row 2, no fam2 in row3)");
    helper.dump(n4table, Array(n4row1,n4row2,n4row3), null, null);
    println("(dump complete)")
    
  }
}
