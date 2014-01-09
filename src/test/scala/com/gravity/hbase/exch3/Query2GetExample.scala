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
 * GetExample
 * GetListExample
 * GetListErrorExample
 * 
 * NOTE: a running hbase is expected to exist; see book for details
 * 
 * Query2 is HPaste query facility.  It consolidates single Get, list Get,
 * and much more.  This first example is limited to Get (and gyrations),
 * and works (similarly to PutOp) by composing the elements to be queried
 * on (row, column family, column name) and then invoking execute().  
 * There can be more than one way to obtain the same result, since query
 * assembly is decomposed into elements used for its construction, that 
 * sometimes can be put together flexibly.  As in PutOp, nothing happens
 * until execute() is invoked: the entire query is buffered client-side. 
 * 
 * 
 */

object Query2GetExample extends Schema {
  
  implicit val conf : Configuration  = HBaseConfiguration.create();
  val n4fam1 : String = "fam1"
  val n4fam2 : String = "fam2"
  val n4col1 : String = "colName1"
  val n4col2 : String = "colName2"
  val n4row1 : String = "rowName1"
  val n4row2 : String = "rowName2"
    
  class SimpleHbaseTable (val tName : String) extends HbaseTable[SimpleHbaseTable,String,SimpleHbaseTableRow] (tName,rowKeyClass=classOf[String], tableConfig = HbaseTableConfig(maxFileSizeInBytes=1073741824)) {
    
    def rowBuilder(result:DeserializedResult) = new SimpleHbaseTableRow(this,result)

    // Column family definition, with 2 columns
    val fam1 = family[String, String, Any](Query2GetExample.n4fam1)
    // assume a column title whose value is a string; e.g. col is fam1:colName1
    val cf1cn1 = column(fam1, n4col1, classOf[String])
    // assume another column whose value is a string
    val cf1cn2 = column(fam1, n4col2, classOf[String])

    // Another column family, with 1 column
    val fam2 = family[String, String, Any](Query2GetExample.n4fam2)
    // assume a column title whose value is a string
    val cf2cn1 = column(fam2, n4col1, classOf[String])
    // assume a column title whose value is a string
    val cf2cn2 = column(fam2, n4col2, classOf[String])

  }

  class SimpleHbaseTableRow(table:SimpleHbaseTable,result:DeserializedResult) extends HRow[SimpleHbaseTable,String](result,table) {
    override def toString = {
      if (result.hasErrors) "errors found"
      else if (result.isEmpty) "nothing found"
      else {
    	  "[found] key:"+result.getRow[String]
      }
    }
  }
  
  private val helper : HBaseHelper = HBaseHelper.getHelper(Query2GetExample.conf);

  def main(args:Array[String]) {
    
    // HOUSEKEEPING
    
    // register with the schema
    val SimpleTable = table(new SimpleHbaseTable("simpleQueryTable"))
    
    // prime hbase (drop and create) using the helper from the book
    Query2GetExample.tables.foreach {
      table => {
        helper.dropTable(table.tableName)
        val famNames = table.familyBytes.toArray.map(new String(_))
        helper.createTable(table.tableName, famNames:_*)
      }
    }
    
    // FILL TABLE WITH SOMETHING FOR LATER GET/QUERY
    
    SimpleTable.put(n4row1)
        .value(_.cf1cn1,"f1c1Val1")
        .value(_.cf1cn2,"f1c2Val1")
        .value(_.cf2cn1,"f2c1Val1")
        .put(n4row2)
        .value(_.cf1cn2,"f1c2ValA")
        .value(_.cf2cn1,"f2c1ValA")
        .value(_.cf2cn2,"f2c2ValA")
        .execute()
    
    // REGULAR EXECUTION    
        
    // get one specific row for one column family, by each column    

    val rowSeq1 : Seq[SimpleHbaseTableRow] = SimpleTable.query2
    	.withKey(n4row1)
    	.withColumn(_.cf1cn1)
    	.withColumn(_.cf1cn2)
    	.execute()
    	
    rowSeq1.foreach(rowKey => 
      for ((cn : String, value : String) <- rowKey.family(_.fam1)) { 
        println("1FbyC BASE "+rowKey.toString+" "+n4fam1+":"+cn+" ( with value => ) "+value)
      }
    )
    
    println

    // Or, alternatively ...

    val rowOpt1 : Option[SimpleHbaseTableRow] = SimpleTable.query2
      .withKey(n4row1)
      .withColumn(_.cf1cn1)
      .withColumn(_.cf1cn2)
      .singleOption()

    rowOpt1.foreach(rowKey =>
      for ((cn : String, value : String) <- rowKey.family(_.fam1)) {
        println("1FbyC BASE "+rowKey.toString+" "+n4fam1+":"+cn+" ( with value => ) "+value)
      }
    )

    println

    // get one specific row for one column family, by each column (alternate 1)

    val rowSeq1alt1 : Seq[SimpleHbaseTableRow] = SimpleTable.query2
    	.withKey(n4row1)
    	.withColumn(_.fam1,n4col1)
    	.withColumn(_.fam1,n4col2)
    	.execute()
    	
    rowSeq1alt1.foreach(rowKey => 
      for ((cn : String, value : String) <- rowKey.family(_.fam1)) { 
        println("1FbyC ALT1 "+rowKey.toString+" "+n4fam1+":"+cn+" ( with value => ) "+value)
      }
    )
    
    println

    // get one specific row for one column family, by each column (alternate 2)    

    val rowSeq1alt2 : Seq[SimpleHbaseTableRow] = SimpleTable.query2
    	.withKey(n4row1)
    	.withColumns(_.cf1cn1, _.cf1cn2)
    	.execute()
    	
    rowSeq1alt2.foreach(rowKey => 
      for ((cn : String, value : String) <- rowKey.family(_.fam1)) { 
        println("1FbyC ALT2 "+rowKey.toString+" "+n4fam1+":"+cn+" ( with value => ) "+value)
      }
    )
    
    println
   	
    // get one specific row for one column family (all columns)

    val rowSeq2 : Seq[SimpleHbaseTableRow] = SimpleTable.query2
    	.withKey(n4row1)
    	.withFamilies(_.fam1)
    	.execute()
    	
    rowSeq2.foreach(rowKey => 
      for ((cn : String, value : String) <- rowKey.family(_.fam1)) { 
        println("1FbyA BASE "+rowKey.toString+" "+n4fam1+":"+cn+" ( with value => ) "+value)
      }
    )
    
    println
   	
    // get one specific row for another column family (all columns)

    val rowSeq2alt : Seq[SimpleHbaseTableRow] = SimpleTable.query2
    	.withKey(n4row1)
    	.withFamilies(_.fam2)
    	.execute()
    	
    rowSeq2alt.foreach(rowKey => 
      for ((cn : String, value : String) <- rowKey.family(_.fam2)) { 
        println("1FbyA ALT1 "+rowKey.toString+" "+n4fam2+":"+cn+" ( with value => ) "+value)
      }
    )
    
    println
   	
    // get one specific row for multiple column families (all columns)

    val rowSeq3 : Seq[SimpleHbaseTableRow] = SimpleTable.query2
    	.withKey(n4row1)
    	.withFamilies(_.fam1, _.fam2)
    	.execute()
    	
    rowSeq3.foreach(rowKey => {
        for ((cn : String, value : String) <- rowKey.family(_.fam1)) { 
          println("MFbyA BASE "+rowKey.toString+" "+n4fam1+":"+cn+" ( with value => ) "+value)
        }
        for ((cn : String, value : String) <- rowKey.family(_.fam2)) { 
          println("MFbyA BASE "+rowKey.toString+" "+n4fam2+":"+cn+" ( with value => ) "+value)
        }
      }
    )
    
    println
   	
    // get multiple rows for one column family (all columns)    

    val rowSeq4 : Seq[SimpleHbaseTableRow] = SimpleTable.query2
    	.withKeys(Set(n4row1,n4row2))
    	.withFamilies(_.fam1)
    	.execute()
    	
    rowSeq4.foreach(rowKey => 
      for ((cn : String, value : String) <- rowKey.family(_.fam1)) { 
        println("1FxMR BASE "+rowKey.toString+" "+n4fam1+":"+cn+" ( with value => ) "+value)
      }
    )
   	
    println
   	
    // get multiple rows for multiple column families (all columns)    

    val rowSeq5 : Seq[SimpleHbaseTableRow] = SimpleTable.query2
    	.withKeys(Set(n4row1,n4row2))
    	.withFamilies(_.fam1,_.fam2)
    	.execute()
    	
    rowSeq5.foreach(rowKey => { 
        for ((cn : String, value : String) <- rowKey.family(_.fam1)) { 
          println("MFxMR BASE "+rowKey.toString+" "+n4fam1+":"+cn+" ( with value => ) "+value)
        }
        for ((cn : String, value : String) <- rowKey.family(_.fam2)) { 
          println("MFxMR BASE "+rowKey.toString+" "+n4fam2+":"+cn+" ( with value => ) "+value)
        }
      }  
    )
   	
    println
   	
    // get multiple rows for multiple column families, some columns only   

    val rowSeq6 : Seq[SimpleHbaseTableRow] = SimpleTable.query2
    	.withKeys(Set(n4row1,n4row2))
    	.withFamilies(_.fam1,_.fam2)
    	.withColumn(_.fam1,n4col1)
    	.withColumn(_.fam2,n4col1)
    	.execute()
    	
    rowSeq6.foreach(rowKey => { 
        for ((cn : String, value : String) <- rowKey.family(_.fam1)) { 
          println("MFMR1 BASE "+rowKey.toString+" "+n4fam1+":"+cn+" ( with value => ) "+value)
        }
        for ((cn : String, value : String) <- rowKey.family(_.fam2)) { 
          println("MFMR1 BASE "+rowKey.toString+" "+n4fam2+":"+cn+" ( with value => ) "+value)
        }
      }  
    )
    
    println
            
    // ERROR CONDITION: bad column family
        
    def bogusFamily[T,R,F,K,V] (arg : T) : Column[SimpleHbaseTable, String, String, String, String] = {
       val bogusFam = SimpleTable.family[String,String,Any]("bogus")
       SimpleTable.column(bogusFam, "(anything)", classOf[String])
    }
    
    // try get multiple rows with a bogus column family    

    try {
      SimpleTable.query2
    	.withKey(n4row1)
    	.withColumns(_.cf1cn1, _.cf1cn2, bogusFamily)
    	.execute()
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
