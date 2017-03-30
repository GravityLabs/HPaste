package com.gravity.hbase.schema

import com.gravity.utilities.GrvConcurrentSet
import junit.framework.TestSuite
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.util.Bytes

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * This sets up the testing cluster.
 * We don't support auto table creation (and all the dangerous thereof), so we manually use the Hbase API to create our test tables.
 */
object LocalCluster {
  val htest = new HBaseTestingUtility()
  htest.startMiniCluster()

  def getTestConfiguration = htest.getConfiguration

  private val alreadyInittedTables = new GrvConcurrentSet[String]()

  def initializeSchema(schema:Schema) = alreadyInittedTables.synchronized {

    schema.tables.foreach { table =>
      //we need to delete the old one before we can initialize the new one
      if(alreadyInittedTables.contains(table.tableName)){
        htest.deleteTable(Bytes.toBytes(table.tableName))
      }
      htest.createTable(Bytes.toBytes(table.tableName), table.familyBytes.toArray)
      alreadyInittedTables += table.tableName
    }
  }

  def deleteSchema(schema:Schema) = alreadyInittedTables.synchronized {
    schema.tables.foreach { table =>
      if(alreadyInittedTables.contains(table.tableName)) {
        htest.deleteTable(Bytes.toBytes(table.tableName))
      }
      alreadyInittedTables -= table.tableName
    }
  }
}

class HPasteTestCase(schema:Schema) extends TestSuite {

  LocalCluster.initializeSchema(schema)

  def withCleanup(work: => Unit) = {
    LocalCluster.initializeSchema(schema)

    try {
      work
    } finally {
      LocalCluster.deleteSchema(schema)
    }
  }

}