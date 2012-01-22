package com.gravity.hbase.schema

import junit.framework.TestCase
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseTestingUtility
import scala.collection.mutable.{SynchronizedSet, HashSet}

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

  private val alreadyInittedTables = new HashSet[String] with SynchronizedSet[String]

  def initializeSchema(schema:Schema) {
    schema.tables.foreach {
      table =>
        if(!alreadyInittedTables.exists(_ == table.tableName)){
          htest.createTable(Bytes.toBytes(table.tableName), table.familyBytes.toArray)
          alreadyInittedTables += table.tableName
        }
    }
  }
}

class HPasteTestCase(schema:Schema) extends TestCase {

  LocalCluster.initializeSchema(schema)
}