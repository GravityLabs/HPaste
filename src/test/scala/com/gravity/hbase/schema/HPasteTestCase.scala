package com.gravity.hbase.schema

import junit.framework.TestCase
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseTestingUtility
import scala.collection.mutable.{SynchronizedSet, HashSet}
import org.junit.{After, Before, Test}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.mapred.MiniMRCluster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import java.io.{File, InputStreamReader, BufferedReader}
import java.net.ServerSocket
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption
import collection.mutable

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

class HPasteTestCase(schema:Schema, name: String) extends TestCase(name) {

  if (schema != null) LocalCluster.initializeSchema(schema)

  def this() = this(null, "none")

  def this(name: String) = this(null, name)

  def this(schema: Schema) = this(schema, "none")

  @Test
  def testEliminateWarning() { /* this avoids a warning */ }

}

class HPasteMapReduceTestCase(paths: Seq[String], name: String) extends TestCase(name) {

  var dfsCluster: MiniDFSCluster = _
  var mrCluster: MiniMRCluster = _
  var conf: Configuration = _


  def this() = this(Seq.empty[String], "none")

  def this(name: String) = this(Seq.empty[String], name)

  def this(paths: Seq[String]) = this(paths, "none")

  def getFileSystem() = dfsCluster.getFileSystem

  def getJobOutput(path: String) = {
    val sb: StringBuilder = new StringBuilder
    getFileSystem().listStatus(new Path(path)).foreach {
      fileStatus =>
        if (!fileStatus.isDir) {
          val stream = new BufferedReader(new InputStreamReader(getFileSystem().open(fileStatus.getPath)))
          var currentLine = ""
          while ( {
            currentLine = stream.readLine(); currentLine
          } != null) sb.append(currentLine + "\n")
          stream.close()
        }
    }
    sb.toString()
  }

  def getRandomPort(): Int = {
    val localmachine: ServerSocket = new ServerSocket(0)
    val randomPort = localmachine.getLocalPort()
    localmachine.close()
    return randomPort
  }

  @Before
  override def setUp() {
    super.setUp()
    val logs = File.createTempFile("hadoop-logs", "").getName
    new File(logs).mkdirs()
    System.setProperty("hadoop.log.dir", logs)
    System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")
    System.setProperty("javax.xml.parsers.SAXParserFactory", "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl")

    val location = File.createTempFile("dfs", "")
    location.delete()
    val dfsName = new File(location.getName + File.separatorChar + "name")
    val dfsData = new File(location.getName + File.separatorChar + "data")
    dfsName.mkdirs()
    dfsData.mkdirs()

    conf = new Configuration
    conf.set("dfs.name.dir", dfsName.getAbsolutePath)
    conf.set("dfs.data.dir", dfsData.getAbsolutePath)

    dfsCluster = new MiniDFSCluster(getRandomPort(), conf, 1, true, false, StartupOption.REGULAR, Array[String]("/rack1"))
    for (p <- paths) getFileSystem().makeQualified(new Path(p))

    mrCluster = new MiniMRCluster(1, getFileSystem().getUri().toString(), 1)
  }

  @Test
  def testEliminateWarning() { /* this avoids a warning */ }

  @After
  override def tearDown() {
    super.tearDown()
    println("stopping services")
    if (mrCluster != null) {
      println("stopping mapred...")
      mrCluster.shutdown()
      mrCluster = null
    }
    if (dfsCluster != null) {
      println("stopping datanodes...")
      dfsCluster.shutdownDataNodes()
      println("stopping hdfs...")
      dfsCluster.shutdown()
      dfsCluster = null
    }
    println("done stopping services")
  }


}