package com.gravity.hbase.exch7

import scala.collection.mutable.HashSet
import scala.collection.mutable.SynchronizedSet

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.binary.Hex

import com.gravity.hbase.schema.HbaseTable
import com.gravity.hbase.schema.HRow
import com.gravity.hbase.schema.Column
import com.gravity.hbase.schema.ColumnFamily
import com.gravity.hbase.schema.Schema
import com.gravity.hbase.schema.HbaseTableConfig
import com.gravity.hbase.schema.DeserializedResult
import com.gravity.hbase.mapreduce._

import util.HBaseHelper;

/**
 * "HBase: the definitive guide" examples translated to HPaste in this file
 * 
 * 
 * NOTE: a running hbase is expected to exist; see book for details
 * 
 * Technical note
 * 
 * HMapper[MK, MV, MOK, MOV] extends Mapper[MK, MV, MOK, MOV] with MRWritable[MOK, MOV]
 * around line 980 in mapreduce2.scala does override map(key,value,Context) 
 * in Mapper<MK, MV, MOK, MOV> and replaces it with its own map().  Its concrete
 * implementations must implement map() and not map(key,value,Context).  In turn,
 * this imposes a different structural model on the concrete Mappers.  In other 
 * words, architectural assumptions rhyme, but are different because HPaste
 * is MapReduce specialized for a new model, better suited to HBase tables
 * (and other items) as abstracted in HPaste itself.  Under MapReduce it happens 
 * that map(key,value,Context) is invoked once per key-value in input.  HPaste,
 * designed for use with HBase, operates under a different assumption that a
 * monolithic-like piece of data is available (an HBase table), that this data
 * can be accessed by index (i.e. by row), and that map() will take over the 
 * responsibility of iterating through the input keys using a locally available
 * object instead of new parameters supplied on a per-call basis.  Yet in other
 * words, the decomposition models of MapReduce in Hadoop, and MapReduce in HPaste,
 * use different abstractions: remotely (as in elsewhere in the code, rather than
 * on a different machine...) iterated data producing different per-call actual 
 * parameters for map(key,value,Context) in Hadoop, vs. a locally (as in this code)
 * iterated blob of data unwound in some custom manner using map() in HPaste.  (Which
 * also means HPaste is implementing its own Readers.)  Accordingly. example
 * 
 * ImportFromFile.java
 * 
 * which deals with reading data from a text file for import into HBase, must
 * be handled in a somewhat different manner.  (See for yet another gyration
 * "Processing a whole file as a record" in "Hadoop: the definitive guide 3rd ed."
 * by Tom White)
 * 
 *   
 */


object Mr2Example extends Schema {
  
  implicit val conf : Configuration  = HBaseConfiguration.create();
  val n4StagingTable : String = "mrJsonStagingTable"
  val n4HodTable : String = "mrJsonByHourOfDayTable"
  val n4AuthorTable : String = "mrJsonAuthorTable"
  val n4fam1 : String = "MR2_fam1"
    
  val n4StagingCol1 : String = "json"
  val n4HodCol : Array[String] = Array(
        "00","01","02","03","04","05","06",
        "07","08","09","10","11","12","13",
        "14","15","16","17","18","19","20",
        "21","22","23"
      )
  val n4AuthorCol1 : String = "latestPostInTheDay"
    
  class SimpleHbaseTable (val tName : String) extends HbaseTable[SimpleHbaseTable,String,SimpleHbaseTableRow] (tName,rowKeyClass=classOf[String], tableConfig = HbaseTableConfig(maxFileSizeInBytes=1073741824)) {
    
    def rowBuilder(result:DeserializedResult) = new SimpleHbaseTableRow(this,result)

    // Column family definition, with 2 columns
    val fam1 = family[String, String, Any](n4fam1)
    // assume a column title whose value is a string
    val cf1cn1 = column(fam1, n4StagingCol1, classOf[String])

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
      val rowHint = result.getRow[String];
      fam1map.foreach{ case(k,v) => sb.append(rowHint+" "+n4fam1+":"+k+"="+v+"; ") }
      sb.toString
    }
    
    def getKey : String = rowid
  }
  
  class JsonByHourOfDayHbaseTable (val tName : String) extends HbaseTable[JsonByHourOfDayHbaseTable,String,JsonByHourOfDayHbaseTableRow] (tName,rowKeyClass=classOf[String], tableConfig = HbaseTableConfig(maxFileSizeInBytes=1073741824)) {
    
    def rowBuilder(result:DeserializedResult) = new JsonByHourOfDayHbaseTableRow(this,result)

    // Column family definition, with 2 columns
    val fam1 = family[String, String, Any](n4fam1)
    // assume a column title whose value is a string
    val cf1cn00 = column(fam1, n4HodCol(0), classOf[String])
    val cf1cn01 = column(fam1, n4HodCol(1), classOf[String])
    val cf1cn02 = column(fam1, n4HodCol(2), classOf[String])
    val cf1cn03 = column(fam1, n4HodCol(3), classOf[String])
    val cf1cn04 = column(fam1, n4HodCol(4), classOf[String])
    val cf1cn05 = column(fam1, n4HodCol(5), classOf[String])
    val cf1cn06 = column(fam1, n4HodCol(6), classOf[String])
    val cf1cn07 = column(fam1, n4HodCol(7), classOf[String])
    val cf1cn08 = column(fam1, n4HodCol(8), classOf[String])
    val cf1cn09 = column(fam1, n4HodCol(9), classOf[String])
    val cf1cn10 = column(fam1, n4HodCol(10), classOf[String])
    val cf1cn11 = column(fam1, n4HodCol(11), classOf[String])
    val cf1cn12 = column(fam1, n4HodCol(12), classOf[String])
    val cf1cn13 = column(fam1, n4HodCol(13), classOf[String])
    val cf1cn14 = column(fam1, n4HodCol(14), classOf[String])
    val cf1cn15 = column(fam1, n4HodCol(15), classOf[String])
    val cf1cn16 = column(fam1, n4HodCol(16), classOf[String])
    val cf1cn17 = column(fam1, n4HodCol(17), classOf[String])
    val cf1cn18 = column(fam1, n4HodCol(18), classOf[String])
    val cf1cn19 = column(fam1, n4HodCol(19), classOf[String])
    val cf1cn20 = column(fam1, n4HodCol(20), classOf[String])
    val cf1cn21 = column(fam1, n4HodCol(21), classOf[String])
    val cf1cn22 = column(fam1, n4HodCol(22), classOf[String])
    val cf1cn23 = column(fam1, n4HodCol(23), classOf[String])
    val cf1col : Array[Column[JsonByHourOfDayHbaseTable,String,String,String,String]] = Array(
        cf1cn00, cf1cn01, cf1cn02, cf1cn03, cf1cn04, cf1cn05, cf1cn06,
        cf1cn07, cf1cn08, cf1cn09, cf1cn10, cf1cn11, cf1cn12, cf1cn13,
        cf1cn14, cf1cn15, cf1cn16, cf1cn17, cf1cn18, cf1cn19, cf1cn20,
        cf1cn21, cf1cn22, cf1cn23
    )

  }

  class JsonByHourOfDayHbaseTableRow(table:JsonByHourOfDayHbaseTable,result:DeserializedResult) extends HRow[JsonByHourOfDayHbaseTable,String](result,table) {
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
      val rowHint = result.getRow[String];
      fam1map.foreach{ case(k,v) => sb.append(rowHint+" "+n4fam1+":"+k+"="+v+"; ") }
      sb.toString
    }
    
    def getKey : String = { result.getRow[String] }
  }

  class JsonAuthorHbaseTable (val tName : String) extends HbaseTable[JsonAuthorHbaseTable,String,JsonAuthorHbaseTableRow] (tName,rowKeyClass=classOf[String], tableConfig = HbaseTableConfig(maxFileSizeInBytes=1073741824)) {
    
    def rowBuilder(result:DeserializedResult) = new JsonAuthorHbaseTableRow(this,result)

    // Column family definition, with 2 columns
    val fam1 = family[String, String, Any](n4fam1)
    // assume a column title whose value is a string
    val cf1cn1 = column(fam1, n4AuthorCol1, classOf[String])

  }

  class JsonAuthorHbaseTableRow(table:JsonAuthorHbaseTable,result:DeserializedResult) extends HRow[JsonAuthorHbaseTable,String](result,table) {
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
      val rowHint = result.getRow[String];
      fam1map.foreach{ case(k,v) => sb.append(rowHint+" "+n4fam1+":"+k+"="+v+"; ") }
      sb.toString
    }
    
    def getKey : String = { result.getRow[String] }
  }
  
  private val helper : HBaseHelper = HBaseHelper.getHelper(Mr2Example.conf);
  val hexer : Hex = new Hex()
  val parser : JSONParser = new JSONParser();


  // register with the schema
  val JsonStagingTable = table(new SimpleHbaseTable(n4StagingTable))
  val JsonByHourOfDayTable = table(new JsonByHourOfDayHbaseTable(n4HodTable)) 
  val JsonAuthorTable = table(new JsonAuthorHbaseTable(n4AuthorTable)) 
  
  def main(args:Array[String]) {
    
    // HOUSEKEEPING
    
    // prime hbase (drop and create) using the helper from the book
    Mr2Example.tables.foreach {(
      table => {
        helper.dropTable(table.tableName)
        val famNames = table.familyBytes.toArray.map(new String(_))
        helper.createTable(table.tableName, famNames:_*)
      })
    }
    
    // execution of Map2KeyValSampleJob() is optional
    new Map2KeyValSampleJob().run(Settings.None, Mr2Example.conf)
    
    // ImportFromFileExampleJob is needed by the next jobs; i.e. you
    // must run ImportFromFileExampleJob before you can successfully
    // run AnalyzeDataExampleJob() or ParseJsonMultiExampleJob(), but
    // AnalyzeDataExampleJob() or ParseJsonMultiExampleJob() are 
    // independent of each other
    new ImportFromFileExampleJob().run(Settings.None, Mr2Example.conf)
    new AnalyzeDataExampleJob().run(Settings.None, Mr2Example.conf)
    new ParseJsonMultiExampleJob().run(Settings.None, Mr2Example.conf)
  }  
}

/**
 * An out-of-band example of how to use HPaste to map from text 
 * to text.  It introduces FromTextFileByLineMapper in a simple
 * form.  A specialization of this Mapper will be used later to
 * write to an HBase table, rather than to a text file.  There 
 * is only the Map part of the job here, so the output is not 
 * sorted.
 */
class Map2KeyValSampleJob extends HJob[NoSettings]("Map text to text",
  HMapTask(
    HTaskID("Map Text File by Line"),
    HTaskConfigs(),
    HIO(
      // where to read an existing source file
      HPathInput(Seq("./src/test/resources/test-data.txt")),
      // directory where to dump the output of map
      HTextOutput[Text,Text]("./target/test-data-text")
    ),
    // FromTextFileByLineMapper finds its input from HPathInput
    // above, and prepares the iterator 'lines' that lazily reads
    // the input file--because it's lazy, data is not buffered
    // into memory so very large files can be handled
    new FromTextFileByLineMapper(classOf[Text], classOf[Text]) {
      def map() {
        if (lines.hasNext) {
          val line : String = lines.next
          // encode to hex for readability
          val willBeKey = new String(Mr2Example.hexer.encode(DigestUtils.md5(line)))
          val willBeVal = line
          write(new Text(willBeKey), new Text(willBeVal))
        } else {
          write(new Text("none"), new Text("empty"))
        }
      }
    }
  )
)

/**
 * HPaste equivalent of the ImportFromFile example.  Use a text file to fill
 * a table in HBase.  There is only the Map part of the job here.
 */
class ImportFromFileExampleJob extends HJob[NoSettings]("Map text to table",
  HMapTask(
    HTaskID("Map Text File to fill HBase Table, one row per line"),
    HTaskConfigs(),
    HIO(
      // where to read an existing source file
      HPathInput(Seq("./src/test/resources/test-data.txt")),
      // table where to dump the output of map
      HTableOutput(Mr2Example.JsonStagingTable)
    ),
    // FromTextFileByLineToTableMapper inherits from the above
    // FromTextFileByLineMapper, but specializes for dumping
    // output to an HBase table.  Only Map in this job, no reduce
    new FromTextFileByLineToTableMapper(Mr2Example.JsonStagingTable) {
      def map() {
        if (lines.hasNext) {
          val line : String = lines.next
          // encode to hex for readability
          val willBeKey = new String(Mr2Example.hexer.encode(DigestUtils.md5(line)))
          val willBeVal = line
          write(Mr2Example.JsonStagingTable.put(willBeKey).value(_.cf1cn1,willBeVal))
        } else {
          println("map out of sync")
        }
      }
    }
  )
)

/**
 * HPaste equivalent of the AnalyzeData example.  This is a full MapReduce
 * job: count, for each author, how many entries there are in the input file.
 */
class AnalyzeDataExampleJob extends HJob[NoSettings]("MapReduce table to text",
  HMapReduceTask(
    HTaskID("Use HBase Table to source data for analysis: count posts by author"),
    HTaskConfigs(),
    HIO(
      // table where to source the data to analyze
      // this table was loaded previously from text
      HTableInput(Mr2Example.JsonStagingTable),
      // directory where to dump the output of map
      HPathOutput("./target/test-data-analyzed")
    ),
    new FromTableBinaryMapper(Mr2Example.JsonStagingTable) {
      def map() {
        val tableRow = row
        val willBeValue = tableRow.getKey
        val jsonText = tableRow.column(_.cf1cn1).getOrElse("")
        val json : JSONObject = Mr2Example.parser.parse(jsonText).asInstanceOf[JSONObject];
        val willBeKey : String = json.get("author").asInstanceOf[String];
        write(
          {keyOutput => keyOutput.writeUTF(willBeKey)},
          {valueOutput => valueOutput.writeUTF(willBeValue)}
        )
      }
    },
    new BinaryToTextReducer() {
      def reduce() {
        val author : String = readKey(_.readUTF())
        var count : Int = 0; 
        perValue {  valueInput => count += 1 }
        writeln(author + "\t" + count)
      }
    }
  )
)

/**
 * HPaste functionally equivalent of the ParseJsonMulti example.  This is a full MapReduce
 * job. Collect which authors update posts by hourly buckets.  Also, for each author,
 * record the hour of the day's latest post i.e. at what latest time before midnight each 
 * author updated. 
 */
class ParseJsonMultiExampleJob extends HJob[NoSettings]("MapReduce table to multiple tables",
  HMapReduceTask(
    HTaskID("Use HBase Table to source data; break up results into different tables"),
    HTaskConfigs(),
    HIO(
      // table where to source the data to analyze
      // this table was loaded previously from text
      HTableInput(Mr2Example.JsonStagingTable),
      // two tables where to write output; two are needed,
      // because each table has a different key (as reflected
      // in the name of the table itself)
      HMultiTableOutput(true,Mr2Example.JsonByHourOfDayTable,Mr2Example.JsonAuthorTable)
    ),
    new FromTableBinaryMapper(Mr2Example.JsonStagingTable) {
      def map() {
        val tableRow = row
        val jsonText = tableRow.column(_.cf1cn1).getOrElse("")
        val json : JSONObject = Mr2Example.parser.parse(jsonText).asInstanceOf[JSONObject];
        val updated : String = json.get("updated").asInstanceOf[String];
        val timePieces = updated.split(":")
        val willBeKey : String = timePieces(0).substring(timePieces(0).length()-2) // hour as 00..23
        val willBeValue : String = json.get("author").asInstanceOf[String];
        write(
          {keyOutput => keyOutput.writeUTF(willBeKey)},
          {valueOutput => valueOutput.writeUTF(willBeValue)}
        )
      }
    },
    new BinaryToMultiTableReducer(Mr2Example.JsonByHourOfDayTable,Mr2Example.JsonAuthorTable) {
      def reduce() {
        val willBeKey : String = readKey(_.readUTF())
        val colIndex : Int = Integer.parseInt(willBeKey)
        val authorIterable = makePerValue {
          author => author.readUTF()
        }
        val willBeValue = authorIterable.mkString(",")
        write(Mr2Example.JsonByHourOfDayTable.put(willBeKey).value(_.cf1col(colIndex),willBeValue))
        val hourValue = willBeKey
        for (willBeAlterKey <- authorIterable)
        	write(Mr2Example.JsonAuthorTable.put(willBeAlterKey).value(_.cf1cn1,hourValue))
      }
    }
  )
)
