package com.gravity.hbase.mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.{Mapper, Reducer, Job}
import com.gravity.hbase.schema._
import com.gravity.hadoop.{GravityTableOutputFormat, HadoopScalaShim}
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper
import org.apache.hadoop.hbase.client.{Scan, Result}
import org.apache.hadoop.hbase.mapreduce.{TableMapper, TableInputFormat}
import java.io.{DataOutputStream, ByteArrayOutputStream}
import org.apache.hadoop.hbase.util.Base64
import collection.mutable.Buffer
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import sun.reflect.Reflection


/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */
class FuncMapper[MK,MV,MOK,MOV] extends Mapper[MK,MV,MOK,MOV] {

  var mapper : (MK,MV,(MOK,MOV)=>Unit,(String,Long)=>Unit)=>Unit = _

  override def setup(context: Mapper[MK,MV,MOK,MOV]#Context) {
    val jobClass = Class.forName(context.getConfiguration.get("mapperholder")).newInstance().asInstanceOf[FunctionalJobBase[MK,MV,MOK,MOV,_,_]]
    mapper = jobClass.mapper
  }

  override def map(key: MK, value: MV, context: Mapper[MK,MV,MOK,MOV]#Context) {
    def counter(ctr:String,times:Long) {context.getCounter("Hi",ctr).increment(times)}
    def write(key:MOK,value:MOV) {context.write(key,value)}

    counter("Test Run",1l)
    counter("Mapper " +context.getConfiguration.get("mapperholder"),1l)
    mapper(key,value,write,counter)
//    mapper(key,value,write,counter)
  }
}

class FuncReducer[IK,IV,OK,OV] extends Reducer[IK,IV,OK,OV] {
  var reducer : (IK,Iterable[IV],(OK,OV)=>Unit,(String,Long)=>Unit) =>Unit = _

  override def setup(context: Reducer[IK,IV,OK,OV]#Context) {
    val jobClass = Class.forName(context.getConfiguration.get("mapperholder")).newInstance().asInstanceOf[FunctionalJobBase[_,_,IK,IV,OK,OV]]
    reducer = jobClass.reducer

  }
}

abstract class FunctionalJobBase[MK,MV,MOK : Manifest,MOV : Manifest,ROK,ROV](name:String,
                  val mapper:(MK,MV,(MOK,MOV)=>Unit,(String,Long)=>Unit)=>Unit,
                  val reducer:(MOK,Iterable[MOV],(ROK,ROV)=>Unit,(String,Long)=>Unit)=>Unit                                   ) extends JobTrait {


  def run(conf:Configuration) {
    val c = new Configuration(conf)
    c.set("mapperholder", getClass.getName)
    configure(c)
    val job = new Job(c)
    job.setJarByClass(getClass)
    job.setJobName(name)
    FileInputFormat.addInputPath(job, new Path("/user/gravity/magellan/beacons/**/*.csv"))

    HadoopScalaShim.registerMapper(job,classOf[FuncMapper[MK,MV,MOK,MOV]])
//    job.setMapperClass(classOf[FuncMapper[MK,MV,MOK,MOV]])
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)

    HadoopScalaShim.registerReducer(job,classOf[FuncReducer[MOK,MOV,ROK,ROV]])
    //job.setReducerClass(classOf[FuncReducer[MOK,MOV,ROK,ROV]])

    FileOutputFormat.setOutputPath(job, new Path("/user/gravity/magellan/output"))

    configureJob(job)
    job.waitForCompletion(true)
  }
}

/**
 * Base class for Jobs that will be composed using the JobTraits.
 */
abstract class JobBase(name: String)(implicit conf: Configuration) extends JobTrait {
  var job: Job = _

  def init() {
    val jobConf = new Configuration(conf)
    configure(jobConf)

    job = new Job(jobConf)
    job.setJarByClass(getClass)
    job.setJobName(name)
    configureJob(job)
  }


  def run(): Boolean = {
    init()
    job.waitForCompletion(true)


  }

}

/**
 * Declares that you want a Reducer.  Will force you to provide the class, and the number thereof.
 * If you specify None for the numReducers it will use the configured default.
 */
trait ReducerJob[M <: Reducer[_,_,_,_]] extends JobTrait {
  val reducer: Class[M]
  val numReducers: Option[Int]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
//    job.setReducerClass(reducer)
    HadoopScalaShim.registerReducer(job, reducer)
    numReducers.foreach(reducerCount => job.setNumReduceTasks(reducerCount))
    super.configureJob(job)
  }
}

/**
 * Declares that you want a Mapper.  You'll be asked to provide a mapper,
 * as well as its output key and value classes (Hadoop can be very picky about this, so we
 * play it safe).
 */
trait MapperJob[M <: StandardMapper[MK, MV], MK, MV] extends JobTrait {
  val mapper: Class[M]
  val mapperOutputKey: Class[MK]
  val mapperOutputValue: Class[MV]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
//    job.setMapperClass(mapper)
    HadoopScalaShim.registerMapper(job, mapper)
    job.setMapOutputKeyClass(mapperOutputKey)
    job.setMapOutputValueClass(mapperOutputValue)

    super.configureJob(job)
  }


}

trait TableReducerJob[R <: TableWritingReducer[TF, TFK, MK, MV], TF <: HbaseTable[TF, TFK], TFK, MK, MV] extends ToTable[TF] {
  val reducer: Class[R]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
//    job.setReducerClass(reducer)
    HadoopScalaShim.registerReducer(job, reducer)
    super.configureJob(job)
  }


}

trait TableMapperJob[M <: TableReadingMapper[TF, TFK, MK, MV], TF <: HbaseTable[TF, TFK], TFK, MK, MV] extends FromTable[TF] {
  val mapper: Class[M]
  val mapperOutputKey: Class[MK]
  val mapperOutputValue: Class[MV]


  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
//    job.setMapperClass(mapper)
    HadoopScalaShim.registerMapper(job, mapper)
    job.setMapOutputKeyClass(mapperOutputKey)
    job.setMapOutputValueClass(mapperOutputValue)
    super.configureJob(job)

  }
}

trait ToPath extends JobTrait {
  val path : String

  override def configure(conf:Configuration) {
    FileSystem.get(conf).delete(new Path(path),true)

    super.configure(conf)
  }

   override def configureJob(job: Job) {
     job.setOutputKeyClass(classOf[NullWritable])
     job.setOutputValueClass(classOf[Text])
    FileOutputFormat.setOutputPath(job, new Path(path))
    super.configureJob(job)

  }
}

/**
 * If your job will be running against a set of files or globs, this trait configures them.
 */
trait FromPaths extends JobTrait {
  val paths: Seq[String]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    paths.foreach(path => {
      FileInputFormat.addInputPath(job, new Path(path))
    })
    super.configureJob(job)
  }

}

/**
 * Base trait.
 */
trait JobTrait {

  def configure(conf: Configuration) {
    println("JobTrait configure called")
  }

  def configureJob(job: Job) {
    println("JobTrait configure job called")
  }
}


/**
 * Specifies the Table from which you'll be mapping.
 */
trait FromTable[T <: HbaseTable[T, _]] extends JobTrait with NoSpeculativeExecution {

  val fromTable: HbaseTable[T, _]

  val families = Buffer[ColumnFamily[T, _, _, _, _]]()
  val columns = Buffer[Column[T, _, _, _, _]]()


  //Override to add custom attributes to the scanner
  def createScanner = new Scan

  def specifyFamily(family: (T) => ColumnFamily[T, _, _, _, _]) {
    families += family(fromTable.pops)
  }

  def specifyColumn(column: (T) => Column[T, _, _, _, _]) {
    columns += column(fromTable.pops)
  }

  override def configure(conf: Configuration) {
    println("Configuring FromTable")
    conf.set(TableInputFormat.INPUT_TABLE, fromTable.tableName)
    val scanner = createScanner
    scanner.setCacheBlocks(false)
    scanner.setCaching(10000)
    scanner.setMaxVersions(1)

    columns.foreach {
      col =>
        scanner.addColumn(col.familyBytes, col.columnBytes)
    }

    families.foreach {
      family =>
        scanner.addFamily(family.familyBytes)
    }

    val bas = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bas)
    scanner.write(dos)
    conf.set(TableInputFormat.SCAN, Base64.encodeBytes(bas.toByteArray))


    super.configure(conf)
  }

  override def configureJob(job: Job) {
    println("Configuring FromTable Job")
    job.setInputFormatClass(classOf[TableInputFormat])
//    HadoopScalaShim.registerInputFormat(job, classOf[TableInputFormat])
    super.configureJob(job)
  }
}

/**
* Turn of speculative execution.  This is always advisable with HBase MR operations, so this is automatically mixed in to the
* ToTable and FromTable traits.
*/
trait NoSpeculativeExecution extends JobTrait {
  override def configure(conf: Configuration) {
    conf.set("mapred.map.tasks.speculative.execution", "false")
    conf.set("mapred.reduce.tasks.speculative.execution", "false")
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    super.configureJob(job)
  }
}

/**
* Specifies the Table against which you'll be outputting your operation.
*/
trait ToTable[T <: HbaseTable[T, _]] extends JobTrait with NoSpeculativeExecution {
  val toTable: HbaseTable[T, _]

  override def configure(conf: Configuration) {
    println("Configuring ToTable")
    conf.set(GravityTableOutputFormat.OUTPUT_TABLE, toTable.tableName)
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    println("Configuring ToTable Job")
//    job.setOutputFormatClass(classOf[GravityTableOutputFormat[ImmutableBytesWritable]])
   HadoopScalaShim.registerOutputFormat(job, classOf[GravityTableOutputFormat[ImmutableBytesWritable]])
    super.configureJob(job)
  }
}

trait MapperOnly extends JobTrait {
  override def configureJob(job: Job) {
    job.setNumReduceTasks(0)
    super.configureJob(job)
  }
}

trait CustomConfig extends JobTrait {

  val configOptions: Map[String, String]

  override def configure(conf: Configuration) {
    for ((key, value) <- configOptions) {
      println("Setting custom option: " + key + " to value : " + value)
      conf.set(key, value)
    }
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    super.configureJob(job)
  }
}

trait ReuseJVMJob extends JobTrait {
  override def configure(conf: Configuration) {
    conf.setInt("mapred.job.reuse.jvm.num.tasks", -1)
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    super.configureJob(job)
  }
}

trait BigMemoryJob extends JobTrait {
  val mapMemory: Int
  val reduceMemory: Int

  override def configure(conf: Configuration) {

    val memory = mapMemory
    val reducememory = reduceMemory
    conf.set("mapred.map.child.java.opts", "-Xmx" + memory + "m" + " -Xms" + memory + "m")
    //    conf.set("mapred.map.child.java.opts", "-Xmx" + memory + "m")
    conf.set("mapred.reduce.child.java.opts", "-Xmx" + reducememory + "m")
    conf.setInt("mapred.job.map.memory.mb", memory + 2000)
    conf.setInt("mapred.job.reduce.memory.mb", reducememory + 1000)

    super.configure(conf)
  }

  override def configureJob(job: Job) {
    super.configureJob(job)
  }

}

trait TableAnnotationMultithreadedMapperJob[M <: TableAnnotationMapper[T, _], T <: HbaseTable[T, _], TT <: HbaseTable[TT, _]] extends JobTrait with FromTable[T] with ToTable[TT] with MapperOnly {
  val mapper: Class[M]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    //job.setMapperClass(mapper)
    println("Configuring Job in Annotation Mapper")
    job.setMapperClass(classOf[MultithreadedMapper[_,_,_,_]])
    HadoopScalaShim.registerMapper(job, classOf[MultithreadedMapper[_, _, _, _]])
//    MultithreadedMapper.setMapperClass(job,mapper)
    HadoopScalaShim.setMultithreadedMapperClass(job, mapper)
//    MultithreadedMapper.setNumberOfThreads(job, 10)
    super.configureJob(job)
  }
}

class TableAnnotationMapReduceJob[M <: TableReadingMapper[TF, TFK, BytesWritable, BytesWritable], R <: TableWritingReducer[TT, TTK, BytesWritable, BytesWritable], TF <: HbaseTable[TF, TFK], TFK, TT <: HbaseTable[TT, TTK], TTK](name: String, conf: Configuration, val fromTable: TF, val toTable: TT, val mapper: Class[M], val reducer: Class[R]) extends JobBase(name)(conf) with TableMapperJob[M, TF, TFK, BytesWritable, BytesWritable] with TableReducerJob[R, TT, TTK, BytesWritable, BytesWritable] {

  val mapperOutputKey = classOf[BytesWritable]
  val mapperOutputValue = classOf[BytesWritable]

}

trait TableAnnotationMapperJob[M <: TableAnnotationMapper[T, _], T <: HbaseTable[T, _], TT <: HbaseTable[TT, _]] extends JobTrait with FromTable[T] with ToTable[TT] with MapperOnly {
  val mapper: Class[M]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    //job.setMapperClass(mapper)
    println("Configuring Job in Annotation Mapper")
//    job.setMapperClass(mapper)
    HadoopScalaShim.registerMapper(job, mapper)
    super.configureJob(job)
  }
}

abstract class TextFileWritingReducer[MK,MV](name:String="Text File Reducer") extends Reducer[MK, MV, NullWritable, Text] {
  def item(key: MK, values: java.lang.Iterable[MV], counter: (String, String) => Unit, writer: (String) => Unit)

  override def reduce(key: MK, values: java.lang.Iterable[MV], context: Reducer[MK, MV, NullWritable, Text]#Context) {
    def counter(grp: String, itm: String) {context.getCounter(grp, itm).increment(1l)}
    def writer(item: String) {context.write(NullWritable.get(), new Text(item))}

    item(key, values, counter _, writer _)
  }
}

abstract class TableWritingReducer[TF <: HbaseTable[TF, TFK], TFK, MK, MV](name: String, table: TF)(implicit conf: Configuration)
        extends Reducer[MK, MV, NullWritable, Writable] {

  def item(key: MK, values: java.lang.Iterable[MV], counter: (String, String) => Unit, writer: (Writable) => Unit)

  override def reduce(key: MK, values: java.lang.Iterable[MV], context: Reducer[MK, MV, NullWritable, Writable]#Context) {
    def counter(grp: String, itm: String) {context.getCounter(grp, itm).increment(1l)}
    def writer(item: Writable) {context.write(NullWritable.get(), item)}

    item(key, values, counter _, writer _)
  }

}

abstract class StandardMapper[MK, MV] extends Mapper[LongWritable, Text, MK, MV] {
  def mapEvent(key: LongWritable, value: Text, writer: (MK, MV) => Unit, counter: (String, String) => Unit) {

  }

  final override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, MK, MV]#Context) {
    def write(key: MK, value: MV) {
      context.write(key, value)
    }

    def counter(group: String, name: String) {
      context.getCounter(group, name).increment(1l)
    }

    mapEvent(key, value, write _, counter _)
  }
}

/**
 * Reads from a specified Table and writes to MK and MV
 */
abstract class TableReadingMapper[TF <: HbaseTable[TF, TFK], TFK, MK, MV](val table: TF) extends TableMapper[MK, MV] {

  def row(value: QueryResult[TF, TFK], counter: (String, Long) => Unit, writer: (MK, MV) => Unit)

  override def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable, Result, MK, MV]#Context) {
    def counter(name: String, count: Long = 1) {context.getCounter(table.tableName + " Reading Job", name).increment(count)}
    def writer(key: MK, value: MV) {context.write(key, value)}

    val queryResult = new QueryResult[TF, TFK](value, table, table.tableName)
    row(queryResult, counter _, writer _)
  }
}

abstract class TableWritingMapper[TF <: HbaseTable[TF, TFK], TFK](val name: String, val table: TF)(implicit conf: Configuration) extends Mapper[LongWritable, Text, NullWritable, Writable] {
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, NullWritable, Writable]#Context) {
    def counter(key: String) {context.getCounter(name, key).increment(1l)}
    item(value, counter _).foreach(row => context.write(NullWritable.get(), row))
  }

  def item(value: Text, counter: (String) => Unit): Iterable[Writable]
}


/**
 * Reads from a specified Table and writes to that Table or another Table
 */
abstract class TableAnnotationMapper[TF <: HbaseTable[TF, TFK], TFK](val table: TF) extends TableMapper[NullWritable, Writable] {
  def row(value: QueryResult[TF, TFK], counter: (String, Long) => Unit, writer: (Writable) => Unit)

  def onStart() {

  }

  override def setup(context: Mapper[ImmutableBytesWritable, Result, NullWritable, Writable]#Context) {
    onStart()
  }

  override def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable, Result, NullWritable, Writable]#Context) {
    def writer(item: Writable) {context.write(NullWritable.get(), item)}
    def counter(name: String, count: Long = 1) {context.getCounter(table.tableName + " Annotation Job", name).increment(count)}
    val queryResult = new QueryResult[TF, TFK](value, table, table.tableName)
    row(queryResult, counter _, writer _)
  }


}
