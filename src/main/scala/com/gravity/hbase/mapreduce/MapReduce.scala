package com.gravity.hbase.mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.{Mapper, Reducer, Job}
import org.apache.hadoop.io.{Text, LongWritable, Writable, NullWritable}
import com.gravity.hbase.schema._
import com.gravity.hadoop.{GravityTableOutputFormat, HadoopScalaShim}
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper
import org.apache.hadoop.hbase.client.{Scan, Row, Result}
import org.apache.hadoop.hbase.filter.Filter
import org.apache.hadoop.hbase.mapreduce.{TableMapper, TableInputFormat}
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import java.io.{DataOutputStream, ByteArrayOutputStream, ByteArrayInputStream}
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import collection.mutable.Buffer

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

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


  def run() : Boolean = {
    init()
    job.waitForCompletion(true)

    
  }

}

/**
 * Declares that you want a Reducer.  Will force you to provide the class, and the number thereof.
 * If you specify None for the numReducers it will use the configured default.
 */
trait ReducerJob[M] extends JobTrait {
  val reducer: Class[M]
  val numReducers: Option[Int]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
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
    HadoopScalaShim.registerMapper(job, mapper)
    job.setMapOutputKeyClass(mapperOutputKey)
    job.setMapOutputValueClass(mapperOutputValue)

    super.configureJob(job)
  }


}

trait TableMapperJob[M <: TableReadingMapper[TF,TFK,MK,MV], TF <: HbaseTable[TF,TFK],TFK,MK,MV] extends FromTable[TF] {
  val mapper: Class[M]
  val mapperOutputKey: Class[MK]
  val mapperOutputValue: Class[MV]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    HadoopScalaShim.registerMapper(job, mapper)
    job.setMapOutputKeyClass(mapperOutputKey)
    job.setMapOutputValueClass(mapperOutputValue)
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

  val families = Buffer[ColumnFamily[T,_,_,_,_]]()
  val columns = Buffer[Column[T,_,_,_,_]]()

  def specifyFamily(family: (T)=>ColumnFamily[T,_, _, _, _]) {
    families += family(fromTable.pops)
  }

  def specifyColumn(column: (T)=>Column[T,_,_,_,_]) {
    columns += column(fromTable.pops)
  }

  override def configure(conf: Configuration) {
    println("Configuring FromTable")
    conf.set(TableInputFormat.INPUT_TABLE, fromTable.tableName)

    val scanner = new Scan()
    scanner.setCacheBlocks(false)
    scanner.setCaching(10000)
    scanner.setMaxVersions(1)

        columns.foreach{col=>
          scanner.addColumn(col.familyBytes,col.columnBytes)
        }

        families.foreach{family=>
          scanner.addFamily(family.familyBytes)
        }

    val bas = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bas)
    scanner.write(dos)
    conf.set(TableInputFormat.SCAN,Base64.encodeBytes(bas.toByteArray))


    super.configure(conf)
  }

  override def configureJob(job: Job) {
    println("Configuring FromTable Job")
    HadoopScalaShim.registerInputFormat(job, classOf[TableInputFormat])
    super.configureJob(job)
  }
}

/**
* Turn of speculative execution.  This is always advisable with HBase MR operations, so this is automatically mixed in to the
* ToTable and FromTable traits.
*/
trait NoSpeculativeExecution extends JobTrait {
  override def configure(conf:Configuration) {
    conf.set("mapred.map.tasks.speculative.execution","false")
    conf.set("mapred.reduce.tasks.speculative.execution","false")
    super.configure(conf)
  }

  override def configureJob(job:Job) {
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

trait ReuseJVMJob extends JobTrait {
  override def configure(conf:Configuration) {
    conf.setInt("mapred.job.reuse.jvm.num.tasks", -1)
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    super.configureJob(job)
  }
}

trait BigMemoryJob extends JobTrait {
  val mapMemory : Int
  val reduceMemory : Int

  override def configure(conf: Configuration) {

    val memory = mapMemory
    val reducememory = reduceMemory
    conf.set("mapred.map.child.java.opts", "-Xmx" + memory + "m")
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
    HadoopScalaShim.registerMapper(job, classOf[MultithreadedMapper[_, _, _, _]])
    HadoopScalaShim.setMultithreadedMapperClass(job, mapper)
    MultithreadedMapper.setNumberOfThreads(job, 10)
    super.configureJob(job)
  }
}


trait TableAnnotationMapperJob[M <: TableAnnotationMapper[T, _], T <: HbaseTable[T, _], TT <: HbaseTable[TT, _]] extends JobTrait with FromTable[T] with ToTable[TT] with MapperOnly {
  val mapper: Class[M]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    //job.setMapperClass(mapper)
    println("Configuring Job in Annotation Mapper")
    HadoopScalaShim.registerMapper(job, mapper)
    super.configureJob(job)
  }
}


abstract class TableWritingReducer[TF <: HbaseTable[TF, TFK], TFK, MK, MV](name: String, table: TF)(implicit conf: Configuration)
        extends Reducer[MK, MV, NullWritable, Writable] {

  def item(key: MK, values: java.lang.Iterable[MV], counter: (String, String) => Unit): Iterable[Writable]

  override def reduce(key: MK, values: java.lang.Iterable[MV], context: Reducer[MK, MV, NullWritable, Writable]#Context) {
    def counter(grp: String, itm: String) = context.getCounter(grp, itm).increment(1l)

    item(key, values, counter _).foreach(row => context.write(NullWritable.get, row))
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
abstract class TableReadingMapper[TF <: HbaseTable[TF, TFK], TFK, MK, MV](val table:TF) extends TableMapper[MK, MV] {

  def row(value: QueryResult[TF, TFK], counter: (String,Long) => Unit, writer: (MK,MV) => Unit)

  override def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable, Result, MK, MV]#Context) {
    def counter(name: String,count:Long=1) {context.getCounter(table.tableName + " Reading Job", name).increment(count)}
    def writer(key:MK, value:MV) {context.write(key,value)}

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
  def row(value: QueryResult[TF, TFK], counter: (String,Long) => Unit): Iterable[Writable]

  override def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable, Result, NullWritable, Writable]#Context) {
    def counter(name: String,count:Long=1) {context.getCounter(table.tableName + " Annotation Job", name).increment(count)}
    val queryResult = new QueryResult[TF, TFK](value, table, table.tableName)
    row(queryResult, counter _).foreach(row => context.write(NullWritable.get(), row))
  }


}
