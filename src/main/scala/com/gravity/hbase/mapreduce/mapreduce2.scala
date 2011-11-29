package com.gravity.hbase.mapreduce

import com.gravity.hbase.schema._
import org.apache.hadoop.conf.Configuration
import java.lang.Iterable
import com.gravity.hbase.schema.HbaseTable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import com.gravity.hadoop.GravityTableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat}
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.client.{Scan, Result}
import org.apache.hadoop.hbase.filter.{FilterList, Filter}
import java.io.{DataOutputStream, ByteArrayOutputStream}
import org.apache.hadoop.hbase.util.Base64
import com.gravity.hbase.schema._
import org.apache.hadoop.mapreduce.{Partitioner, Job, Reducer, Mapper}
import scala.collection.mutable.Buffer
import org.apache.hadoop.io._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

object HJobRegistry {
  var job: HJob[_] = null
}

class HJobND[S <: SettingsBase](name: String) {
  val tasks = Buffer[HTask[_, _, _, _, S]]()

  def addTask(task: HTask[_, _, _, _, S], previous: HTask[_, _, _, _, S]) {tasks += task}

}

case class HTaskConfigs(configs: HConfigLet*)

abstract class HConfigLet() {
  def configure(job: Job) {

  }
}

case class ReuseJVMConf(reuse: Boolean = true) extends HConfigLet {
  override def configure(job: Job) {
    if (reuse) {
      job.getConfiguration.setInt("mapred.job.reuse.jvm.num.tasks", -1)
    }
  }
}

case class BigMemoryConf(mapMemoryMB: Int, reduceMemoryMB: Int, mapBufferMB: Int = 800, reduceBufferMB: Int=800) extends HConfigLet {
  override def configure(job: Job) {
    val memory = mapMemoryMB
    val reducememory = reduceMemoryMB
    job.getConfiguration.set("mapred.map.child.java.opts", "-Xmx" + memory + "m" + " -Xms" + memory + "m")
    //    conf.set("mapred.map.child.java.opts", "-Xmx" + memory + "m")
    job.getConfiguration.set("mapred.reduce.child.java.opts", "-Xmx" + reducememory + "m")
    job.getConfiguration.setInt("mapred.job.map.memory.mb", memory + mapBufferMB)
    job.getConfiguration.setInt("mapred.job.reduce.memory.mb", reducememory + reduceBufferMB)
  }
}


/**
* A job encompasses a series of tasks that cooperate to build output.  Each task is usually an individual map or map/reduce operation.
*
* To use the job, create a class with a parameterless constructor that inherits HJob, and pass the tasks into the constructor as a sequence.
*/
class HJob[S <: SettingsBase](val name: String, tasks: HTask[_, _, _, _, S]*) {
  def run(settings: S, conf: Configuration, dryRun: Boolean = false) = {
    require(tasks.size > 0, "HJob requires at least one task to be defined")
    conf.setStrings("hpaste.jobchain.jobclass", getClass.getName)

    var previousTask: HTask[_, _, _, _, S] = null

    def taskByName(name: String) = tasks.find(_.taskId.name == name)

    for (task <- tasks) {
      if (task.taskId.previousTaskName != null) {
        val previousTask = taskByName(task.taskId.previousTaskName).get
        task.previousTask = previousTask
        previousTask.nextTasks += task

        //If there is a previous HTask, then initialize the input of this task as the output of that task.
        if (previousTask.hio.output.isInstanceOf[HRandomSequenceOutput[_, _]] && task.hio.input.isInstanceOf[HRandomSequenceInput[_, _]]) {
          task.hio.input.asInstanceOf[HRandomSequenceInput[_, _]].previousPath = previousTask.hio.output.asInstanceOf[HRandomSequenceOutput[_, _]].path
        }

        //        task.hio.input = previousTask.hio.output
      }
    }

    var idx = 0


    def makeJob(task: HTask[_, _, _, _, S]) = {
      task.settings = settings
      val taskConf = new Configuration(conf)
      taskConf.setInt("hpaste.jobchain.mapper.idx", idx)
      taskConf.setInt("hpaste.jobchain.reducer.idx", idx)

      settings.toSettings(taskConf)
      taskConf.set("hpaste.settingsclass", settings.getClass.getName)


      task.configure(taskConf, previousTask)

      val job = task.makeJob(previousTask)
      job.setJarByClass(getClass)
      job.setJobName(name + " : " + task.taskId.name + " (" + (idx + 1) + " of " + tasks.size + ")")

      previousTask = task
      idx = idx + 1
      job
    }

    def declare(tasks: Seq[HTask[_, _, _, _, S]], level: String = "\t") {
      tasks.map {
        task =>
          println(level + "Task: " + task.taskId.name)
          println(level + "\twill run after " + (if (task.previousTask == null) "nothing" else task.taskId.previousTaskName))
          println(level + "Input: " + task.hio.input)
          println(level + "Output: " + task.hio.output)

          declare(task.nextTasks, level + "\t")
      }
    }

    def runrecursively(tasks: Seq[HTask[_, _, _, _, S]]): Boolean = {
      val jobs = tasks.map {
        task =>
          makeJob(task)
      }

      jobs.foreach {
        job =>
          if (!job.waitForCompletion(true)) {
            return false
          }
        //        job.submit()
      }
      //      var allDone = false
      //      while(!allDone) {
      //        Thread.sleep(500)
      //        if(jobs.exists(_.isComplete == false)) {
      //          allDone = false
      //        }else {
      //          allDone = true
      //        }
      //      }

      if (jobs.exists(_.isSuccessful == false)) {
        false
      } else {
        val nextTasks = tasks.flatMap(_.nextTasks)
        if (nextTasks.size == 0) {
          true
        } else {
          runrecursively(nextTasks)

        }
      }
    }

    val firstTasks = tasks.filter(_.previousTask == null)

    println("Job: " + name + " has " + tasks.size + " tasks")
    declare(firstTasks)

    if (!dryRun) {
      runrecursively(firstTasks)
    }else {
      true
    }
  }

  def getMapperFunc[MK, MV, MOK, MOV](idx: Int) = {
    val task = tasks(idx)
    if (task.isInstanceOf[HMapReduceTask[MK, MV, MOK, MOV, _, _, S]]) {
      val tk = task.asInstanceOf[HMapReduceTask[MK, MV, MOK, MOV, _, _, S]]
      tk.mapper
    }
    else if (task.isInstanceOf[HMapTask[MK, MV, MOK, MOV, S]]) {
      val tk = task.asInstanceOf[HMapTask[MK, MV, MOK, MOV, S]]
      tk.mapper
    } else {
      throw new RuntimeException("Unable to find mapper for index " + idx)
    }
  }

  def getReducerFunc[MOK, MOV, ROK, ROV](idx: Int) = {
    val task = tasks(idx)
    if (task.isInstanceOf[HMapReduceTask[_, _, MOK, MOV, ROK, ROV, S]]) {
      val tk = task.asInstanceOf[HMapReduceTask[_, _, MOK, MOV, ROK, ROV, S]]
      tk.reducer
    } else {
      throw new RuntimeException("Unable to find reducer for index " + idx)
    }
  }
}


/**
* Base class for initializing the input to an HJob
*/
abstract class HInput {
  def init(job: Job)
}

/**
* Base class for initializing the output from an HJob
*/
abstract class HOutput {
  def init(job: Job)
}

case class Columns[T <: HbaseTable[T, _]](columns: ColumnExtractor[T, _, _, _, _]*)

case class Families[T <: HbaseTable[T, _]](families: FamilyExtractor[T, _, _, _, _]*)

case class Filters[T <: HbaseTable[T, _]](filters: Filter*)

/**
* Initializes input from an HPaste Table
*/
case class HTableInput[T <: HbaseTable[T, _]](table: T, families: Families[T] = Families[T](), columns: Columns[T] = Columns[T](), filters: Seq[Filter] = Seq(), scan: Scan = new Scan()) extends HInput {


  override def toString = "Input: From table: \"" + table.tableName + "\""

  override def init(job: Job) {
    println("Setting input table to: " + table.tableName)

    val scanner = scan
    scanner.setCacheBlocks(false)
    scanner.setCaching(100)
    scanner.setMaxVersions(1)

    columns.columns.foreach {
      col =>
        val column = col(table)
        scanner.addColumn(column.familyBytes, column.columnBytes)
    }
    families.families.foreach {
      fam =>
        val family = fam(table)
        scanner.addFamily(family.familyBytes)
    }

    if (filters.size > 0) {
      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
      filters.foreach {filter => filterList.addFilter(filter)}
      scanner.setFilter(filterList)
    }

    val bas = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bas)
    scanner.write(dos)
    job.getConfiguration.set(TableInputFormat.SCAN, Base64.encodeBytes(bas.toByteArray))



    job.getConfiguration.set(TableInputFormat.INPUT_TABLE, table.tableName)
    job.setInputFormatClass(classOf[TableInputFormat])
  }
}

/**
* Initializes input from a series of paths.
*/
case class HPathInput(paths: Seq[String]) extends HInput {

  override def toString = "Input: Paths: " + paths.mkString("{", ",", "}")

  override def init(job: Job) {
    paths.foreach(path => {
      FileInputFormat.addInputPath(job, new Path(path))
    })
  }
}

/**
* Outputs to an HPaste Table
*/
case class HTableOutput[T <: HbaseTable[T, _]](table: T) extends HOutput {

  override def toString = "Output: Table: " + table.tableName

  override def init(job: Job) {
    println("Initializing output table to: " + table.tableName)
    job.getConfiguration.set(GravityTableOutputFormat.OUTPUT_TABLE, table.tableName)
    job.setOutputFormatClass(classOf[GravityTableOutputFormat[ImmutableBytesWritable]])
  }
}

/**
* Outputs to an HDFS directory
*/
case class HPathOutput(path: String) extends HOutput {


  override def toString = "Output: File: " + path

  override def init(job: Job) {
    FileSystem.get(job.getConfiguration).delete(new Path(path), true)
    FileOutputFormat.setOutputPath(job, new Path(path))
  }
}

/**
* This is the input to a task that is in the middle of a job.
* It reads from the output of the previous task.
*/
case class HRandomSequenceInput[K, V]() extends HInput {
  var previousPath: Path = _


  override def toString = "Input: Random Sequence File at " + previousPath.toUri.toString

  override def init(job: Job) {
    FileInputFormat.addInputPath(job, previousPath)

    job.setInputFormatClass(classOf[SequenceFileInputFormat[K, V]])
  }
}

/**
* This is the output from a task in the middle of a job.  It writes to a sequence temp file
*/
case class HRandomSequenceOutput[K, V]() extends HOutput {

  override def toString = "Output: Random Sequence File at " + path.toUri.toString

  var path = new Path(genTmpFile)

  override def init(job: Job) {
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[K, V]])
    FileOutputFormat.setOutputPath(job, path)
  }

}

case class HIO[IK, IV, OK, OV, S <: SettingsBase](var input: HInput = HRandomSequenceInput[IK, IV](), var output: HOutput = HRandomSequenceOutput[OK, OV]())


/**
* This is a single task in an HJob.  It is usually a single hadoop job (an HJob being composed of several).
*/
abstract class HTask[IK, IV, OK, OV, S <: SettingsBase](val taskId: HTaskID, val configLets: HTaskConfigs = HTaskConfigs(), val hio: HIO[IK, IV, OK, OV, S] = HIO()) {
  var configuration: Configuration = _
  var settings: S = _


  var previousTask: HTask[_, _, _, _, S] = _
  val nextTasks = Buffer[HTask[_, _, _, _, S]]()

  def configure(conf: Configuration, previousTask: HTask[_, _, _, _, S]) {

    configuration = conf
  }

  def decorateJob(job: Job)

  def makeJob(previousTask: HTask[_, _, _, _, S]) = {

    val job = new Job(configuration)


    hio.input.init(job)
    hio.output.init(job)

    decorateJob(job)

    for (config <- configLets.configs) {
      config.configure(job)
    }

    job
  }
}


abstract class MapperFxBase[MK, MV, MOK, MOV, S <: SettingsBase] {
  def map(hContext: HMapContext[MK, MV, MOK, MOV, S]) {

  }
}

abstract class ReducerFxBase[MOK, MOV, ROK, ROV, S <: SettingsBase] {
  def reduce(hContext: HReduceContext[MOK, MOV, ROK, ROV, S]) {

  }
}

case class MapperFx[MK, MV, MOK, MOV, S <: SettingsBase](mapper: (HMapContext[MK, MV, MOK, MOV, S]) => Unit) extends MapperFxBase[MK, MV, MOK, MOV, S] {
  override def map(hContext: HMapContext[MK, MV, MOK, MOV, S]) {
    mapper(hContext)
  }
}

case class ReducerFx[MOK, MOV, ROK, ROV, S <: SettingsBase](reducer: (HReduceContext[MOK, MOV, ROK, ROV, S]) => Unit) extends ReducerFxBase[MOK, MOV, ROK, ROV, S] {
  override def reduce(hContext: HReduceContext[MOK, MOV, ROK, ROV, S]) {
    reducer(hContext)
  }
}


case class FromTableBinaryMapper[T <: HbaseTable[T,R],R, S <: SettingsBase](table:T, tableMapper: (TableToBinaryMapContext[T,R,S])=>Unit)
  extends MapperFxBase[ImmutableBytesWritable,Result, BytesWritable,BytesWritable,S] {

  var fromTBMContext : TableToBinaryMapContext[T,R,S] = _

  override def map(hContext: HMapContext[ImmutableBytesWritable,Result,BytesWritable,BytesWritable,S]) {
    if(fromTBMContext == null) {
      fromTBMContext = new TableToBinaryMapContext[T,R,S](table,hContext.conf,hContext.counter,hContext.context)
    }
    tableMapper(fromTBMContext)
  }
}

case class FromTableMapper[T <: HbaseTable[T, R], R, MOK, MOV, S <: SettingsBase](table: T, tableMapper: (QueryResult[T, R], HMapContext[ImmutableBytesWritable, Result, MOK, MOV, S]) => Unit)
        extends MapperFxBase[ImmutableBytesWritable, Result, MOK, MOV, S] {

  override def map(hContext: HMapContext[ImmutableBytesWritable, Result, MOK, MOV, S]) {
    tableMapper(new QueryResult[T, R](hContext.value, table, table.tableName), hContext)
  }

}


case class ToTableReducer[T <: HbaseTable[T,R], R, MOK, MOV, S <: SettingsBase](table:T, tableReducer: ToTableReduceContext[MOK,MOV,T,R,S] => Unit)
  extends ReducerFxBase[MOK,MOV,NullWritable,Writable,S] {

  var toTableContext : ToTableReduceContext[MOK,MOV,T,R,S] = _

  override def reduce(hContext: HReduceContext[MOK, MOV, NullWritable, Writable, S]) {
    if(toTableContext == null) {
      toTableContext = new ToTableReduceContext[MOK,MOV,T,R,S](hContext.conf, hContext.counter, hContext.context)
    }
    tableReducer(toTableContext)
  }
}


//case class FromTableMapper[T <: HbaseTable[T, R], R, MOK, MOV, S <: SettingsBase](table: T, tableMapper: (QueryResult[T, R], HMapContext[ImmutableBytesWritable, Result, MOK, MOV, S]) => Unit)
//        extends MapperFx[ImmutableBytesWritable, Result, MOK, MOV, S]((ctx: HMapContext[ImmutableBytesWritable, Result, MOK, MOV, S]) => {
//          tableMapper(new QueryResult[T, R](ctx.value, table, table.tableName), ctx)
//        })


/**
* An HTask that wraps a standard mapper and reducer function.
*/
case class HMapReduceTask[MK, MV, MOK: Manifest, MOV: Manifest, ROK: Manifest, ROV: Manifest, S <: SettingsBase](id: HTaskID, configs: HTaskConfigs = HTaskConfigs(), io: HIO[MK, MV, ROK, ROV, S] = HIO(), mapper: MapperFxBase[MK, MV, MOK, MOV, S], reducer: ReducerFxBase[MOK, MOV, ROK, ROV, S]) extends HTask[MK, MV, ROK, ROV, S](id, configs, io) {

  val mapperClass = classOf[HMapper[MK, MV, MOK, MOV, S]]
  val reducerClass = classOf[HReducer[MOK, MOV, ROK, ROV, S]]
  val partitionersClass = classOf[HPartitioner[MOK, MOV]]


  def decorateJob(job: Job) {
    job.setMapperClass(mapperClass)
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setOutputKeyClass(classManifest[ROK].erasure)
    job.setOutputValueClass(classManifest[ROV].erasure)

    job.setReducerClass(reducerClass)

  }
}

case class HTaskID(name: String, previousTaskName: String = null)

/**
* A Task for a mapper-only job
*/
case class HMapTask[MK, MV, MOK: Manifest, MOV: Manifest, S <: SettingsBase](id: HTaskID, configs: HTaskConfigs = HTaskConfigs(), io: HIO[MK, MV, MOK, MOV, S] = HIO(), mapper: MapperFxBase[MK, MV, MOK, MOV, S]) extends HTask[MK, MV, MOK, MOV, S](id, configs, io) {
  def decorateJob(job: Job) {
    job.setMapperClass(classOf[HMapper[MK, MV, MOK, MOV, S]])
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setNumReduceTasks(0)
  }

}

/**
* A task for a Mapper / Combiner / Reducer combo
*/
case class HMapCombineReduceTask[MK, MV, MOK: Manifest, MOV: Manifest, ROK, ROV, S <: SettingsBase](id: HTaskID, configs: HTaskConfigs = HTaskConfigs(), io: HIO[MK, MV, ROK, ROV, S] = HIO(), mapper: MapperFxBase[MK, MV, MOK, MOV, S], combiner: ReducerFxBase[MOK, MOV, ROK, ROV, S], reducer: ReducerFxBase[MOK, MOV, ROK, ROV, S]) extends HTask[MK, MV, ROK, ROV, S](id, configs, io) {
  def decorateJob(job: Job) {
    job.setMapperClass(classOf[HMapper[MK, MV, MOK, MOV, S]])
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setReducerClass(classOf[HReducer[MOK, MOV, ROK, ROV, S]])
    job.setCombinerClass(classOf[HReducer[MOK, MOV, ROK, ROV, S]])
  }
}

class HPartitioner[MOK, MOV] extends Partitioner[MOK, MOV] {

  override def getPartition(key: MOK, value: MOV, numPartitions: Int): Int = {
    3
    //    val job = HJobRegistry.job
  }
}

/**
* This is the class that gets loaded by Hadoop as a mapper.  It delegates the actual mapper functionality back
* to the HTask that it represents.
*/
class HMapper[MK, MV, MOK, MOV, S <: SettingsBase] extends Mapper[MK, MV, MOK, MOV] {


  var mapperFx: MapperFxBase[MK, MV, MOK, MOV, S] = _

  var hcontext: HMapContext[MK, MV, MOK, MOV, S] = _
  var context: Mapper[MK, MV, MOK, MOV]#Context = _

  var job: HJob[S] = _

  def counter(message: String, count: Long) {
    context.getCounter("Custom", message).increment(count)
  }

  override def setup(ctx: Mapper[MK, MV, MOK, MOV]#Context) {
    context = ctx

    job = Class.forName(context.getConfiguration.get("hpaste.jobchain.jobclass")).newInstance.asInstanceOf[HJob[S]]
    HJobRegistry.job = job
    mapperFx = job.getMapperFunc(context.getConfiguration.getInt("hpaste.jobchain.mapper.idx", -1))

    hcontext = new HMapContext[MK, MV, MOK, MOV, S](context.getConfiguration, counter, context)
  }

  override def map(key: MK, value: MV, context: Mapper[MK, MV, MOK, MOV]#Context) {
    mapperFx.map(hcontext)
  }
}

/**
* This is the actual Reducer that gets loaded by Hadoop.  It delegates the actual reduce functionality back to the
* HTask that it represents.
*/
class HReducer[MOK, MOV, ROK, ROV, S <: SettingsBase] extends Reducer[MOK, MOV, ROK, ROV] {
  var hcontext: HReduceContext[MOK, MOV, ROK, ROV, S] = _
  var context: Reducer[MOK, MOV, ROK, ROV]#Context = _
  var reducerFx: ReducerFxBase[MOK, MOV, ROK, ROV, S] = _

  var job: HJob[S] = _

  def counter(message: String, count: Long) {
    context.getCounter("Custom", message).increment(count)
  }

  override def setup(ctx: Reducer[MOK, MOV, ROK, ROV]#Context) {
    context = ctx

    job = Class.forName(context.getConfiguration.get("hpaste.jobchain.jobclass")).newInstance().asInstanceOf[HJob[S]]
    HJobRegistry.job = job

    reducerFx = job.getReducerFunc(context.getConfiguration.getInt("hpaste.jobchain.reducer.idx", -1))

    hcontext = new HReduceContext[MOK, MOV, ROK, ROV, S](context.getConfiguration, counter, context)
  }

  override def reduce(key: MOK, values: java.lang.Iterable[MOV], context: Reducer[MOK, MOV, ROK, ROV]#Context) {
    reducerFx.reduce(hcontext)
  }
}

class TableToBinaryMapContext[T <: HbaseTable[T,R],R, S <: SettingsBase](table:T, conf:Configuration, counter: (String,Long)=>Unit, context: Mapper[ImmutableBytesWritable,Result,BytesWritable,BytesWritable]#Context)
  extends HMapContext[ImmutableBytesWritable,Result,BytesWritable,BytesWritable,S](conf, counter, context)
{
  def row = new QueryResult[T,R](context.getCurrentValue,table,table.tableName)
}

/**
* This is the context object for a Map function.  It gets passed into the mapper function defined in an HTask.
* It contains simplified functions for writing values and incrementing counters.
*/
class HMapContext[MK, MV, MOK, MOV, S <: SettingsBase](conf: Configuration, counter: (String, Long) => Unit, val context: Mapper[MK, MV, MOK, MOV]#Context) extends HContext[S](conf, counter) {
  def key = context.getCurrentKey

  def value = context.getCurrentValue

  def write(key: MOK, value: MOV) {context.write(key, value)}
}

/**
* This is the context object for a Reduce function.  It gets passed into the reducer defined in an HTask.
*/
class HReduceContext[MOK, MOV, ROK, ROV, S <: SettingsBase](conf: Configuration, counter: (String, Long) => Unit, val context: Reducer[MOK, MOV, ROK, ROV]#Context) extends HContext[S](conf, counter) {
  def key = context.getCurrentKey

  def values = context.getValues

  def write(key: ROK, value: ROV) {context.write(key, value)}
}

class ToTableReduceContext[MOK, MOV, T <: HbaseTable[T,R],R, S <: SettingsBase](conf:Configuration, counter: (String,Long)=>Unit, context: Reducer[MOK,MOV,NullWritable,Writable]#Context) extends HReduceContext[MOK,MOV,NullWritable,Writable,S](conf, counter, context) {
  def write(operation: OpBase[T,R]) {
    operation.getOperations.foreach{op=>write(NullWritable.get(),op)}
  }
}

/**
* Base class for contextual objects.  It handles the business for initializing a context properly.
*/
class HContext[S <: SettingsBase](val conf: Configuration, val counter: (String, Long) => Unit) {
  def apply(message: String, count: Long) {counter(message, count)}

  val settings = Class.forName(conf.get("hpaste.settingsclass")).newInstance().asInstanceOf[S]
  settings.fromSettings(conf)

}

