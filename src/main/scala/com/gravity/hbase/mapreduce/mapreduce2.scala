package com.gravity.hbase.mapreduce

import com.gravity.hbase.schema._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{NullWritable, LongWritable, Text, BytesWritable}
import java.lang.Iterable
import com.gravity.hbase.schema.HbaseTable
import org.apache.hadoop.mapreduce.{Job, Reducer, Mapper}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import com.gravity.hadoop.GravityTableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, FileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat}
import scala.collection.JavaConversions._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


/**
* A job encompasses a series of tasks that cooperate to build output.  Each task is usually an individual map or map/reduce operation.
*
* To use the job, create a class with a parameterless constructor that inherits HJob, and pass the tasks into the constructor as a sequence.
*/
class HJob[S <: SettingsBase](input: HInput,output: HOutput, tasks: HTask[_, _, _, _, S]*) {
  def run(settings: S, conf:Configuration) {
    require(tasks.size > 0, "HJob requires at least one task to be defined")
    conf.setStrings("hpaste.jobchain.jobclass", getClass.getName)


    tasks.head.input = input
    tasks.last.output = output


    var previousTask: HTask[_, _, _, _, S] = null

    var idx = 0
    val jobs = for (task <- tasks) yield {
      task.settings = settings
      val taskConf = new Configuration(conf)
      taskConf.setInt("hpaste.jobchain.mapper.idx", idx)
      taskConf.setInt("hpaste.jobchain.reducer.idx", idx)
      task.configure(taskConf, previousTask)

      val job = task.makeJob(previousTask)
      job.setJarByClass(getClass)
      job.setJobName("My Job (" + (idx + 1) + " of " + tasks.size + ")")

      previousTask = task
      idx = idx + 1
      job
    }

    jobs.foreach {_.waitForCompletion(true)}
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
  def init(conf: Configuration, job: Job)
}

/**
* Base class for initializing the output from an HJob
*/
abstract class HOutput {
  def init(conf: Configuration, job: Job)
}

/**
* Initializes input from an HPaste Table
*/
case class HTableInput[T <: HbaseTable[T, R], R](table: T) extends HInput {
  override def init(conf: Configuration, job: Job) {
    conf.set(TableInputFormat.INPUT_TABLE, table.tableName)
    job.setInputFormatClass(classOf[TableInputFormat])
  }
}

/**
* Initializes input from a series of paths.
*/
case class HPathInput(paths: Seq[String]) extends HInput {
  override def init(conf: Configuration, job: Job) {
    paths.foreach(path => {
      FileInputFormat.addInputPath(job, new Path(path))
    })
  }
}

/**
* Outputs to an HPaste Table
*/
case class HTableOutput[T <: HbaseTable[T, R], R](table: T) extends HOutput {
  override def init(conf: Configuration, job: Job) {
    conf.set(GravityTableOutputFormat.OUTPUT_TABLE, table.tableName)
    job.setOutputFormatClass(classOf[GravityTableOutputFormat[ImmutableBytesWritable]])
  }
}

/**
* Outputs to an HDFS directory
*/
case class HPathOutput(path: String) extends HOutput {
  override def init(conf: Configuration, job: Job) {
    FileSystem.get(conf).delete(new Path(path), true)
    FileOutputFormat.setOutputPath(job, new Path(path))
  }
}

/**
* This is the input to a task that is in the middle of a job.
* It reads from the output of the previous task.
*/
case class HRandomSequenceInput[K, V]() extends HInput {
  var previousPath: Path = _

  override def init(conf: Configuration, job: Job) {
    FileInputFormat.addInputPath(job, previousPath)

    job.setInputFormatClass(classOf[SequenceFileInputFormat[K, V]])
  }
}

/**
* This is the output from a task in the middle of a job.  It writes to a sequence temp file
*/
case class HRandomSequenceOutput[K, V]() extends HOutput {
  var path = new Path(genTmpFile)

  override def init(conf: Configuration, job: Job) {
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[K, V]])
    FileOutputFormat.setOutputPath(job, path)
  }

}

/**
* This is a single task in an HJob.  It is usually a single hadoop job (an HJob being composed of several).
*/
abstract class HTask[IK, IV, OK, OV, S <: SettingsBase](var input: HInput = HRandomSequenceInput[IK, IV](), var output: HOutput = HRandomSequenceOutput[OK, OV]()) {
  var configuration: Configuration = _
  var settings: S = _

  def configure(conf: Configuration, previousTask: HTask[_, _, _, _, S]) {
    configuration = conf
  }

  def decorateJob(conf: Configuration, job: Job)

  def makeJob(previousTask: HTask[_, _, _, _, S]) = {

    val job = new Job(configuration)

    //If there is a previous HTask, then initialize the input of this task as the output of that task.
    if (previousTask != null && previousTask.output.isInstanceOf[HRandomSequenceOutput[_, _]] && input.isInstanceOf[HRandomSequenceInput[_, _]]) {
      input.asInstanceOf[HRandomSequenceInput[_, _]].previousPath = previousTask.output.asInstanceOf[HRandomSequenceOutput[_, _]].path
    }

    input.init(configuration, job)
    output.init(configuration, job)

    decorateJob(configuration, job)

    job
  }
}

case class MapperFx[MK,MV,MOK,MOV,S <: SettingsBase](mapper:(HMapContext[MK,MV,MOK,MOV,S]) => Unit)

case class ReducerFx[MOK,MOV,ROK,ROV,S <: SettingsBase](reducer:(HReduceContext[MOK,MOV,ROK,ROV,S]) => Unit)

/**
* An HTask that wraps a standard mapper and reducer function.
*/
case class HMapReduceTask[MK, MV, MOK: Manifest, MOV: Manifest, ROK, ROV, S <: SettingsBase](mapper: MapperFx[MK, MV, MOK, MOV, S], reducer: ReducerFx[MOK, MOV, ROK, ROV, S]) extends HTask[MK, MV, ROK, ROV, S] {

  val mapperClass = classOf[HMapper[MK,MV,MOK,MOV,S]]
  val reducerClass = classOf[HReducer[MOK,MOV,ROK,ROV,S]]


  def decorateJob(conf: Configuration, job: Job) {
    job.setMapperClass(mapperClass)
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
//    job.setOutputKeyClass(classManifest[ROK].erasure)
//    job.setOutputValueClass(classManifest[ROV].erasure)
    job.setOutputKeyClass(classOf[BytesWritable])
    job.setOutputValueClass(classOf[BytesWritable])

    job.setReducerClass(reducerClass)

  }
}

/**
* A Task for a mapper-only job
*/
case class HMapTask[MK, MV, MOK: Manifest, MOV: Manifest, S <: SettingsBase](mapper: MapperFx[MK, MV, MOK, MOV, S]) extends HTask[MK, MV, MOK, MOV, S] {
  def decorateJob(conf: Configuration, job: Job) {
    job.setMapperClass(classOf[HMapper[MK, MV, MOK, MOV, S]])
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
  }

}

/**
* A task for a Mapper / Combiner / Reducer combo
*/
case class HMapCombineReduceTask[MK, MV, MOK: Manifest, MOV: Manifest, ROK, ROV, S <: SettingsBase](mapper: MapperFx[MK, MV, MOK, MOV, S], combiner: ReducerFx[MOK, MOV, ROK, ROV, S], reducer: ReducerFx[MOK, MOV, ROK, ROV, S]) extends HTask[MK, MV, ROK, ROV, S] {
  def decorateJob(conf: Configuration, job: Job) {
    job.setMapperClass(classOf[HMapper[MK, MV, MOK, MOV, S]])
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setReducerClass(classOf[HReducer[MOK, MOV, ROK, ROV, S]])
    job.setCombinerClass(classOf[HReducer[MOK, MOV, ROK, ROV, S]])
  }
}

/**
* This is the class that gets loaded by Hadoop as a mapper.  It delegates the actual mapper functionality back
* to the HTask that it represents.
*/
class HMapper[MK, MV, MOK, MOV, S <: SettingsBase] extends Mapper[MK, MV, MOK, MOV] {

  var mapperFunc: MapperFunc[MK, MV, MOK, MOV, S] = _

  var hcontext: HMapContext[MK, MV, MOK, MOV, S] = _
  var context: Mapper[MK, MV, MOK, MOV]#Context = _

  var job: HJob[S] = _

  def counter(message: String, count: Long) {
    context.getCounter("Custom", message).increment(count)
  }

  override def setup(ctx: Mapper[MK, MV, MOK, MOV]#Context) {
    context = ctx

    job = Class.forName(context.getConfiguration.get("hpaste.jobchain.jobclass")).newInstance.asInstanceOf[HJob[S]]
    mapperFunc = job.getMapperFunc(context.getConfiguration.getInt("hpaste.jobchain.mapper.idx", -1)).mapper

    hcontext = new HMapContext[MK, MV, MOK, MOV, S](context.getConfiguration, counter, context)
  }

  override def map(key: MK, value: MV, context: Mapper[MK, MV, MOK, MOV]#Context) {
    mapperFunc(hcontext)
  }
}

/**
* This is the actual Reducer that gets loaded by Hadoop.  It delegates the actual reduce functionality back to the
* HTask that it represents.
*/
class HReducer[MOK, MOV, ROK, ROV, S <: SettingsBase] extends Reducer[MOK, MOV, ROK, ROV] {
  var hcontext: HReduceContext[MOK, MOV, ROK, ROV, S] = _
  var context: Reducer[MOK, MOV, ROK, ROV]#Context = _
  var reducerFunc: ReducerFunc[MOK, MOV, ROK, ROV, S] = _

  var job: HJob[S] = _

  def counter(message: String, count: Long) {
    context.getCounter("Custom", message).increment(count)
  }

  override def setup(ctx: Reducer[MOK, MOV, ROK, ROV]#Context) {
    context = ctx

    job = Class.forName(context.getConfiguration.get("hpaste.jobchain.jobclass")).newInstance().asInstanceOf[HJob[S]]
    reducerFunc = job.getReducerFunc(context.getConfiguration.getInt("hpaste.jobchain.reducer.idx", -1)).reducer

    hcontext = new HReduceContext[MOK, MOV, ROK, ROV, S](context.getConfiguration, counter, context)
  }

  override def reduce(key: MOK, values: java.lang.Iterable[MOV], context: Reducer[MOK, MOV, ROK, ROV]#Context) {
    reducerFunc(hcontext)
  }
}

/**
* This is the context object for a Map function.  It gets passed into the mapper function defined in an HTask.
* It contains simplified functions for writing values and incrementing counters.
*/
class HMapContext[MK, MV, MOK, MOV, S <: SettingsBase](conf: Configuration, counter: (String, Long) => Unit, context: Mapper[MK, MV, MOK, MOV]#Context) extends HContext(conf, counter) {
  def key = context.getCurrentKey

  def value = context.getCurrentValue

  def write(key: MOK, value: MOV) {context.write(key, value)}
}

/**
* This is the context object for a Reduce function.  It gets passed into the reducer defined in an HTask.
*/
class HReduceContext[MOK, MOV, ROK, ROV, S <: SettingsBase](conf: Configuration, counter: (String, Long) => Unit, context: Reducer[MOK, MOV, ROK, ROV]#Context) extends HContext(conf, counter) {
  def key = context.getCurrentKey

  def values = context.getValues

  def write(key: ROK, value: ROV) {context.write(key, value)}
}

/**
* Base class for contextual objects.  It handles the business for initializing a context properly.
*/
class HContext[S <: SettingsBase](conf: Configuration, val counter: (String, Long) => Unit) {
  def apply(message: String, count: Long) {counter(message, count)}

  val settings: Option[S] = try {
    val s = Class.forName(conf.get("hpaste.settingsclass")).newInstance().asInstanceOf[S]
    s.fromSettings(conf)
    Some(s)
  } catch {
    case ex: Throwable => None
  }

}

