/** Licensed to Gravity.com under one
  * or more contributor license agreements. See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership. Gravity.com licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package com.gravity.hbase.mapreduce

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import java.lang.Iterable

import com.google.protobuf.InvalidProtocolBufferException
import com.gravity.hadoop.GravityTableOutputFormat
import com.gravity.hbase.schema.{HbaseTable, _}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Mutation, Result, Scan}
import org.apache.hadoop.hbase.filter.{Filter, FilterList}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{MultiTableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, SequenceFileInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, SequenceFileOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.collection.{mutable, _}
import scala.collection.mutable.Buffer

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


object Settings {

  object None extends NoSettings
  def convertScanToString(scan: Scan): String = {
    val proto : org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def convertStringToScan(base64:String): Scan =  {
    val decoded = Base64.decode(base64)

    var scan : org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan = null
    try {
      scan = org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan.parseFrom(decoded)
    } catch {
      case var4 : InvalidProtocolBufferException =>
        throw new IOException(var4)
    }

    ProtobufUtil.toScan(scan)
  }

}


class NoSettings extends SettingsBase {
  def hithere() = "Hi"
}

class SettingsBase {
  def fromSettings(conf: Configuration) {

  }

  def toSettings(conf: Configuration) {

  }

  def jobNameQualifier = ""
}

///*
//Experimental support for a non declarative constructor for hjobs.
// */
//class HJobND[S <: SettingsBase](name: String) {
//  val tasks = Buffer[HTask[_, _, _, _, S]]()
//
//  def addMR(name:String) {
//    HMapReduceTask(
//      HTaskID(name),
//      HTaskConfigs(),
//      HIO(),
//
//    )
//
//    tasks += task
//  }
//
//}

abstract class HTaskConfigsBase {
  def configs: Seq[HConfigLet]

  def init(settings: SettingsBase) {

  }
}

case class HTaskSettingsConfigs[S <: SettingsBase](configMaker: (S) => Seq[HConfigLet]) extends HTaskConfigsBase {
  var configs: Seq[HConfigLet] = _

  override def init(settings: SettingsBase) {
    configs = configMaker(settings.asInstanceOf[S])
  }
}

/*
Holds a list of configuration objects.  Each object should encapsulate a particular set of configuration options (for example, whether or not to reuse the JVM)
 */
case class HTaskConfigs(configs: HConfigLet*) extends HTaskConfigsBase {
}

/*
The base class for a single configuration object.
 */
abstract class HConfigLet() {
  def configure(job: Job) {

  }
}

case class ReducerCountConf(reducers: Int = 1) extends HConfigLet {
  override def configure(job: Job) {
    job.setNumReduceTasks(reducers)
  }
}

case class SpeculativeExecutionConf(on: Boolean = false) extends HConfigLet {
  override def configure(job: Job) {
    if (!on) {
      job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")
      job.getConfiguration.set("mapreduce.map.speculative", "false")
      job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")
      job.getConfiguration.set("mapreduce.reduce.speculative", "false")
    }
  }
}

case class ReuseJVMConf(reuse: Boolean = true) extends HConfigLet {
  override def configure(job: Job) {
    if (reuse) {
      job.getConfiguration.setInt("mapred.job.reuse.jvm.num.tasks", -1)
      job.getConfiguration.setInt("mapreduce.job.jvm.numtasks", -1)
    }
  }
}

case class BigMemoryConf(mapMemoryMB: Int, reduceMemoryMB: Int, mapBufferMB: Int = 800, reduceBufferMB: Int = 800) extends HConfigLet {
  override def configure(job: Job) {
    val memory = mapMemoryMB
    val reducememory = reduceMemoryMB
    job.getConfiguration.set("mapred.map.child.java.opts", "-Xmx" + memory + "m" + " -Xms" + memory + "m")
    job.getConfiguration.set("mapreduce.map.java.opts", "-Xmx" + memory + "m" + " -Xms" + memory + "m")
    //    conf.set("mapred.map.child.java.opts", "-Xmx" + memory + "m")
    job.getConfiguration.set("mapred.reduce.child.java.opts", "-Xmx" + reducememory + "m")
    job.getConfiguration.set("mapreduce.reduce.java.opts", "-Xmx" + reducememory + "m")
    job.getConfiguration.setInt("mapred.job.map.memory.mb", memory + mapBufferMB)
    job.getConfiguration.setInt("mapreduce.map.memory.mb", memory + mapBufferMB)
    job.getConfiguration.setInt("mapred.job.reduce.memory.mb", reducememory + reduceBufferMB)
    job.getConfiguration.setInt("mapreduce.reduce.memory.mb", reducememory + reduceBufferMB)
  }
}

case class LongRunningJobConf(timeoutInSeconds: Int) extends HConfigLet {
  override def configure(job: Job) {
    job.getConfiguration.setInt("mapred.task.timeout", timeoutInSeconds)
    job.getConfiguration.setInt("mapreduce.task.timeout", timeoutInSeconds)
  }
}

object HJob {
  def job(name: String): HJobBuilder = new HJobBuilder(name)
}


class HJobBuilder(name: String) {
  private val tasks = mutable.Buffer[HTask[_, _, _, _]]()

  def withTask(task: HTask[_, _, _, _]): HJobBuilder = {
    tasks += task
    this
  }

  def build[S <: SettingsBase]: HJob[S] = new HJob[S](name, tasks: _*)

}

/*
object HTask {
  def task(name:String) = new HTaskBuilder(name)
}
class HTaskBuilder(name: String) {
  var previousTaskName : String = _
  var previousTask : HTask[_,_,_,_] = _

  var mapper: HMapper[_,_,_,_] = _
  var reducer : HReducer[_,_,_,_] = _

  var input :HInput = _
  var output : HOutput = _

  var configlets = Buffer[HConfigLet]()

  def mapFromTable[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table:HbaseTable[T,R,RR])(families: FamilyExtractor[T, _, _, _, _]*)(tmapper:FromTableBinaryMapper[T,R,RR]) = {
    input = HTableInput(table.asInstanceOf[T],Families[T](families:_*))
    mapper = tmapper
    this
  }

  def withConfigs(configs:HConfigLet*) = {
    configs.foreach{config=> configlets += config}
    this
  }

  def build = {
    val taskId = if(previousTaskName != null) HTaskID(name,previousTaskName) else if(previousTask != null) HTaskID(name,requiredTask=previousTask) else HTaskID(name)

    val hio = if(input != null && output != null) HIO(input,output) else if(input != null && output == null) HIO(input) else HIO()
    val finalConfigs = HTaskConfigs(configlets:_*)

    if(reducer != null && mapper != null) {
      HMapReduceTask(
        taskId,
        finalConfigs,
        hio,
        mapper,
        reducer
      )
    }else if(mapper != null && reducer == null) {
      HMapTask(
        taskId,
        finalConfigs,
        hio,
        mapper
      )
    }else {
      throw new RuntimeException("Must specify at least a mapper function")
    }
  }

}
*/

case class JobPriority(name: String)

object JobPriorities {
  val VERY_LOW = JobPriority("VERY_LOW")
  val LOW: JobPriority = JobPriority("LOW")
  val NORMAL: JobPriority = JobPriority("NORMAL")
  val HIGH: JobPriority = JobPriority("HIGH")
  val VERY_HIGH: JobPriority = JobPriority("VERY_HIGH")
}

/**
  * A job encompasses a series of tasks that cooperate to build output.  Each task is usually an individual map or map/reduce operation.
  *
  * To use the job, create a class with a parameterless constructor that inherits HJob, and pass the tasks into the constructor as a sequence.
  */
class HJob[S <: SettingsBase](val name: String, tasks: HTask[_, _, _, _]*) {
  type RunResult = (Boolean, Seq[(HTask[_, _, _, _], Job)], mutable.Map[String, DateTime], mutable.Map[String, DateTime])

  def run(settings: S, conf: Configuration, dryRun: Boolean = false, skipToTask: String = null, priority: JobPriority = JobPriorities.NORMAL): RunResult = {
    require(tasks.nonEmpty, "HJob requires at least one task to be defined")
    conf.setStrings("hpaste.jobchain.jobclass", getClass.getName)

    var previousTask: HTask[_, _, _, _] = null

    def taskByName(name: String) = tasks.find(_.taskId.name == name)

    def getPreviousTask(task: HTask[_, _, _, _]) = {
      if (task.taskId.previousTaskName != null) {
        try {
          taskByName(task.taskId.previousTaskName).get
        } catch {
          case ex: Exception =>
            println("WARNING: Task " + task.taskId.name + " specifies previous task " + task.taskId.previousTaskName + " which was not submitted to the job.  Make sure you did this intentionally")
            null
            //            throw new RuntimeException("Task " + task.taskId.name + " requires task " + task.taskId.previousTaskName + " which was not submitted to the job")
        }
      } else {
        if (!tasks.exists(_.taskId.name == task.taskId.requiredTask.taskId.name)) {
          println("WARNING: Task " + task.taskId.name + " specifies previous task " + task.taskId.requiredTask.taskId.name + " which was not submitted to the job.  Make sure you did this intentionally")
          null
          //          throw new RuntimeException("Task " + task.taskId.name + " requires task " + task.taskId.requiredTask.taskId.name + " which has not been submitted to the job")
        } else {
          task.taskId.requiredTask
        }
      }
    }

    for (task <- tasks) {
      if (task.taskId.previousTaskName != null || task.taskId.requiredTask != null) {
        previousTask = getPreviousTask(task)
        task.previousTask = previousTask
        if (task.previousTask != null) {
          previousTask.nextTasks += task

          //If there is a previous HTask, then initialize the input of this task as the output of that task.
          previousTask.hio.output match {
            case value: HRandomSequenceOutput[_, _] if task.hio.input.isInstanceOf[HRandomSequenceInput[_, _]] =>
              task.hio.input.asInstanceOf[HRandomSequenceInput[_, _]].previousPath = value.path
            case _ =>
          }
        }
      }
    }

    var idx = 0


    def makeJob(task: HTask[_, _, _, _]) = {
      val taskConf = new Configuration(conf)
      taskConf.set("mapred.job.priority", priority.name)
      taskConf.set("mapreduce.job.priority", priority.name)
      taskConf.setInt("hpaste.jobchain.mapper.idx", idx)
      taskConf.setInt("hpaste.jobchain.reducer.idx", idx)

      settings.toSettings(taskConf)
      taskConf.set("hpaste.settingsclass", settings.getClass.getName)


      task.configure(taskConf, previousTask)

      val job = task.makeJob(previousTask, settings)
      job.setJarByClass(getClass)
      if (settings.jobNameQualifier.length > 0) {
        job.setJobName(name + " : " + task.taskId.name + " (" + (idx + 1) + " of " + tasks.size + ")" + " [" + settings.jobNameQualifier + "]")
      }
      else {
        job.setJobName(name + " : " + task.taskId.name + " (" + (idx + 1) + " of " + tasks.size + ")")
      }


      previousTask = task
      idx = idx + 1
      job
    }

    def declare(tasks: Seq[HTask[_, _, _, _]], level: String = "\t") {
      tasks.foreach {
        task =>
          println(level + "Task: " + task.taskId.name)
          println(level + "\twill run after " + (if (task.previousTask == null) "nothing" else task.previousTask.taskId.name))
          println(level + "Input: " + task.hio.input)
          println(level + "Output: " + task.hio.output)

          declare(task.nextTasks, level + "\t")
      }
    }

    val taskJobBuffer = mutable.Buffer[(HTask[_, _, _, _], Job)]()
    val taskStartTimes = mutable.Map[String, DateTime]()
    val taskEndTimes = mutable.Map[String, DateTime]()

    def runrecursively(tasks: Seq[HTask[_, _, _, _]]): RunResult = {
      val jobs = tasks.flatMap {
        task =>
          if (skipToTask != null && task.taskId.name != skipToTask) {
            println("Skipping task: " + task.taskId.name + " because we're skipping to : " + skipToTask)
            None
          } else {
            val job = makeJob(task)
            taskJobBuffer.add((task, job))
            Some(job)
          }
      }

      jobs.foreach {
        job =>
          taskStartTimes(job.getJobName) = new DateTime()
          val result = job.waitForCompletion(true)
          taskEndTimes(job.getJobName) = new DateTime()
          if (!job.waitForCompletion(true)) {
            return (false, taskJobBuffer, taskStartTimes, taskEndTimes)
          }
      }

      if (jobs.exists(_.isSuccessful == false)) {
        (false, taskJobBuffer, taskStartTimes, taskEndTimes)
      } else {
        val nextTasks = tasks.flatMap(_.nextTasks)
        if (nextTasks.isEmpty) {
          (true, taskJobBuffer, taskStartTimes, taskEndTimes)
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
    } else {
      (true, taskJobBuffer, taskStartTimes, taskEndTimes)
    }
  }

  def getMapperFunc[MK, MV, MOK, MOV](idx: Int): HMapper[MK, MV, MOK, MOV] = {
    val task = tasks(idx)
    task match {
      case tk: HMapReduceTask[_, _, _, _, _, _] =>
        tk.mapper.asInstanceOf[HMapper[MK, MV, MOK, MOV]]
      case tk: HMapTask[_, _, _, _] =>
        tk.mapper.asInstanceOf[HMapper[MK, MV, MOK, MOV]]
      case _ =>
        throw new RuntimeException("Unable to find mapper for index " + idx)
    }
  }

  def getReducerFunc[MOK, MOV, ROK, ROV](idx: Int): HReducer[MOK, MOV, ROK, ROV] = {
    val task = tasks(idx)
    task match {
      case tk: HMapReduceTask[_, _, _, _, _, _] =>
        tk.reducer.asInstanceOf[HReducer[MOK, MOV, ROK, ROV]]
      case _ =>
        throw new RuntimeException("Unable to find reducer for index " + idx)
    }
  }
}


/**
  * Base class for initializing the input to an HJob
  */
abstract class HInput {
  def init(job: Job, settings: SettingsBase)
}

/**
  * Base class for initializing the output from an HJob
  */
abstract class HOutput {
  def init(job: Job, settings: SettingsBase)
}

case class Columns[T <: HbaseTable[T, _, _]](columns: ColumnExtractor[T]*)

case class Families[T <: HbaseTable[T, _, _]](families: FamilyExtractor[T]*)

case class Filters[T <: HbaseTable[T, _, _]](filters: Filter*)


case class HTableQuery[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R], S <: SettingsBase](query: Query2[T, R, RR], cacheBlocks: Boolean = false, maxVersions: Int = 1, cacheSize: Int = 100) extends HInput {
  override def toString = "Input: From table query"


  override def init(job: Job, settings: SettingsBase) {
    val thisQuery = query
    val scanner = thisQuery.makeScanner(maxVersions, cacheBlocks, cacheSize)
    job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapreduce.map.speculative", "false")

    job.getConfiguration.set(TableInputFormat.SCAN, Settings.convertScanToString(scanner))



    job.getConfiguration.set(TableInputFormat.INPUT_TABLE, thisQuery.table.tableName)
    job.getConfiguration.setInt(TableInputFormat.SCAN_CACHEDROWS, cacheSize)
    job.setInputFormatClass(classOf[TableInputFormat])

  }
}

case class HTableSettingsQuery[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R], S <: SettingsBase](query: (S) => Query2[T, R, RR], cacheBlocks: Boolean = false, maxVersions: Int = 1, cacheSize: Int = 100) extends HInput {
  override def toString = "Input: From table query"


  override def init(job: Job, settings: SettingsBase) {
    val thisQuery = query(settings.asInstanceOf[S])
    val scanner = thisQuery.makeScanner(maxVersions, cacheBlocks, cacheSize)
    job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapreduce.map.speculative", "false")

    job.getConfiguration.set(TableInputFormat.SCAN, Settings.convertScanToString(scanner))



    job.getConfiguration.set(TableInputFormat.INPUT_TABLE, thisQuery.table.tableName)
    job.getConfiguration.setInt(TableInputFormat.SCAN_CACHEDROWS, cacheSize)
    job.setInputFormatClass(classOf[TableInputFormat])

  }
}


/**
  * Initializes input from an HPaste Table
  */
case class HTableInput[T <: HbaseTable[T, _, _]](table: T, families: Families[T] = Families[T](), columns: Columns[T] = Columns[T](), filters: Seq[Filter] = Seq(), scan: Scan = new Scan(), scanCache: Int = 100) extends HInput {


  override def toString: String = "Input: From table: \"" + table.tableName + "\""

  override def init(job: Job, settings: SettingsBase) {
    println("Setting input table to: " + table.tableName)

    //Disabling speculative execution because it is never useful for a table input.
    job.getConfiguration.set("mapred.map.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapreduce.map.speculative", "false")

    val scanner = scan
    scanner.setCacheBlocks(false)
    scanner.setCaching(scanCache)
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

    if (filters.nonEmpty) {
      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
      filters.foreach {filter => filterList.addFilter(filter)}
      scanner.setFilter(filterList)
    }

    job.getConfiguration.set(TableInputFormat.SCAN, Settings.convertScanToString(scanner))



    job.getConfiguration.set(TableInputFormat.INPUT_TABLE, table.tableName)
    job.getConfiguration.setInt(TableInputFormat.SCAN_CACHEDROWS, scanCache)
    job.setInputFormatClass(classOf[TableInputFormat])
  }
}

/**
  * Initializes input from a series of paths.
  */
case class HPathInput(paths: Seq[String]) extends HInput {

  override def toString: String = "Input: Paths: " + paths.mkString("{", ",", "}")

  override def init(job: Job, settings: SettingsBase) {
    paths.foreach(path => {
      FileInputFormat.addInputPath(job, new Path(path))
    })
  }
}



/** Allows the output to be written to multiple tables. Currently the list of tables passed in is
  * purely for documentation.  There is no check in the output that will keep you from writing to other tables.
  */
case class HMultiTableOutput(writeToTransactionLog: Boolean, tables: HbaseTable[_, _, _]*) extends HOutput {
  override def toString: String = "Output: The following tables: " + tables.map(_.tableName).mkString("{", ",", "}")


  override def init(job: Job, settings: SettingsBase) {
    if (!writeToTransactionLog) {
      job.getConfiguration.setBoolean(MultiTableOutputFormat.WAL_PROPERTY, MultiTableOutputFormat.WAL_OFF)
    }
    job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapreduce.reduce.speculative", "false")
    job.setOutputFormatClass(classOf[MultiTableOutputFormat])
  }
}

/**
  * Outputs to an HPaste Table
  */
case class HTableOutput[T <: HbaseTable[T, _, _]](table: T) extends HOutput {

  override def toString: String = "Output: Table: " + table.tableName

  override def init(job: Job, settings: SettingsBase) {
    println("Initializing output table to: " + table.tableName)
    job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")
    job.getConfiguration.set("mapreduce.reduce.speculative", "false")
    job.getConfiguration.set(GravityTableOutputFormat.OUTPUT_TABLE, table.tableName)
    job.setOutputFormatClass(classOf[GravityTableOutputFormat[ImmutableBytesWritable]])
  }
}

/**
  * Outputs to an HDFS directory
  */
case class HPathOutput(path: String) extends HOutput {


  override def toString: String = "Output: File: " + path

  override def init(job: Job, settings: SettingsBase) {
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


  override def toString: String = "Input: Random Sequence File at " + previousPath.toUri.toString

  override def init(job: Job, settings: SettingsBase) {
    FileInputFormat.addInputPath(job, previousPath)

    job.setInputFormatClass(classOf[SequenceFileInputFormat[K, V]])
  }
}

/**
  * Specifies the input to be a sequence file or files given the specified paths.
  * @param paths Paths for files
  * @tparam K Key class of input
  * @tparam V Value class of input
  */
case class HSequenceInput[K,V](paths:Seq[String]) extends HInput {
  override def toString: String = "Input: Sequence Files at: " + paths.mkString("{",",","}")

  override def init(job: Job, settings: SettingsBase) {
    paths.foreach(path => {
      FileInputFormat.addInputPath(job, new Path(path))
    })

    job.setInputFormatClass(classOf[SequenceFileInputFormat[K, V]])
  }

}

/**
  * Output will be to a sequence file or files at the specified path
  * @param seqPath
  * @tparam K
  * @tparam V
  * @tparam S
  */
case class HSequenceSettingsOutput[K : Manifest,V : Manifest, S <: SettingsBase](seqPath:(S)=>String) extends HOutput {
  override def toString = "Output: Sequence File"

  override def init(job: Job, settings: SettingsBase) {
    val path = new Path(seqPath(settings.asInstanceOf[S]))
    FileSystem.get(job.getConfiguration).delete(path, true)

    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[K, V]])
    FileOutputFormat.setOutputPath(job, path)
  }
}


/**
  * Output will be to a sequence file or files at the specified path
  * @param seqPath
  * @tparam K
  * @tparam V
  */
case class HSequenceOutput[K : Manifest,V : Manifest](seqPath:String) extends HOutput {
  override def toString: String = "Output: Sequence File at " + path.toUri.toString

  var path: Path = new Path(seqPath)

  override def init(job: Job, settings: SettingsBase) {
    FileSystem.get(job.getConfiguration).delete(path, true)

    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[K, V]])
    FileOutputFormat.setOutputPath(job, path)
  }
}

/**
  * This is the output from a task in the middle of a job.  It writes to a sequence temp file
  */
case class HRandomSequenceOutput[K, V]() extends HOutput {

  override def toString: String = "Output: Random Sequence File at " + path.toUri.toString

  var path: Path = new Path(genTmpFile)

  override def init(job: Job, settings: SettingsBase) {
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[K, V]])
    FileOutputFormat.setOutputPath(job, path)
  }

}

case class HIO[IK, IV, OK, OV](var input: HInput = HRandomSequenceInput[IK, IV](), var output: HOutput = HRandomSequenceOutput[OK, OV]())


/**
  * This is a single task in an HJob.  It is usually a single hadoop job (an HJob being composed of several).
  */
abstract class HTask[IK, IV, OK, OV](val taskId: HTaskID, val configLets: HTaskConfigsBase = HTaskConfigs(), val hio: HIO[IK, IV, OK, OV] = HIO()) {
  var configuration: Configuration = _


  var previousTask: HTask[_, _, _, _] = _
  val nextTasks: mutable.Buffer[HTask[_, _, _, _]] = mutable.Buffer[HTask[_, _, _, _]]()

  def configure(conf: Configuration, previousTask: HTask[_, _, _, _]) {

    configuration = conf
  }

  def decorateJob(job: Job)

  def makeJob(previousTask: HTask[_, _, _, _], settings: SettingsBase): Job = {

    val job = new Job(configuration)


    hio.input.init(job, settings)
    hio.output.init(job, settings)

    decorateJob(job)

    configLets.init(settings)

    for (config <- configLets.configs) {
      config.configure(job)
    }

    job
  }
}

/** This trait recognizes that mappers and reducers both write key-value pairs, so allows code to be abstracted across mapper and reducer implementations.  It is implemented
  * by HMapper and HReducer, so there should be no need to implement it again.  Instead, write convenience functions that use this trait as a self-type.
  *
  */
trait MRWritable[OK, OV] {
  def write(key: OK, value: OV)
}


trait BinaryWritable {
  this: MRWritable[BytesWritable, BytesWritable] =>

  def write(keyWriter: (PrimitiveOutputStream) => Unit, valueWriter: (PrimitiveOutputStream) => Unit) {
    write(makeWritable(keyWriter), makeWritable(valueWriter))
  }
}

/** Can read reducer input composed of BytesWritable
  *
  */
trait BinaryReadable {
  this: HReducer[BytesWritable, BytesWritable, _, _] =>

  def readKey[T](reader: (PrimitiveInputStream) => T): T = readWritable(key) {reader}

  def perValue(reader: (PrimitiveInputStream) => Unit) {values.foreach {value => readWritable(value)(reader)}}

  def makePerValue[T](reader: (PrimitiveInputStream) => T): scala.Iterable[T] = values.map {value => readWritable(value)(reader)}
}

/** Can write to a specific table
  *
  */
trait ToTableWritable[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]] {
  this: MRWritable[NullWritable, Mutation] =>

  def write(operation: OpBase[T, R]) {
    operation.getOperations.foreach {op => write(NullWritable.get(), op)}
  }
}

/** Can write to multiple tables
  *
  */
trait MultiTableWritable {
  this: MRWritable[ImmutableBytesWritable, Mutation] =>

  val validTableNames: Set[String]

  /** Perform a buffered write to one of the tables specified.  If the table is not in the specified list, will throw an exception saying so.
    */
  def write[T <: HbaseTable[T, R, _], R](operation: OpBase[T, R]) {
    if (validTableNames.contains(operation.table.tableName)) {
      val tableName = new ImmutableBytesWritable(operation.table.tableName.getBytes("UTF-8"))
      operation.getOperations.foreach {op => write(tableName, op)}
    } else {
      throw new RuntimeException("Attempted to write to table: " + operation.table.tableName + ", when allowed tables are : " + validTableNames.mkString("{", ",", "}"))
    }
  }
}

abstract class FromTableMapper[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R], MOK, MOV](table: HbaseTable[T, R, RR], outputKey: Class[MOK], outputValue: Class[MOV])
        extends HMapper[ImmutableBytesWritable, Result, MOK, MOV] {

  def row: RR = table.buildRow(context.getCurrentValue)
}

/** In a map-only job, this covers a table that will write to itself */
abstract class TableSelfMapper[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR]) extends FromTableToTableMapper(table, table)


abstract class FromTableToTableMapper[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R], TT <: HbaseTable[TT, RT, TTRR], RT, TTRR <: HRow[TT, RT]](fromTable: HbaseTable[T, R, RR], toTable: HbaseTable[TT, RT, TTRR])
        extends FromTableMapper[T, R, RR, NullWritable, Mutation](fromTable, classOf[NullWritable], classOf[Mutation]) with ToTableWritable[TT, RT, TTRR] {

}


abstract class FromTableBinaryMapper[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR])
        extends FromTableMapper[T, R, RR, BytesWritable, BytesWritable](table, classOf[BytesWritable], classOf[BytesWritable]) with BinaryWritable

abstract class FromTableBinaryMapperFx[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR])
        extends FromTableMapper[T, R, RR, BytesWritable, BytesWritable](table, classOf[BytesWritable], classOf[BytesWritable]) with BinaryWritable with DelayedInit {

  private var initCode: () => Unit = _

  override def delayedInit(body: => Unit) {
    initCode = () => body
  }

  def map() {
    initCode()
  }
}

abstract class GroupByRow[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR])(grouper: (RR, PrimitiveOutputStream) => Unit) extends FromTableBinaryMapper[T, R, RR](table) {

  def groupBy(row: RR, extractor: PrimitiveOutputStream) {
    grouper(row, extractor)
  }

  final def map() {
    val rr = row

    val bos = new ByteArrayOutputStream()
    val dataOutput = new PrimitiveOutputStream(bos)
    groupBy(rr, dataOutput)
    write(new BytesWritable(bos.toByteArray), makeWritable {vw => vw.writeRow(table, rr)})

  }
}


abstract class GroupingRowMapper[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR]) extends FromTableBinaryMapper[T, R, RR](table) {

  def groupBy(row: RR, extractor: PrimitiveOutputStream)

  final def map() {
    val rr = row

    val bos = new ByteArrayOutputStream()
    val dataOutput = new PrimitiveOutputStream(bos)
    groupBy(rr, dataOutput)
    write(new BytesWritable(bos.toByteArray), makeWritable {vw => vw.writeRow(table, rr)})

  }
}

object MRFx {
  //  def groupBy[T <: HbaseTable[T,R,RR],R,RR <: HRow[T,R]](table:HbaseTable[T,R,RR])(grouper:(RR,PrimitiveOutputStream)=>Unit) =
  //    new GroupingRowMapperFx[T,R,RR](table,grouper){}
}

//
//object MRFx {
//
//  def ftb[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](fromTable: HbaseTable[T, R, RR], mapFx: (RR, FromTableBinaryMapper[T, R, RR]) => Unit) = new FromTableBinaryMapperFx(fromTable, mapFx) {
//  }
//
//  def fromToTableMR[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R], T2 <: HbaseTable[T2, R2, RR2], R2, RR2 <: HRow[T2, R2]](name: String, prev: String, fromTable: HbaseTable[T, R, RR], toTable: HbaseTable[T2, R2, RR2])(mapFx: (RR, FromTableBinaryMapper[T, R, RR]) => Unit)(reduceFx: (BytesWritable, Iterable[BytesWritable], ToTableBinaryReducer[T2, R2, RR2]) => Unit) = {
//    val mrt = new HMapReduceTask(
//      HTaskID(name, prev),
//      HTaskConfigs(),
//      HIO(HTableInput(fromTable.asInstanceOf[T]), HTableOutput(toTable.asInstanceOf[T2])),
//      new FromTableBinaryMapper(fromTable) {
//        def map() {
//          mapFx(row, this)
//        }
//      },
//      new ToTableBinaryReducer(toTable) {
//        def reduce() {
//          reduceFx(key, values, this)
//        }
//      }
//    ) {}
//    mrt
//  }
//}

abstract class BinaryToTableReducer[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR])
        extends ToTableReducer[T, R, RR, BytesWritable, BytesWritable](table) with BinaryReadable

abstract class ToTableReducer[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R], MOK, MOV](table: HbaseTable[T, R, RR])
        extends HReducer[MOK, MOV, NullWritable, Mutation] with ToTableWritable[T, R, RR]

abstract class BinaryToMultiTableReducer(tables: HbaseTable[_, _, _]*) extends ToMultiTableReducer[BytesWritable, BytesWritable](tables: _*)

abstract class ToMultiTableReducer[MOK, MOV](tables: HbaseTable[_, _, _]*) extends HReducer[MOK, MOV, ImmutableBytesWritable, Mutation] with MultiTableWritable {
  val validTableNames: Predef.Set[String] = tables.map(_.tableName).toSet
}

abstract class ToTableBinaryReducer[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR])
        extends HReducer[BytesWritable, BytesWritable, NullWritable, Mutation] with ToTableWritable[T, R, RR] with BinaryReadable

abstract class ToTableBinaryReducerFx[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR])
        extends HReducer[BytesWritable, BytesWritable, NullWritable, Mutation] with ToTableWritable[T, R, RR] with BinaryReadable with DelayedInit {
  private var initCode: () => Unit = _

  override def delayedInit(body: => Unit) {
    initCode = () => body
  }

  def reduce() {
    initCode()
  }

}


abstract class TextToBinaryMapper extends HMapper[LongWritable, Text, BytesWritable, BytesWritable] with BinaryWritable {
}

abstract class BinaryMapper extends HMapper[BytesWritable, BytesWritable, BytesWritable, BytesWritable] with BinaryWritable

abstract class BinaryReducer extends HReducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable] with BinaryWritable with BinaryReadable

abstract class BinaryReducerFx extends BinaryReducer with DelayedInit {
  private var initCode: () => Unit = _

  override def delayedInit(body: => Unit) {
    initCode = () => body
  }

  def reduce() {
    initCode()
  }

}

abstract class BinaryToTextReducerFx extends BinaryToTextReducer with DelayedInit {
  private var initCode: () => Unit = _

  override def delayedInit(body: => Unit) {
    initCode = () => body
  }

  def reduce() {
    initCode()
  }
}

abstract class BinaryToTextReducer extends HReducer[BytesWritable, BytesWritable, NullWritable, Text] with BinaryReadable {
  def writeln(line: String) {write(NullWritable.get(), new Text(line))}

  def writetabs(items: Any*) {
    val sb = new StringBuilder()
    for (item <- items) {
      sb.append(item)
      sb.append("\t")
    }
    write(NullWritable.get(), new Text(sb.toString))
  }


}

//case class FromTableMapper[T <: HbaseTable[T, R], R, MOK, MOV, S <: SettingsBase](table: T, tableMapper: (QueryResult[T, R], HMapContext[ImmutableBytesWritable, Result, MOK, MOV, S]) => Unit)
//        extends MapperFx[ImmutableBytesWritable, Result, MOK, MOV, S]((ctx: HMapContext[ImmutableBytesWritable, Result, MOK, MOV, S]) => {
//          tableMapper(new QueryResult[T, R](ctx.value, table, table.tableName), ctx)
//        })

abstract class HMapper[MK, MV, MOK, MOV] extends Mapper[MK, MV, MOK, MOV] with MRWritable[MOK, MOV] {

  type SettingsClass <: SettingsBase

  var context: Mapper[MK, MV, MOK, MOV]#Context = null

  var settings: SettingsClass = _

  def onStart() {

  }

  final override def setup(context: Mapper[MK, MV, MOK, MOV]#Context) {
    this.context = context
    settings = Class.forName(context.getConfiguration.get("hpaste.settingsclass")).newInstance().asInstanceOf[SettingsClass]
    settings.fromSettings(context.getConfiguration)
    onStart()
  }


  override def cleanup(context: Mapper[MK, MV, MOK, MOV]#Context) {
    cleanup()
  }

  def ctr(message: String, count: Long) {counter(message, count)}

  def ctr(message: String) {counter(message, 1l)}

  def counter(message: String, count: Long) {
    context.getCounter("Custom", message).increment(count)
  }

  def write(key: MOK, value: MOV) {context.write(key, value)}

  def key: MK = context.getCurrentKey

  def value: MV = context.getCurrentValue

  def map()

  def cleanup() {}

  override def map(key: MK, value: MV, context: Mapper[MK, MV, MOK, MOV]#Context) {
    map()
  }
}

abstract class HReducer[MOK, MOV, ROK, ROV] extends Reducer[MOK, MOV, ROK, ROV] with MRWritable[ROK, ROV] {

  type SettingsClass <: SettingsBase

  var context: Reducer[MOK, MOV, ROK, ROV]#Context = null
  var settings: SettingsClass = _

  def counter(message: String, count: Long) {
    context.getCounter("Custom", message).increment(count)
  }

  def ctr(message: String, count: Long) {counter(message, count)}

  def ctr(message: String) {ctr(message, 1l)}

  def write(key: ROK, value: ROV) {context.write(key, value)}


  def key: MOK = context.getCurrentKey

  def values: Iterable[MOV] = context.getValues

  override def setup(context: Reducer[MOK, MOV, ROK, ROV]#Context) {
    this.context = context
    settings = Class.forName(context.getConfiguration.get("hpaste.settingsclass")).newInstance().asInstanceOf[SettingsClass]
    settings.fromSettings(context.getConfiguration)
  }

  override def reduce(key: MOK, values: Iterable[MOV], context: Reducer[MOK, MOV, ROK, ROV]#Context) {
    reduce()
  }

  def reduce()

}

object HMapReduceTask {
  def apply[MK, MV, MOK: Manifest, MOV: Manifest, ROK: Manifest, ROV: Manifest](name: String, mapper: HMapper[MK, MV, MOK, MOV], reducer: HReducer[MOK, MOV, ROK, ROV]): HMapReduceTask[MK, MV, MOK, MOV, ROK, ROV] = {
    HMapReduceTask(
      HTaskID(name),
      HTaskConfigs(),
      HIO(),
      mapper,
      reducer
    )
  }

}


case class HGroupingTask[MK, MV, MOK: Manifest, MOV: Manifest, ROK: Manifest, ROV: Manifest](
                                                                                                    id: HTaskID,
                                                                                                    configs: HTaskConfigsBase = HTaskConfigs(),
                                                                                                    io: HIO[MK, MV, ROK, ROV] = HIO(),
                                                                                                    mapper: HMapper[MK, MV, MOK, MOV],
                                                                                                    reducer: HReducer[MOK, MOV, ROK, ROV],
                                                                                                    partitioner: HPartitioner[MOK, MOV],
                                                                                                    groupingComparator: HBinaryComparator,
                                                                                                    sortComparator: HBinaryComparator
                                                                                                    ) extends HTask[MK,MV,ROK,ROV](id,configs,io) {
  def decorateJob(job: Job) {
      job.setMapperClass(mapper.getClass)
      job.setMapOutputKeyClass(classManifest[MOK].erasure)
      job.setMapOutputValueClass(classManifest[MOV].erasure)
      job.setOutputKeyClass(classManifest[ROK].erasure)
      job.setOutputValueClass(classManifest[ROV].erasure)
      job.setReducerClass(reducer.getClass)
      job.setPartitionerClass(partitioner.getClass)
      job.setGroupingComparatorClass(groupingComparator.getClass)
      job.setSortComparatorClass(sortComparator.getClass)
    }
}

/**
  * An HTask that wraps a standard mapper and reducer function.
  */
case class HMapReduceTask[MK, MV, MOK: Manifest, MOV: Manifest, ROK: Manifest, ROV: Manifest](
                                                                                                     id: HTaskID,
                                                                                                     configs: HTaskConfigsBase = HTaskConfigs(),
                                                                                                     io: HIO[MK, MV, ROK, ROV] = HIO(),
                                                                                                     mapper: HMapper[MK, MV, MOK, MOV],
                                                                                                     reducer: HReducer[MOK, MOV, ROK, ROV],
                                                                                                     combiner: HReducer[MOK, MOV, MOK, MOV] = null)
        extends HTask[MK, MV, ROK, ROV](id, configs, io) {


  def decorateJob(job: Job) {
    job.setMapperClass(mapper.getClass)
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setOutputKeyClass(classManifest[ROK].erasure)
    job.setOutputValueClass(classManifest[ROV].erasure)
    job.setReducerClass(reducer.getClass)
    if (combiner != null) {
      job.setCombinerClass(combiner.getClass)
    }

  }
}


abstract class HBinaryComparator extends RawComparator[BytesWritable] {

  //  /**Override this for the cheapest comparison */
    override def compare(theseBytes: Array[Byte], thisOffset: Int, thisLength: Int, thoseBytes: Array[Byte], thatOffset: Int, thatLength: Int) : Int = {
      val thisInput = new ByteArrayInputStream(theseBytes, thisOffset, thisLength)
      thisInput.skip(4)
      val thatInput = new ByteArrayInputStream(thoseBytes, thatOffset, thatLength)
      thatInput.skip(4)
      compareBytes(new PrimitiveInputStream(thisInput), new PrimitiveInputStream(thatInput))
    }

  override def compare(bw:BytesWritable,bw2:BytesWritable): Int = {
    println("Compare called")
    0
  }

  /** Override for a less cheap comparison */
  def compareBytes(thisReader: PrimitiveInputStream, thatReader: PrimitiveInputStream): Int = {
    println("Compare bytes called")
    0
  }
}

//abstract class HBinaryComparator extends WritableComparator(classOf[BytesWritable], true) {
//  override def compare(a: WritableComparable[_], b: WritableComparable[_]) = {
//    val ab = a.asInstanceOf[BytesWritable]
//    val bb = b.asInstanceOf[BytesWritable]
//    compareBytes(ab, bb)
//  }
//
//  def compareBytes(a: BytesWritable, b: BytesWritable): Int = 0
//}

case class HTaskID(name: String, previousTaskName: String = null, requiredTask: HTask[_, _, _, _] = null)

/**
  * A Task for a mapper-only job
  */
case class HMapTask[MK, MV, MOK: Manifest, MOV: Manifest](id: HTaskID, configs: HTaskConfigs = HTaskConfigs(), io: HIO[MK, MV, MOK, MOV] = HIO(), mapper: HMapper[MK, MV, MOK, MOV]) extends HTask[MK, MV, MOK, MOV](id, configs, io) {
  def decorateJob(job: Job) {
    job.setMapperClass(mapper.getClass)

    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)

    job.setOutputKeyClass(classManifest[MOK].erasure)
    job.setOutputValueClass(classManifest[MOV].erasure)

    job.setNumReduceTasks(0)
  }

}

/**
  * A task for a Mapper / Combiner / Reducer combo
  */
case class HMapCombineReduceTask[MK, MV, MOK: Manifest, MOV: Manifest, ROK, ROV](id: HTaskID, configs: HTaskConfigs = HTaskConfigs(), io: HIO[MK, MV, ROK, ROV] = HIO(), mapper: HMapper[MK, MV, MOK, MOV], combiner: HReducer[MOK, MOV, ROK, ROV], reducer: HReducer[MOK, MOV, ROK, ROV]) extends HTask[MK, MV, ROK, ROV](id, configs, io) {
  def decorateJob(job: Job) {
    job.setMapperClass(mapper.getClass)
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setReducerClass(reducer.getClass)
    job.setCombinerClass(combiner.getClass)
  }
}


abstract class HPartitioner[MOK, MOV]() extends Partitioner[MOK, MOV] {
  override def getPartition(key: MOK, value: MOV, numPartitions: Int): Int = {
    0
  }
}


/**
  * This is the class that gets loaded by Hadoop as a mapper.  It delegates the actual mapper functionality back
  * to the HTask that it represents.
  */
//class HMapper[MK, MV, MOK, MOV, S <: SettingsBase] extends Mapper[MK, MV, MOK, MOV] {
//
//
//  var mapperFx: MapperFxBase[MK, MV, MOK, MOV, S] = _
//
//  var hcontext: HMapContext[MK, MV, MOK, MOV, S] = _
//  var context: Mapper[MK, MV, MOK, MOV]#Context = _
//
//  var job: HJob[S] = _
//
//  def counter(message: String, count: Long) {
//    context.getCounter("Custom", message).increment(count)
//  }
//
//  override def setup(ctx: Mapper[MK, MV, MOK, MOV]#Context) {
//    context = ctx
//
//    job = Class.forName(context.getConfiguration.get("hpaste.jobchain.jobclass")).newInstance.asInstanceOf[HJob[S]]
//    HJobRegistry.job = job
//    mapperFx = job.getMapperFunc(context.getConfiguration.getInt("hpaste.jobchain.mapper.idx", -1))
//
//    hcontext = new HMapContext[MK, MV, MOK, MOV, S](context.getConfiguration, counter, context)
//  }
//
//  override def map(key: MK, value: MV, context: Mapper[MK, MV, MOK, MOV]#Context) {
//    mapperFx.map(hcontext)
//  }
//}

/**
  * This is the actual Reducer that gets loaded by Hadoop.  It delegates the actual reduce functionality back to the
  * HTask that it represents.
  */
//class HReducer[MOK, MOV, ROK, ROV, S <: SettingsBase] extends Reducer[MOK, MOV, ROK, ROV] {
//  var hcontext: HReduceContext[MOK, MOV, ROK, ROV, S] = _
//  var context: Reducer[MOK, MOV, ROK, ROV]#Context = _
//  var reducerFx: ReducerFxBase[MOK, MOV, ROK, ROV, S] = _
//
//  var job: HJob[S] = _
//
//  def counter(message: String, count: Long) {
//    context.getCounter("Custom", message).increment(count)
//  }
//
//  override def setup(ctx: Reducer[MOK, MOV, ROK, ROV]#Context) {
//    context = ctx
//
//    job = Class.forName(context.getConfiguration.get("hpaste.jobchain.jobclass")).newInstance().asInstanceOf[HJob[S]]
//    HJobRegistry.job = job
//
//    reducerFx = job.getReducerFunc(context.getConfiguration.getInt("hpaste.jobchain.reducer.idx", -1))
//
//    hcontext = new HReduceContext[MOK, MOV, ROK, ROV, S](context.getConfiguration, counter, context)
//  }
//
//  override def reduce(key: MOK, values: java.lang.Iterable[MOV], context: Reducer[MOK, MOV, ROK, ROV]#Context) {
//    reducerFx.reduce(hcontext)
//  }
//}

class TableToBinaryMapContext[T <: HbaseTable[T, R, _], R, S <: SettingsBase](table: T, conf: Configuration, counter: (String, Long) => Unit, context: Mapper[ImmutableBytesWritable, Result, BytesWritable, BytesWritable]#Context)
        extends HMapContext[ImmutableBytesWritable, Result, BytesWritable, BytesWritable, S](conf, counter, context) {
  def row: QueryResult[T, R] = new QueryResult[T, R](table.convertResult(context.getCurrentValue), table, table.tableName)
}

/**
  * This is the context object for a Map function.  It gets passed into the mapper function defined in an HTask.
  * It contains simplified functions for writing values and incrementing counters.
  */
class HMapContext[MK, MV, MOK, MOV, S <: SettingsBase](conf: Configuration, counter: (String, Long) => Unit, val context: Mapper[MK, MV, MOK, MOV]#Context) extends HContext[S](conf, counter) {
  def key: MK = context.getCurrentKey

  def value: MV = context.getCurrentValue

  def write(key: MOK, value: MOV) {context.write(key, value)}
}

/**
  * This is the context object for a Reduce function.  It gets passed into the reducer defined in an HTask.
  */
class HReduceContext[MOK, MOV, ROK, ROV, S <: SettingsBase](conf: Configuration, counter: (String, Long) => Unit, val context: Reducer[MOK, MOV, ROK, ROV]#Context) extends HContext[S](conf, counter) {
  def key: MOK = context.getCurrentKey

  def values: Iterable[MOV] = context.getValues

  def write(key: ROK, value: ROV) {context.write(key, value)}
}

class ToTableReduceContext[MOK, MOV, T <: HbaseTable[T, R, _], R, S <: SettingsBase](conf: Configuration, counter: (String, Long) => Unit, context: Reducer[MOK, MOV, NullWritable, Mutation]#Context) extends HReduceContext[MOK, MOV, NullWritable, Mutation, S](conf, counter, context) {
  def write(operation: OpBase[T, R]) {
    operation.getOperations.foreach {op => write(NullWritable.get(), op)}
  }
}

/**
  * Base class for contextual objects.  It handles the business for initializing a context properly.
  */
class HContext[S <: SettingsBase](val conf: Configuration, val counter: (String, Long) => Unit) {
  def apply(message: String, count: Long) {counter(message, count)}

  val settings: S = Class.forName(conf.get("hpaste.settingsclass")).newInstance().asInstanceOf[S]
  settings.fromSettings(conf)

}

