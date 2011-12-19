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
import org.apache.hadoop.hbase.util.Base64
import com.gravity.hbase.schema._
import org.apache.hadoop.mapreduce.{Partitioner, Job, Reducer, Mapper}
import scala.collection.mutable.Buffer
import org.apache.hadoop.io._
import java.io.{DataInputStream, ByteArrayInputStream, ByteArrayOutputStream}

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/*
Where the currently running job gets registered in the mapper or reducer process.  This is how mapper/reducer functions get wired up.
 */
object HJobRegistry {
  var job: HJob[_] = null


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

/*
Holds a list of configuration objects.  Each object should encapsulate a particular set of configuration options (for example, whether or not to reuse the JVM)
 */
case class HTaskConfigs(configs: HConfigLet*)

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
      job.getConfiguration.set("mapred.reduce.tasks.speculative.execution", "false")
    }
  }
}

case class ReuseJVMConf(reuse: Boolean = true) extends HConfigLet {
  override def configure(job: Job) {
    if (reuse) {
      job.getConfiguration.setInt("mapred.job.reuse.jvm.num.tasks", -1)
    }
  }
}

case class BigMemoryConf(mapMemoryMB: Int, reduceMemoryMB: Int, mapBufferMB: Int = 800, reduceBufferMB: Int = 800) extends HConfigLet {
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
    } else {
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

case class Columns[T <: HbaseTable[T, _, _]](columns: ColumnExtractor[T, _, _, _, _]*)

case class Families[T <: HbaseTable[T, _, _]](families: FamilyExtractor[T, _, _, _, _]*)

case class Filters[T <: HbaseTable[T, _, _]](filters: Filter*)

/**
  * Initializes input from an HPaste Table
  */
case class HTableInput[T <: HbaseTable[T, _, _]](table: T, families: Families[T] = Families[T](), columns: Columns[T] = Columns[T](), filters: Seq[Filter] = Seq(), scan: Scan = new Scan(), scanCache: Int = 100) extends HInput {


  override def toString = "Input: From table: \"" + table.tableName + "\""

  override def init(job: Job) {
    println("Setting input table to: " + table.tableName)

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

    if (filters.size > 0) {
      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL)
      filters.foreach {filter => filterList.addFilter(filter)}
      scanner.setFilter(filterList)
    }

    val bas = new ByteArrayOutputStream()
    val dos = new PrimitiveOutputStream(bas)
    scanner.write(dos)
    job.getConfiguration.set(TableInputFormat.SCAN, Base64.encodeBytes(bas.toByteArray))



    job.getConfiguration.set(TableInputFormat.INPUT_TABLE, table.tableName)
    job.getConfiguration.setInt(TableInputFormat.SCAN_CACHEDROWS, scanCache)
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
case class HTableOutput[T <: HbaseTable[T, _, _]](table: T) extends HOutput {

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


trait MRWritable[OK, OV] {
  def write(key: OK, value: OV)
}


trait BinaryWritable {
  this: MRWritable[BytesWritable, BytesWritable] =>

  def write(keyWriter: (PrimitiveOutputStream) => Unit, valueWriter: (PrimitiveOutputStream) => Unit) {
    write(makeWritable(keyWriter), makeWritable(valueWriter))
  }
}

trait BinaryReadable {
  this : HReducer[BytesWritable,BytesWritable,_,_] =>

  def readKey[T](reader: (PrimitiveInputStream) => T) = readWritable(key) {reader}

    def perValue(reader: (PrimitiveInputStream) => Unit) {values.foreach {value => readWritable(value)(reader)}}

    def makePerValue[T](reader: (PrimitiveInputStream) => T) = values.map {value => readWritable(value)(reader)}
}

trait ToTableWritable[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]] {
  this: MRWritable[NullWritable, Writable] =>

  def write(operation: OpBase[T, R]) {
    operation.getOperations.foreach {op => write(NullWritable.get(), op)}
  }
}

abstract class FromTableMapper[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R], MOK, MOV](table: HbaseTable[T, R, RR], outputKey: Class[MOK], outputValue: Class[MOV])
        extends HMapper[ImmutableBytesWritable, Result, MOK, MOV] {

  def row = table.buildRow(context.getCurrentValue)
}

abstract class FromTableToTableMapper[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R], TT <: HbaseTable[TT, RT, TTRR], RT, TTRR <: HRow[TT, RT]](fromTable: HbaseTable[T, R, RR], toTable: HbaseTable[TT, RT, TTRR])
        extends FromTableMapper[T, R, RR, NullWritable, Writable](fromTable, classOf[NullWritable], classOf[Writable]) with ToTableWritable[TT, RT, TTRR]


abstract class FromTableBinaryMapper[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR])
        extends FromTableMapper[T, R, RR, BytesWritable, BytesWritable](table, classOf[BytesWritable], classOf[BytesWritable]) with BinaryWritable


abstract class ToTableReducer[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R], MOK, MOV](table: HbaseTable[T, R, RR])
        extends HReducer[MOK, MOV, NullWritable, Writable] with ToTableWritable[T, R, RR]

abstract class ToTableBinaryReducer[T <: HbaseTable[T, R, RR], R, RR <: HRow[T, R]](table: HbaseTable[T, R, RR])
        extends HReducer[BytesWritable, BytesWritable, NullWritable, Writable] with ToTableWritable[T, R, RR] with BinaryReadable

abstract class BinaryMapper extends HMapper[BytesWritable, BytesWritable, BytesWritable, BytesWritable] with BinaryWritable

abstract class BinaryReducer extends HReducer[BytesWritable, BytesWritable, BytesWritable, BytesWritable] with BinaryWritable with BinaryReadable

abstract class BinaryToTextReducer extends HReducer[BytesWritable, BytesWritable, NullWritable, Text] with BinaryReadable {
  def writeln(line: String) {write(NullWritable.get(), new Text(line))}

  def writetabs(items: Any*) {
    val txt = new Text()
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

  override def setup(context: Mapper[MK, MV, MOK, MOV]#Context) {
    this.context = context
    settings = Class.forName(context.getConfiguration.get("hpaste.settingsclass")).newInstance().asInstanceOf[SettingsClass]
    settings.fromSettings(context.getConfiguration)
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

  def key = context.getCurrentKey

  def value = context.getCurrentValue

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


  def key = context.getCurrentKey

  def values = context.getValues

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
  def apply[MK, MV, MOK: Manifest, MOV: Manifest, ROK: Manifest, ROV: Manifest, S <: SettingsBase](name: String, mapper: HMapper[MK, MV, MOK, MOV], reducer: HReducer[MOK, MOV, ROK, ROV]): HMapReduceTask[MK, MV, MOK, MOV, ROK, ROV, S] = {
    HMapReduceTask(
      HTaskID(name),
      HTaskConfigs(),
      HIO(),
      mapper,
      reducer
    )
  }

}

/**
  * An HTask that wraps a standard mapper and reducer function.
  */
case class HMapReduceTask[MK, MV, MOK: Manifest, MOV: Manifest, ROK: Manifest, ROV: Manifest, S <: SettingsBase](
                                                                                                                        id: HTaskID,
                                                                                                                        configs: HTaskConfigs = HTaskConfigs(),
                                                                                                                        io: HIO[MK, MV, ROK, ROV, S] = HIO(),
                                                                                                                        mapper: HMapper[MK, MV, MOK, MOV],
                                                                                                                        reducer: HReducer[MOK, MOV, ROK, ROV],
                                                                                                                        partitioner: HPartitioner[MOK, MOV] = null,
                                                                                                                        groupingComparator: HBinaryComparator = null)
        extends HTask[MK, MV, ROK, ROV, S](id, configs, io) {


  def decorateJob(job: Job) {
    job.setMapperClass(mapper.getClass)
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setOutputKeyClass(classManifest[ROK].erasure)
    job.setOutputValueClass(classManifest[ROV].erasure)
    job.setReducerClass(reducer.getClass)
    if (partitioner != null) {
      job.setPartitionerClass(partitioner.getClass)
    }
    if (groupingComparator != null) {
      job.setGroupingComparatorClass(groupingComparator.getClass)
    }
    //    job.setGroupingComparatorClass()
    //    job.setSortComparatorClass()

  }
}

abstract class HBinaryComparator extends WritableComparator(classOf[BytesWritable],true) {

//  /**Override this for the cheapest comparison */
//  override def compare(theseBytes: Array[Byte], thisOffset: Int, thisLength: Int, thoseBytes: Array[Byte], thatOffset: Int, thatLength: Int) : Int = {
//    val thisInput = new ByteArrayInputStream(theseBytes, thisOffset, thisLength)
//    val thatInput = new ByteArrayInputStream(thoseBytes, thatOffset, thatLength)
//    compareBytes(new PrimitiveInputStream(thisInput), new PrimitiveInputStream(thatInput))
//  }

  def compare(thisItem:BytesWritable, thatItem:BytesWritable) : Int = {
    compareBytes(new PrimitiveInputStream(new ByteArrayInputStream(thisItem.getBytes)), new PrimitiveInputStream(new ByteArrayInputStream(thatItem.getBytes)))
  }

  /**Override for a less cheap comparison */
  def compareBytes(thisReader:PrimitiveInputStream, thatReader: PrimitiveInputStream) = {
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

case class HTaskID(name: String, previousTaskName: String = null)

/**
  * A Task for a mapper-only job
  */
case class HMapTask[MK, MV, MOK: Manifest, MOV: Manifest, S <: SettingsBase](id: HTaskID, configs: HTaskConfigs = HTaskConfigs(), io: HIO[MK, MV, MOK, MOV, S] = HIO(), mapper: HMapper[MK, MV, MOK, MOV]) extends HTask[MK, MV, MOK, MOV, S](id, configs, io) {
  def decorateJob(job: Job) {
    job.setMapperClass(mapper.getClass)
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setNumReduceTasks(0)
  }

}

/**
  * A task for a Mapper / Combiner / Reducer combo
  */
case class HMapCombineReduceTask[MK, MV, MOK: Manifest, MOV: Manifest, ROK, ROV, S <: SettingsBase](id: HTaskID, configs: HTaskConfigs = HTaskConfigs(), io: HIO[MK, MV, ROK, ROV, S] = HIO(), mapper: HMapper[MK, MV, MOK, MOV], combiner: HReducer[MOK, MOV, ROK, ROV], reducer: HReducer[MOK, MOV, ROK, ROV]) extends HTask[MK, MV, ROK, ROV, S](id, configs, io) {
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
  def row = new QueryResult[T, R](table.convertResult(context.getCurrentValue), table, table.tableName)
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

class ToTableReduceContext[MOK, MOV, T <: HbaseTable[T, R, _], R, S <: SettingsBase](conf: Configuration, counter: (String, Long) => Unit, context: Reducer[MOK, MOV, NullWritable, Writable]#Context) extends HReduceContext[MOK, MOV, NullWritable, Writable, S](conf, counter, context) {
  def write(operation: OpBase[T, R]) {
    operation.getOperations.foreach {op => write(NullWritable.get(), op)}
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

