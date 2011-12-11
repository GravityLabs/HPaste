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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapreduce.{Mapper, Reducer, Job}
import com.gravity.hbase.schema._
import com.gravity.hadoop.{GravityTableOutputFormat}
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper
import java.io.{DataOutputStream, ByteArrayOutputStream}
import org.apache.hadoop.hbase.util.Base64
import collection.mutable.Buffer
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import sun.reflect.Reflection
import org.apache.hadoop.hbase.mapreduce.{TableReducer, TableMapper, TableInputFormat}
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.client.{Delete, Put, Scan, Result}
import java.lang.Iterable
import org.apache.hadoop.hbase.filter.{SingleColumnValueFilter, FilterList, Filter}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

trait OurMapper[MK, MV, MOK, MOV, S <: SettingsBase] {
  var hcontext: HpasteContext[S] = _
  var context: Mapper[MK, MV, MOK, MOV]#Context = _

  def counter(message: String, count: Long) {
    context.getCounter("Custom", message).increment(count)
  }

  def setupOurs(ctx: Mapper[MK, MV, MOK, MOV]#Context) {
    context = ctx
    hcontext = new HpasteContext[S](context.getConfiguration, counter)
  }
}

trait OurReducer[MOK, MOV, RK, RV, S <: SettingsBase] {
  var hcontext: HpasteContext[S] = _
  var context: Reducer[MOK, MOV, RK, RV]#Context = _

  def counter(message: String, count: Long) {
    context.getCounter("Custom", message).increment(count)
  }

  def setupOurs(ctx: Reducer[MOK, MOV, RK, RV]#Context) {
    context = ctx
    hcontext = new HpasteContext[S](context.getConfiguration, counter)
  }
}

class HpasteContext[S <: SettingsBase](conf: Configuration, val counter: (String, Long) => Unit) {
  def apply(message: String, count: Long) {counter(message, count)}


  //TODO: Make this not optional and refactor clients
  val settings: Option[S] = try {
    val s = Class.forName(conf.get("hpaste.settingsclass")).newInstance().asInstanceOf[S]
    s.fromSettings(conf)
    Some(s)
  } catch {
    case ex: Throwable => None
  }
}


class FuncMapper[MK, MV, MOK, MOV, S <: SettingsBase] extends Mapper[MK, MV, MOK, MOV] with OurMapper[MK, MV, MOK, MOV, S] {

  var mapper: (MK, MV, (MOK, MOV) => Unit, HpasteContext[S]) => Unit = _

  override def setup(context: Mapper[MK, MV, MOK, MOV]#Context) {
    val jobClass = Class.forName(context.getConfiguration.get("mapperholder")).newInstance().asInstanceOf[FunctionalJobBase[MK, MV, MOK, MOV, _, _, S]]
    mapper = jobClass.mapper
    setupOurs(context)
  }

  override def map(key: MK, value: MV, context: Mapper[MK, MV, MOK, MOV]#Context) {
    def write(key: MOK, value: MOV) {context.write(key, value)}

    mapper(key, value, write, hcontext)
    //    mapper(key,value,write,counter)
  }
}


class FuncReducer[IK, IV, OK, OV, S <: SettingsBase] extends Reducer[IK, IV, OK, OV] with OurReducer[IK, IV, OK, OV, S] {
  var reducer: (IK, Iterable[IV], (OK, OV) => Unit, HpasteContext[S]) => Unit = _

  override def setup(context: Reducer[IK, IV, OK, OV]#Context) {
    val jobClass = Class.forName(context.getConfiguration.get("mapperholder")).newInstance().asInstanceOf[FunctionalJobBase[_, _, IK, IV, OK, OV, S]]
    reducer = jobClass.reducer

    setupOurs(context)
  }
}

class FuncTableMapper[T <: HbaseTable[T, R,_], R, S <: SettingsBase] extends TableMapper[NullWritable, Writable] with OurMapper[ImmutableBytesWritable, Result, NullWritable, Writable, S] {


  var jobBase: TableAnnotationJobBase[T, R, S] = _

  override def setup(context: Mapper[ImmutableBytesWritable, Result, NullWritable, Writable]#Context) {
    jobBase = Class.forName(context.getConfiguration.get("mapperholder")).newInstance().asInstanceOf[TableAnnotationJobBase[T, R, S]]
    jobBase.fromSettings(context.getConfiguration)

    setupOurs(context)
  }

  override def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable, Result, NullWritable, Writable]#Context) {
    def write(operation: Writable) {context.write(NullWritable.get(), operation)}

    jobBase.mapper(new QueryResult[T, R](value, jobBase.mapTable, jobBase.mapTable.tableName), write, hcontext)
    //    mapper(key,value,write,counter)
  }
}

class FuncTableExternMapper[T <: HbaseTable[T, R,_], R, MOK, MOV, S <: SettingsBase] extends TableMapper[MOK, MOV] with OurMapper[ImmutableBytesWritable, Result, MOK, MOV, S] {


  var jobBase: TableAnnotationMRJobBase[T, R, _, _, MOK, MOV, S] = _

  override def setup(context: Mapper[ImmutableBytesWritable, Result, MOK, MOV]#Context) {
    jobBase = Class.forName(context.getConfiguration.get("mapperholder")).newInstance().asInstanceOf[TableAnnotationMRJobBase[T, R, _, _, MOK, MOV, S]]

    jobBase.fromSettings(context.getConfiguration)
    setupOurs(context)
  }

  override def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable, Result, MOK, MOV]#Context) {
    def write(key: MOK, value: MOV) {context.write(key, value)}

    jobBase.mapper(new QueryResult[T, R](value, jobBase.mapTable, jobBase.mapTable.tableName), write, hcontext)
    //    mapper(key,value,write,counter)
  }
}

class FuncTableExternReducer[T <: HbaseTable[T, R,_], R, MOK, MOV, S <: SettingsBase] extends TableReducer[MOK, MOV, NullWritable] with OurReducer[MOK, MOV, NullWritable, Writable, S] {
  var jobBase: TableAnnotationMRJobBase[_, _, T, R, MOK, MOV, S] = _

  override def setup(context: Reducer[MOK, MOV, NullWritable, Writable]#Context) {
    jobBase = Class.forName(context.getConfiguration.get("mapperholder")).newInstance().asInstanceOf[TableAnnotationMRJobBase[_, _, T, R, MOK, MOV, S]]
    jobBase.fromSettings(context.getConfiguration)
    setupOurs(context)

  }

  override def reduce(key: MOK, values: java.lang.Iterable[MOV], context: Reducer[MOK, MOV, NullWritable, Writable]#Context) {
    def write(value: OpBase[T, R]) {
      value.getOperations.foreach {
        op =>
          context.write(NullWritable.get(), op)
      }
    }

    jobBase.reducer(key, values, write, hcontext)
  }
}

class PathTableExternReducer[T <: HbaseTable[T, R,_], R, MOK, MOV, S <: SettingsBase] extends TableReducer[MOK, MOV, NullWritable] with OurReducer[MOK, MOV, NullWritable, Writable, S] {
  var jobBase: PathToTableMRJobBase[T, R, MOK, MOV, S] = _

  override def setup(context: Reducer[MOK, MOV, NullWritable, Writable]#Context) {
    jobBase = Class.forName(context.getConfiguration.get("mapperholder")).newInstance().asInstanceOf[PathToTableMRJobBase[T, R, MOK, MOV, S]]

    jobBase.fromSettings(context.getConfiguration)
    setupOurs(context)

  }

  override def reduce(key: MOK, values: java.lang.Iterable[MOV], context: Reducer[MOK, MOV, NullWritable, Writable]#Context) {
    def write(value: OpBase[T, R]) {
      value.getOperations.foreach {
        op =>
          context.write(NullWritable.get(), op)
      }
    }

    jobBase.reducer(key, values, write, hcontext)
  }
}


class PathMapper[MOK, MOV, S <: SettingsBase] extends Mapper[LongWritable, Text, MOK, MOV] with OurMapper[LongWritable, Text, MOK, MOV, S] {

  var mapper: (LongWritable, Text, (MOK, MOV) => Unit, HpasteContext[S]) => Unit = _

  override def setup(context: Mapper[LongWritable, Text, MOK, MOV]#Context) {

    val jobClass = Class.forName(context.getConfiguration.get("mapperholder")).newInstance().asInstanceOf[PathToTableMRJobBase[_, _, MOK, MOV, S]]
    mapper = jobClass.mapper

    jobClass.fromSettings(context.getConfiguration)
    setupOurs(context)
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, MOK, MOV]#Context) {
    def write(key: MOK, value: MOV) {context.write(key, value)}

    mapper(key, value, write, hcontext)
    //    mapper(key,value,write,counter)
  }
}


class TableToPathMapper[T <: HbaseTable[T, R,_], R, MOK, MOV, S <: SettingsBase] extends TableMapper[MOK, MOV] with OurMapper[ImmutableBytesWritable, Result, MOK, MOV, S] {


  var jobBase: TableToPathMRJobBase[T, R, MOK, MOV, S] = _


  override def setup(context: Mapper[ImmutableBytesWritable, Result, MOK, MOV]#Context) {
    jobBase = Class.forName(context.getConfiguration.get("mapperholder")).newInstance().asInstanceOf[TableToPathMRJobBase[T, R, MOK, MOV, S]]
    jobBase.fromSettings(context.getConfiguration)

    setupOurs(context)
  }

  override def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable, Result, MOK, MOV]#Context) {
    def write(key: MOK, value: MOV) {context.write(key, value)}

    jobBase.mapper(new QueryResult[T, R](value, jobBase.mapTable, jobBase.mapTable.tableName), write, hcontext)
    //    mapper(key,value,write,counter)
  }
}


class TableToPathReducer[MOK, MOV, S <: SettingsBase] extends Reducer[MOK, MOV, NullWritable, Text] with OurReducer[MOK, MOV, NullWritable, Text, S] {
  var reducer: (MOK, Iterable[MOV], (String) => Unit, HpasteContext[S]) => Unit = _


  override def setup(context: Reducer[MOK, MOV, NullWritable, Text]#Context) {
    val jobClass = Class.forName(context.getConfiguration.get("mapperholder")).newInstance().asInstanceOf[TableToPathMRJobBase[_, _, MOK, MOV, S]]
    reducer = jobClass.reducer
    jobClass.fromSettings(context.getConfiguration)
    setupOurs(context)
  }


  override def reduce(key: MOK, values: java.lang.Iterable[MOV], context: Reducer[MOK, MOV, NullWritable, Text]#Context) {
    def write(value: String) {context.write(NullWritable.get(), new Text(value))}

    reducer(key, values, write, hcontext)

  }
}

abstract class TableToPathMRJobBase[T <: HbaseTable[T, R,_], R, MOK: Manifest, MOV: Manifest, S <: SettingsBase]
(
        name: String,
        val mapTable: T,
        val mapper: (QueryResult[T, R], (MOK, MOV) => Unit, HpasteContext[S]) => Unit,
        val reducer: (MOK, java.lang.Iterable[MOV], (String) => Unit, HpasteContext[S]) => Unit,
        conf: Configuration
        ) extends SettingsJobBase[S](name)(conf) with FromTable[T] with ToPath with JobSettings {
  val fromTable = mapTable

  val reduceTasks = 1

  override def configure(conf: Configuration) {
    conf.set("mapperholder", getClass.getName)
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    job.setMapperClass(classOf[TableToPathMapper[T,R,MOK,MOV,S]])
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setReducerClass(classOf[TableToPathReducer[MOK,MOV,S]])
    job.setNumReduceTasks(reduceTasks)
    super.configureJob(job)
  }
}

class PathToPathMapper[MOK, MOV, S <: SettingsBase] extends Mapper[LongWritable, Text, MOK, MOV] with OurMapper[LongWritable, Text, MOK, MOV, S] {
  var jobBase: PathToPathMRJobBase[MOK, MOV, S] = _

  override def setup(context: Mapper[LongWritable, Text, MOK, MOV]#Context) {
    if (jobBase == null) {
      jobBase = Class.forName(context.getConfiguration.get("mapperholder")).newInstance().asInstanceOf[PathToPathMRJobBase[MOK, MOV, S]]
    }
    jobBase.fromSettings(context.getConfiguration)

    setupOurs(context)
  }

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, MOK, MOV]#Context) {
    def write(key: MOK, value: MOV) {context.write(key, value)}
    jobBase.mapper(value, write, hcontext)
  }
}

class PathToPathReducer[MOK, MOV, S <: SettingsBase] extends Reducer[MOK, MOV, NullWritable, Text] with OurReducer[MOK, MOV, NullWritable, Text, S] {
  var reducer: (MOK, Iterable[MOV], (String) => Unit, HpasteContext[S]) => Unit = _

  override def setup(context: Reducer[MOK, MOV, NullWritable, Text]#Context) {
    if (reducer == null) {
      val jobClass = Class.forName(context.getConfiguration.get("mapperholder")).newInstance.asInstanceOf[PathToPathMRJobBase[MOK, MOV, S]]
      reducer = jobClass.reducer
      jobClass.fromSettings(context.getConfiguration)
    }
    setupOurs(context)
  }

  override def reduce(key: MOK, values: java.lang.Iterable[MOV], context: Reducer[MOK, MOV, NullWritable, Text]#Context) {
    def write(value: String) {context.write(NullWritable.get(), new Text(value))}

    reducer(key, values, write, hcontext)
  }
}

abstract class PathToPathMRJobBase[MOK: Manifest, MOV: Manifest, S <: SettingsBase](
                                                                                           name: String,
                                                                                           val fromPaths: Seq[String],
                                                                                           val toPath: String,
                                                                                           val mapper: (Text, (MOK, MOV) => Unit, HpasteContext[S]) => Unit,
                                                                                           val reducer: (MOK, java.lang.Iterable[MOV], (String) => Unit, HpasteContext[S]) => Unit,
                                                                                           conf: Configuration
                                                                                           ) extends SettingsJobBase[S](name)(conf) with ToPath with FromPaths with JobSettings {
  override def configure(conf: Configuration) {
    conf.set("mapperholder", getClass.getName)
    super.configure(conf)
  }

  val reduceTasks = 1

  val paths = fromPaths
  val path = toPath

  override def configureJob(job: Job) {
    job.setMapperClass(classOf[PathToPathMapper[MOK,MOV,S]])
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setReducerClass(classOf[PathToPathReducer[MOK,MOV,S]])
    job.setNumReduceTasks(reduceTasks)
    super.configureJob(job)
  }
}

abstract class PathToTableMRJobBase[T <: HbaseTable[T, R,_], R, MOK: Manifest, MOV: Manifest, S <: SettingsBase]
(
        name: String,
        val reduceTable: T,
        val mapper: (LongWritable, Text, (MOK, MOV) => Unit, HpasteContext[S]) => Unit,
        val reducer: (MOK, java.lang.Iterable[MOV], (OpBase[T, R]) => Unit, HpasteContext[S]) => Unit,
        val paths: Seq[String],
        conf: Configuration
        ) extends SettingsJobBase[S](name)(conf) with ToTable[T] with FromPaths with JobSettings {
  val toTable = reduceTable


  override def configure(conf: Configuration) {
    conf.set("mapperholder", getClass.getName)
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    job.setMapperClass(classOf[PathMapper[MOK,MOV,S]])
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setReducerClass(classOf[PathTableExternReducer[T,R,MOK,MOV,S]])
    job.setNumReduceTasks(50)
    super.configureJob(job)
  }

}

abstract class TableAnnotationMRJobBase[T <: HbaseTable[T, R,_], R, TT <: HbaseTable[TT, RR, _], RR, MOK: Manifest, MOV: Manifest, S <: SettingsBase]
(name: String, val mapTable: T, val reduceTable: TT,
 val mapper: (QueryResult[T, R], (MOK, MOV) => Unit, HpasteContext[S]) => Unit,
 val reducer: (MOK, Iterable[MOV], (OpBase[TT, RR]) => Unit, HpasteContext[S]) => Unit,
 conf: Configuration
        ) extends SettingsJobBase[S](name)(conf) with FromTable[T] with ToTable[TT] with JobSettings {

  val fromTable = mapTable
  val toTable = reduceTable

  val reduceTasks = 1

  override def configure(conf: Configuration) {
    conf.set("mapperholder", getClass.getName)
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    job.setMapperClass(classOf[FuncTableExternMapper[T,R,MOK,MOV,S]])
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setReducerClass(classOf[FuncTableExternReducer[TT,RR,MOK,MOV,S]])
    job.setNumReduceTasks(reduceTasks)

    super.configureJob(job)
  }


}


abstract class TableAnnotationJobBase[T <: HbaseTable[T, R,_], R, S <: SettingsBase]
(name: String, val mapTable: T,
 val mapper: (QueryResult[T, R], (Writable) => Unit, HpasteContext[S]) => Unit,
 conf: Configuration
        ) extends SettingsJobBase[S](name)(conf) with FromTable[T] with ToTable[T] with JobSettings {

  val fromTable = mapTable
  val toTable = mapTable

  val mapperThreads = 1


  override def configure(conf: Configuration) {
    conf.set("mapperholder", getClass.getName)
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    if (mapperThreads > 1) {
      println("Invoking multithreaded mapper and setting mapper threads to " + mapperThreads)
      MultithreadedMapper.setNumberOfThreads(job, mapperThreads)
      job.setMapperClass(classOf[MultithreadedMapper[_, _, _, _]])
      job.setMapperClass(classOf[MultithreadedMapper[_,_,_,_]])
      MultithreadedMapper.setMapperClass(job,classOf[FuncTableMapper[T,R,S]])
    } else {
      job.setMapperClass(classOf[FuncTableMapper[T,R,S]])
    }
    job.setMapOutputKeyClass(classOf[NullWritable])
    job.setMapOutputValueClass(classOf[Writable])
    job.setNumReduceTasks(0)
    super.configureJob(job)
  }

}

abstract class FunctionalJobBase[MK, MV, MOK: Manifest, MOV: Manifest, ROK, ROV, S <: SettingsBase]
(name: String,
 val mapper: (MK, MV, (MOK, MOV) => Unit, HpasteContext[S]) => Unit,
 val reducer: (MOK, Iterable[MOV], (ROK, ROV) => Unit, HpasteContext[S]) => Unit) extends JobTrait {


  def run(conf: Configuration) {
    val c = new Configuration(conf)
    c.set("mapperholder", getClass.getName)
    configure(c)
    val job = new Job(c)
    job.setJarByClass(getClass)
    job.setJobName(name)
    FileInputFormat.addInputPath(job, new Path("/user/gravity/magellan/beacons/**/*.csv"))

    job.setMapperClass(classOf[FuncMapper[MK,MV,MOK,MOV,S]])
    //    job.setMapperClass(classOf[FuncMapper[MK,MV,MOK,MOV]])
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)

    job.setReducerClass(classOf[FuncReducer[MOK,MOV,ROK,ROV,S]])
    //job.setReducerClass(classOf[FuncReducer[MOK,MOV,ROK,ROV]])

    FileOutputFormat.setOutputPath(job, new Path("/user/gravity/magellan/output"))

    configureJob(job)
    job.waitForCompletion(true)
  }
}

object Settings {

  object None extends NoSettings

}


class NoSettings extends SettingsBase {

}

class SettingsBase {
  def fromSettings(conf: Configuration) {

  }

  def toSettings(conf: Configuration) {

  }

  def jobNameQualifier = ""
}

abstract class SettingsJobBase[S <: SettingsBase](name: String)(implicit conf: Configuration) extends JobBase(name)(conf) {
  var _settings: S = _


  override def jobName = {
    if(_settings.jobNameQualifier.size > 0) {
      name + "(" + _settings.jobNameQualifier + ")"
    }else {
      name
    }
  }

  override def configure(conf: Configuration) {
    _settings.toSettings(conf)
    conf.set("hpaste.settingsclass", _settings.getClass.getName)
    super.configure(conf)
  }

  def run(settings: S) = {
    _settings = settings
    super.run()
  }


}

/**
 * Base class for Jobs that will be composed using the JobTraits.
 */
abstract class JobBase(val name: String)(implicit conf: Configuration) extends JobTrait {
  var job: Job = _


  def jobName = name

  def init() {
    val jobConf = new Configuration(conf)
    configure(jobConf)

    job = new Job(jobConf)
    job.setJarByClass(getClass)
    job.setJobName(jobName)
    configureJob(job)
  }


  def run(): Boolean = {
    init()
    job.waitForCompletion(true)
  }

}


trait JobSettings extends JobTrait {

  def fromSettings(conf: Configuration) {

  }

  def toSettings(conf: Configuration) {

  }


  override def configure(conf: Configuration) {
    toSettings(conf)

    super.configure(conf)
  }

  override def configureJob(job: Job) {
    super.configureJob(job)
  }
}

/**
* Declares that you want a Reducer.  Will force you to provide the class, and the number thereof.
* If you specify None for the numReducers it will use the configured default.
*/
trait ReducerJob[M <: Reducer[_, _, _, _]] extends JobTrait {
  val reducer: Class[M]
  val numReducers: Option[Int]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    //    job.setReducerClass(reducer)
    job.setReducerClass(reducer)
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
    job.setMapperClass(mapper)
    job.setMapOutputKeyClass(mapperOutputKey)
    job.setMapOutputValueClass(mapperOutputValue)

    super.configureJob(job)
  }


}

trait TableReducerJob[R <: TableWritingReducer[TF, TFK, MK, MV], TF <: HbaseTable[TF, TFK, _], TFK, MK, MV] extends ToTable[TF] {
  val reducer: Class[R]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    //    job.setReducerClass(reducer)
    job.setReducerClass(reducer)
    super.configureJob(job)
  }


}

trait TableMapperJob[M <: TableReadingMapper[TF, TFK, MK, MV], TF <: HbaseTable[TF, TFK, _], TFK, MK, MV] extends FromTable[TF] {
  val mapper: Class[M]
  val mapperOutputKey: Class[MK]
  val mapperOutputValue: Class[MV]


  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    job.setMapperClass(mapper)
    job.setMapOutputKeyClass(mapperOutputKey)
    job.setMapOutputValueClass(mapperOutputValue)
    super.configureJob(job)

  }
}

trait ToPath extends JobTrait {
  val path: String

  override def configure(conf: Configuration) {
    FileSystem.get(conf).delete(new Path(path), true)

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
trait FromTable[T <: HbaseTable[T, _, _]] extends JobTrait with NoSpeculativeExecution {

  val fromTable: HbaseTable[T, _, _]

  val families = Buffer[ColumnFamily[T, _, _, _, _]]()
  val columns = Buffer[Column[T, _, _, _, _]]()
  val filterBuffer = scala.collection.mutable.Buffer[Filter]()

  var startRow: Option[Array[Byte]] = None
  var endRow: Option[Array[Byte]] = None

  //Override to add custom attributes to the scanner
  def createScanner = new Scan

  def specifyFamily(family: (T) => ColumnFamily[T, _, _, _, _]) {
    families += family(fromTable.pops)
  }

  def specifyColumn(column: (T) => Column[T, _, _, _, _]) {
    columns += column(fromTable.pops)
  }

  def specifyColumnValue[V](column: (T) => Column[T, _, _, _, V], value: V)(implicit c: ByteConverter[V]) {
    val col = column(fromTable.pops)
    val filter = new SingleColumnValueFilter(
      col.familyBytes,
      col.columnBytes,
      CompareOp.EQUAL,
      c.toBytes(value)
    )
    filterBuffer += filter
  }

  def specifyFilter(filter: Filter) {
    filterBuffer.add(filter)
  }

  /*
  Prepares the scanner for use by chaining the filters together.  Should be called immediately before passing the scanner to the table.
   */
  def combineFilters(operator: FilterList.Operator = FilterList.Operator.MUST_PASS_ALL): Option[FilterList] = {
    if (filterBuffer.size > 0) {
      val filterList = new FilterList(operator)
      filterBuffer.foreach {filter => filterList.addFilter(filter)}
      Some(filterList)
    } else None
  }

  def specifyStartKey[R](key: R)(implicit c: ByteConverter[R]) {startRow = Some(c.toBytes(key))}

  def specifyEndKey[R](key: R)(implicit c: ByteConverter[R]) {endRow = Some(c.toBytes(key))}

  override def configure(conf: Configuration) {
    println("Configuring FromTable")
    conf.set(TableInputFormat.INPUT_TABLE, fromTable.tableName)
    val scanner = createScanner
    scanner.setCacheBlocks(false)
    scanner.setCaching(1000)
    scanner.setMaxVersions(1)

    columns.foreach {
      col =>
        scanner.addColumn(col.familyBytes, col.columnBytes)
    }

    families.foreach {
      family =>
        scanner.addFamily(family.familyBytes)
    }

    startRow.foreach {
      bytes =>
        scanner.setStartRow(bytes)
    }

    endRow.foreach {
      bytes =>
        scanner.setStopRow(bytes)
    }

    combineFilters().foreach {
      filters =>
        scanner.setFilter(filters)
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
trait ToTable[T <: HbaseTable[T, _, _]] extends JobTrait with NoSpeculativeExecution {
  val toTable: HbaseTable[T, _, _]

  override def configure(conf: Configuration) {
    println("Configuring ToTable")
    conf.set(GravityTableOutputFormat.OUTPUT_TABLE, toTable.tableName)
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    println("Configuring ToTable Job")
     job.setOutputFormatClass(classOf[GravityTableOutputFormat[ImmutableBytesWritable]])
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

trait TemporaryMemoryBaseSettings extends JobTrait {

  override def configure(conf: Configuration) {
    conf.set("mapred.map.child.java.opts", "-Xmx1024m -server -Djava.net.preferIPv4Stack=true")
    conf.set("mapred.reduce.child.java.opts", "-Xmx1024m -server -Djava.net.preferIPv4Stack=true")
    conf.setInt("mapred.job.map.memory.mb", 1824)
    conf.setInt("mapred.job.reduce.memory.mb", 1824)

    super.configure(conf)
  }

  override def configureJob(job: Job) {
    super.configureJob(job)
  }
}

trait MemoryOverrideJob extends JobTrait {
  val mapMemory: Int
  val reduceMemory: Int

  override def configure(conf: Configuration) {

    val memory = mapMemory
    val reducememory = reduceMemory
    conf.set("mapred.map.child.java.opts", "-Xmx" + memory + "m" + " -Xms" + memory + "m")
    //    conf.set("mapred.map.child.java.opts", "-Xmx" + memory + "m")
    conf.set("mapred.reduce.child.java.opts", "-Xmx" + reducememory + "m")
    conf.setInt("mapred.job.map.memory.mb", memory + 800)
    conf.setInt("mapred.job.reduce.memory.mb", reducememory + 800)

    super.configure(conf)
  }

  override def configureJob(job: Job) {
    super.configureJob(job)
  }
}

trait LongRunningJob extends JobTrait {
  val timeoutInSeconds: Int

  override def configure(conf: Configuration) {
    conf.setInt("mapred.task.timeout", timeoutInSeconds)
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    super.configureJob(job)
  }
}

trait BigMemoryJob extends JobTrait {
  val mapMemory: Int
  val reduceMemory: Int

  val mapperBufferMB: Int = 2500
  val reducerBufferMB: Int = 2000

  override def configure(conf: Configuration) {

    val memory = mapMemory
    val reducememory = reduceMemory
    conf.set("mapred.map.child.java.opts", "-Xmx" + memory + "m" + " -Xms" + memory + "m")
    //    conf.set("mapred.map.child.java.opts", "-Xmx" + memory + "m")
    conf.set("mapred.reduce.child.java.opts", "-Xmx" + reducememory + "m")
    conf.setInt("mapred.job.map.memory.mb", memory + mapperBufferMB)
    conf.setInt("mapred.job.reduce.memory.mb", reducememory + reducerBufferMB)

    super.configure(conf)
  }

  override def configureJob(job: Job) {
    super.configureJob(job)
  }

}

trait TableAnnotationMultithreadedMapperJob[M <: TableAnnotationMapper[T, _], T <: HbaseTable[T, _, _], TT <: HbaseTable[TT, _, _]] extends JobTrait with FromTable[T] with ToTable[TT] with MapperOnly {
  val mapper: Class[M]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    //job.setMapperClass(mapper)
    println("Configuring Job in Annotation Mapper")
    job.setMapperClass(classOf[MultithreadedMapper[_, _, _, _]])
    MultithreadedMapper.setMapperClass(job,mapper)
    //    MultithreadedMapper.setNumberOfThreads(job, 10)
    super.configureJob(job)
  }
}

class TableAnnotationMapReduceJob[M <: TableReadingMapper[TF, TFK, BytesWritable, BytesWritable], R <: TableWritingReducer[TT, TTK, BytesWritable, BytesWritable], TF <: HbaseTable[TF, TFK,_], TFK, TT <: HbaseTable[TT, TTK,_], TTK](name: String, conf: Configuration, val fromTable: TF, val toTable: TT, val mapper: Class[M], val reducer: Class[R]) extends JobBase(name)(conf) with TableMapperJob[M, TF, TFK, BytesWritable, BytesWritable] with TableReducerJob[R, TT, TTK, BytesWritable, BytesWritable] {

  val mapperOutputKey = classOf[BytesWritable]
  val mapperOutputValue = classOf[BytesWritable]

}

trait TableAnnotationMapperJob[M <: TableAnnotationMapper[T, _], T <: HbaseTable[T, _, _], TT <: HbaseTable[TT, _, _]] extends JobTrait with FromTable[T] with ToTable[TT] with MapperOnly {
  val mapper: Class[M]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    println("Configuring Job in Annotation Mapper")
    job.setMapperClass(mapper)
    super.configureJob(job)
  }
}

abstract class TextFileWritingReducer[MK, MV](name: String = "Text File Reducer") extends Reducer[MK, MV, NullWritable, Text] {
  def item(key: MK, values: java.lang.Iterable[MV], counter: (String, String) => Unit, writer: (String) => Unit)

  override def reduce(key: MK, values: java.lang.Iterable[MV], context: Reducer[MK, MV, NullWritable, Text]#Context) {
    def counter(grp: String, itm: String) {context.getCounter(grp, itm).increment(1l)}
    def writer(item: String) {context.write(NullWritable.get(), new Text(item))}

    item(key, values, counter _, writer _)
  }
}

abstract class TableWritingReducer[TF <: HbaseTable[TF, TFK, _], TFK, MK, MV](name: String, table: TF)(implicit conf: Configuration)
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
abstract class TableReadingMapper[TF <: HbaseTable[TF, TFK,_], TFK, MK, MV](val table: TF) extends TableMapper[MK, MV] {

  def row(value: QueryResult[TF, TFK], counter: (String, Long) => Unit, writer: (MK, MV) => Unit)

  override def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable, Result, MK, MV]#Context) {
    def counter(name: String, count: Long = 1) {context.getCounter(table.tableName + " Reading Job", name).increment(count)}
    def writer(key: MK, value: MV) {context.write(key, value)}

    val queryResult = new QueryResult[TF, TFK](value, table, table.tableName)
    row(queryResult, counter _, writer _)
  }
}

abstract class TableWritingMapper[TF <: HbaseTable[TF, TFK,_], TFK](val name: String, val table: TF)(implicit conf: Configuration) extends Mapper[LongWritable, Text, NullWritable, Writable] {
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, NullWritable, Writable]#Context) {
    def counter(key: String) {context.getCounter(name, key).increment(1l)}
    item(value, counter _).foreach(row => context.write(NullWritable.get(), row))
  }

  def item(value: Text, counter: (String) => Unit): Iterable[Writable]
}


/**
 * Reads from a specified Table and writes to that Table or another Table
 */
abstract class TableAnnotationMapper[TF <: HbaseTable[TF, TFK, _], TFK](val table: TF) extends TableMapper[NullWritable, Writable] {
  def row(value: QueryResult[TF, TFK], counter: (String, Long) => Unit, writer: (Writable) => Unit)

  var context: Mapper[ImmutableBytesWritable, Result, NullWritable, Writable]#Context = _

  def onStart() {

  }

  def getConf = context.getConfiguration

  override def setup(context: Mapper[ImmutableBytesWritable, Result, NullWritable, Writable]#Context) {
    onStart()
  }

  override def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable, Result, NullWritable, Writable]#Context) {
    def writer(item: Writable) {context.write(NullWritable.get(), item)}
    def counter(name: String, count: Long = 1) {context.getCounter(table.tableName + " Annotation Job", name).increment(count)}
    val queryResult = new QueryResult[TF, TFK](value, table, table.tableName)
    this.context = context
    row(queryResult, counter _, writer _)
  }


}
