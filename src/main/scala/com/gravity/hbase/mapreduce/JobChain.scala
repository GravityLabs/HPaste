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

//Mapper template: HMapper[MK,MV,MOK,MOV,S]
//Reducer tempmlate: HReducer[MOK,MOV,ROK,ROV,S]

object TryErOut extends App {
}

class MyJob extends HJob[NoSettings](
  HPathInput("/user/gravity/magellan/beacons/2011_100/*.csv" :: Nil),

  Seq(
    HMapReduceTask(
      mapper = (ctx: HMapContext[LongWritable, Text, BytesWritable, BytesWritable, NoSettings]) => {
        val line = ctx.value.toString
        val splits = line.split("\\^")
        val date = splits(0)
        val action = splits(1)
        val site = splits(2)
        ctx.write(
          makeWritable {
            output =>
              output.writeUTF(action)
          },
          makeWritable {
            output =>
              output.writeUTF(site)
              output.writeUTF(date)
          }
        )
      }
      , reducer = (ctx: HReduceContext[BytesWritable, BytesWritable, BytesWritable, BytesWritable, NoSettings]) => {
        val action = readWritable(ctx.key) {input => input.readUTF()}
        var count = 0l
        ctx.values.foreach {value => count = count + 1l}

        ctx.write(
          makeWritable {output => output.writeLong(count)},
          makeWritable {output => output.writeUTF(action)}
        )
      }),
    HMapReduceTask(
      mapper = (ctx: HMapContext[BytesWritable, BytesWritable, BytesWritable, BytesWritable, NoSettings]) => {
        ctx.write(ctx.key, ctx.value)
      }, reducer = (ctx: HReduceContext[BytesWritable, BytesWritable, NullWritable, Text, NoSettings]) => {
        val count = ctx.key
        ctx.values.foreach {
          value =>
            ctx.write(NullWritable.get(), new Text(readWritable(value) {input => input.readUTF()} + " : " + count))
        }
      })
  )
  ,
  HPathOutput("/user/chris/testhpaste/")
)


class HJob[S <: SettingsBase](input: HInput, tasks: Seq[HTask[_, _, _, _, S]], output: HOutput) {
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

class MyMapper extends HMapReduceTask[LongWritable, Text, BytesWritable, BytesWritable, NullWritable, Text, NoSettings](
  (hMapContext: HMapContext[LongWritable, Text, BytesWritable, BytesWritable, NoSettings]) => {},
  (reduceContext: HReduceContext[BytesWritable, BytesWritable, NullWritable, Text, NoSettings]) => {}
)

//class HMapReduceJob[M <: HMapper[MK,MV,MOK,MOV,S], R <: HReducer[MOK,MOV,ROK,ROV,S], MK,MV,MOK,MOV,ROK,ROV,S <: SettingsBase](mapper:M, reducer: R) {
//  def run(settings: S) {
//
//  }
//}

abstract class HInput {
  def init(conf: Configuration, job: Job) {

  }
}

abstract class HOutput {
  def init(conf: Configuration, job: Job) {

  }
}

case class HTableInput[T <: HbaseTable[T, R], R](table: T) extends HInput {
  override def init(conf: Configuration, job: Job) {
    conf.set(TableInputFormat.INPUT_TABLE, table.tableName)
    job.setInputFormatClass(classOf[TableInputFormat])
  }
}

case class HPathInput(paths: Seq[String]) extends HInput {
  override def init(conf: Configuration, job: Job) {
    paths.foreach(path => {
      FileInputFormat.addInputPath(job, new Path(path))
    })
  }
}

case class HTableOutput[T <: HbaseTable[T, R], R](table: T) extends HOutput {
  override def init(conf: Configuration, job: Job) {
    conf.set(GravityTableOutputFormat.OUTPUT_TABLE, table.tableName)
    job.setOutputFormatClass(classOf[GravityTableOutputFormat[ImmutableBytesWritable]])
  }
}

case class HPathOutput(path: String) extends HOutput {
  override def init(conf: Configuration, job: Job) {
    FileSystem.get(conf).delete(new Path(path), true)
    FileOutputFormat.setOutputPath(job, new Path(path))
  }
}

case class HRandomSequenceInput[K, V]() extends HInput {
  var previousPath: Path = _

  override def init(conf: Configuration, job: Job) {
    FileInputFormat.addInputPath(job, previousPath)

    job.setInputFormatClass(classOf[SequenceFileInputFormat[K, V]])
  }
}

case class HRandomSequenceOutput[K, V]() extends HOutput {
  var path = new Path(genTmpFile)

  override def init(conf: Configuration, job: Job) {
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[K, V]])
    FileOutputFormat.setOutputPath(job, path)
  }

}


abstract class HTask[IK, IV, OK, OV, S <: SettingsBase](var input: HInput = HRandomSequenceInput[IK, IV](), var output: HOutput = HRandomSequenceOutput[OK, OV]()) {
  var configuration: Configuration = _
  var settings: S = _

  def configure(conf: Configuration, previousTask: HTask[_, _, _, _, S]) {
    configuration = conf
  }

  def decorateJob(conf: Configuration, job: Job)

  def makeJob(previousTask: HTask[_, _, _, _, S]) = {

    val job = new Job(configuration)
    job.setJarByClass(getClass)

    if (previousTask != null && previousTask.output.isInstanceOf[HRandomSequenceOutput[_, _]] && input.isInstanceOf[HRandomSequenceInput[_, _]]) {
      input.asInstanceOf[HRandomSequenceInput[_, _]].previousPath = previousTask.output.asInstanceOf[HRandomSequenceOutput[_, _]].path
    }

    input.init(configuration, job)
    output.init(configuration, job)

    decorateJob(configuration, job)

    job
  }
}

case class HMapReduceTask[MK, MV, MOK: Manifest, MOV: Manifest, ROK, ROV, S <: SettingsBase](mapper: MapperFunc[MK, MV, MOK, MOV, S], reducer: ReducerFunc[MOK, MOV, ROK, ROV, S]) extends HTask[MK, MV, ROK, ROV, S] {

  val mapperClass = classOf[HMapper[MK,MV,MOK,MOV,S]]
  val reducerClass = classOf[HReducer[MOK,MOV,ROK,ROV,S]]


  def decorateJob(conf: Configuration, job: Job) {
    job.setMapperClass(mapperClass)
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setReducerClass(reducerClass)

  }
}


case class HMapTask[MK, MV, MOK: Manifest, MOV: Manifest, S <: SettingsBase](mapper: MapperFunc[MK, MV, MOK, MOV, S]) extends HTask[MK, MV, MOK, MOV, S] {
  def decorateJob(conf: Configuration, job: Job) {
    job.setMapperClass(classOf[HMapper[MK, MV, MOK, MOV, S]])
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
  }

}

case class HMapCombineReduceTask[MK, MV, MOK: Manifest, MOV: Manifest, ROK, ROV, S <: SettingsBase](mapper: MapperFunc[MK, MV, MOK, MOV, S], combiner: ReducerFunc[MOK, MOV, ROK, ROV, S], reducer: ReducerFunc[MOK, MOV, ROK, ROV, S]) extends HTask[MK, MV, ROK, ROV, S] {
  def decorateJob(conf: Configuration, job: Job) {
    job.setMapperClass(classOf[HMapper[MK, MV, MOK, MOV, S]])
    job.setMapOutputKeyClass(classManifest[MOK].erasure)
    job.setMapOutputValueClass(classManifest[MOV].erasure)
    job.setReducerClass(classOf[HReducer[MOK, MOV, ROK, ROV, S]])
    job.setCombinerClass(classOf[HReducer[MOK, MOV, ROK, ROV, S]])
  }
}

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
    mapperFunc = job.getMapperFunc(context.getConfiguration.getInt("hpaste.jobchain.mapper.idx", -1))

    hcontext = new HMapContext[MK, MV, MOK, MOV, S](context.getConfiguration, counter, context)
  }

  override def map(key: MK, value: MV, context: Mapper[MK, MV, MOK, MOV]#Context) {
    mapperFunc(hcontext)
  }
}

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
    reducerFunc = job.getReducerFunc(context.getConfiguration.getInt("hpaste.jobchain.reducer.idx", -1))

    hcontext = new HReduceContext[MOK, MOV, ROK, ROV, S](context.getConfiguration, counter, context)
  }

  override def reduce(key: MOK, values: java.lang.Iterable[MOV], context: Reducer[MOK, MOV, ROK, ROV]#Context) {
    reducerFunc(hcontext)
  }
}

class HMapContext[MK, MV, MOK, MOV, S <: SettingsBase](conf: Configuration, counter: (String, Long) => Unit, context: Mapper[MK, MV, MOK, MOV]#Context) extends HContext(conf, counter) {
  def key = context.getCurrentKey

  def value = context.getCurrentValue

  def write(key: MOK, value: MOV) {context.write(key, value)}
}

class HReduceContext[MOK, MOV, ROK, ROV, S <: SettingsBase](conf: Configuration, counter: (String, Long) => Unit, context: Reducer[MOK, MOV, ROK, ROV]#Context) extends HContext(conf, counter) {
  def key = context.getCurrentKey

  def values = context.getValues

  def write(key: ROK, value: ROV) {context.write(key, value)}
}

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

//class JobChain(jobs: Seq[HMapReduceJob]) {
//
//  def submit() {
//
//  }
//
//}
//
//class HMapper(mapper: Mapper) {
//
//}
//
//class HReducer()
//
//class HCombiner()
//
//class HMapReduceJob(mapper:HMapper, reducer:HReducer, combiner:HCombiner = null) {
//
//
//
//}
//
//class JobChainExample