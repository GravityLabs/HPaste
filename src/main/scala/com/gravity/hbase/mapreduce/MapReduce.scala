package com.gravity.hbase.mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import com.gravity.hadoop.HadoopScalaShim
import org.apache.hadoop.mapreduce.{Mapper, Reducer, Job}
import org.apache.hadoop.io.{Text, LongWritable, Writable, NullWritable}
import org.apache.hadoop.hbase.mapreduce.{TableMapper, TableOutputFormat, TableInputFormat}
import org.apache.hadoop.hbase.client.{Row, Result}
import com.gravity.hbase.schema._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


trait ReducerJob[M] extends JobTrait {
  val reducer : Class[M]
  val numReducers : Option[Int]

  override def configure(conf:Configuration) {
    super.configure(conf)
  }

  override def configureJob(job:Job) {
    HadoopScalaShim.registerReducer(job,reducer)
    numReducers.foreach(reducerCount => job.setNumReduceTasks(reducerCount))
    super.configureJob(job)
  }
}

trait MapperJob[M <: StandardMapper[MK,MV],MK,MV] extends JobTrait {
  val mapper : Class[M]
  val mapperOutputKey : Class[MK]
  val mapperOutputValue : Class[MV]

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

trait FromPaths extends JobTrait {
  val paths : Seq[String]

  override def configure(conf:Configuration) {
    super.configure(conf)
  }

  override def configureJob(job:Job) {
    paths.foreach(path=>{
      FileInputFormat.addInputPath(job, new Path(path))
    })
    super.configureJob(job)
  }

}

trait JobTrait {

  def configure(conf: Configuration) {
    println("JobTrait configure called")
  }

  def configureJob(job: Job) {
    println("JobTrait configure job called")
  }
}

trait FromTable[T <: HbaseTable[T,_]] extends JobTrait {
  val fromTable: HbaseTable[T, _]
  override def configure(conf:Configuration) {
    println("Configuring FromTable")
    conf.set(TableInputFormat.INPUT_TABLE, fromTable.tableName)
    super.configure(conf)
  }

  override def configureJob(job:Job) {
    println("Configuring FromTable Job")
    HadoopScalaShim.registerInputFormat(job, classOf[TableInputFormat])
    super.configureJob(job)
  }
}

trait ToTable[T <: HbaseTable[T,_]] extends JobTrait {
  val toTable: HbaseTable[T,_]
  override def configure(conf:Configuration) {
    println("Configuring ToTable")
    conf.set(TableOutputFormat.OUTPUT_TABLE, toTable.tableName)
    conf.setInt(TableInputFormat.SCAN_CACHEDROWS, 1000)
    conf.setInt(TableInputFormat.SCAN_MAXVERSIONS, 1)
    super.configure(conf)
  }

  override def configureJob(job:Job) {
    println("Configuring ToTable Job")
    HadoopScalaShim.registerOutputFormat(job, classOf[TableOutputFormat[ImmutableBytesWritable]])
    super.configureJob(job)
  }
}

trait MapperOnly extends JobTrait {
  override def configureJob(job:Job) {
    job.setNumReduceTasks(0)
    super.configureJob(job)
  }
}


trait TableAnnotationMapperJob[M <: TableAnnotationMapper[T,_], T <: HbaseTable[T, _], TT <: HbaseTable[TT,_]] extends JobTrait with FromTable[T] with ToTable[TT] with MapperOnly {
  val mapper : Class[M]

  override def configure(conf: Configuration) {
    super.configure(conf)
  }

  override def configureJob(job: Job) {
    //job.setMapperClass(mapper)
    println("Configuring Job in Annotation Mapper")
    HadoopScalaShim.registerMapper(job,mapper)
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
  def mapEvent(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, MK, MV]#Context)

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, MK, MV]#Context) {
    mapEvent(key, value, context)
  }
}

/**
 * Reads from a specified Table and writes to MK and MV
 */
abstract class TableReadingMapper[TF <: HbaseTable[TF, TFK], TFK, MK, MV] extends TableMapper[MK,MV] {
  override def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable,Result,MK,MV]#Context) {

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
abstract class TableAnnotationMapper[TF <: HbaseTable[TF, TFK], TFK](val name: String, val table: TF, val includedFamilies: Seq[ColumnFamily[TF, TFK, _, _, _]] = Nil)(implicit conf: Configuration) extends TableMapper[NullWritable, Writable] {
  def row(value: QueryResult[TF, TFK], counter: (String) => Unit): Iterable[Row]

  override def map(key: ImmutableBytesWritable, value: Result, context: Mapper[ImmutableBytesWritable, Result, NullWritable, Writable]#Context) {
    def counter(name: String) {context.getCounter("Article Job", name).increment(1l)}
    val queryResult = new QueryResult[TF, TFK](value, table)
    row(queryResult, counter _).foreach(row => context.write(NullWritable.get(), row))
  }


}
