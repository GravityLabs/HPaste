package com.gravity.hbase.mapreduce

import junit.framework.TestCase
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import com.gravity.hbase.schema.{ExampleSchema, ClusterTest}
import org.apache.hadoop.io.{NullWritable, Writable, Text, LongWritable}
import com.gravity.hbase.schema._
import org.junit.Test

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

/**
 * This is an example of a simple mapper that uses the getOperations() call of a data modification chain
 * to write data to HBase in the context of a MapReduce job.
 */
class ExampleTableMapper extends Mapper[LongWritable,Text,NullWritable,Writable] {
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable,Text,NullWritable,Writable]#Context) {
    val ops = ExampleSchema.ExampleTable
            .put("Joe").value(_.title,"Joe and the Volcano")
            .put("Bill").value(_.title,"Bill and the Cheese")
            .getOperations

    ops.foreach(op=>{
      context.write(NullWritable.get(),op)
    })
  }
}

class MapReduceTest {
  @Test def testLoading() {
    val mapperInstance = Class.forName("com.gravity.hbase.mapreduce.HReducer").newInstance()
    
    println(mapperInstance.getClass.getName)
  }
}
