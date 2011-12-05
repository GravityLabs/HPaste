package com.gravity.hbase.mapreduce

import junit.framework.TestCase
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import com.gravity.hbase.schema.{ExampleSchema, ClusterTest}
import com.gravity.hbase.schema._
import org.junit.Test
import org.apache.hadoop.io._

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */

//class TestJob extends HJobND[BytesWritable,BytesWritable,NoSettings] {
//
//  addMapper(new HMapper{
//    def map() {
//    }
//  })
////  val task1 = addTask(
////    HMapReduceTask(
////      HTaskID("Hi"),
////      HTaskConfigs(),
////      HIO(),
////      new HMapper() {
////        def map(){}
////      },
////      new HReducer() {
////        def reduce() {}
////      }
////    )
////  )
//}