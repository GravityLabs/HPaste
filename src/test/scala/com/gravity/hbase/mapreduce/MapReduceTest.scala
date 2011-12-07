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