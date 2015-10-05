/**Licensed to Gravity.com under one
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

package com.gravity.hbase.schema

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util._
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import scala.collection._
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.joda.time.ReadableInstant
import org.apache.hadoop.hbase.filter._
import java.util

/*             )\._.,--....,'``.
.b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */



object Query2 {
  def p(depth: Int = 1, msg: Any) {
    println(("\t" * depth) + msg)
  }

  def printFilter(depth: Int, f: Filter) {
    p(depth, "Filter All Remaining: " + f.filterAllRemaining())
    p(depth, "Has Filter Row: " + f.hasFilterRow)
    p(depth, "To String: " + f.toString)
    f match {
      case fl: FilterList =>
        p(depth, "Operator: " + fl.getOperator)
        fl.getFilters.foreach(sf => printFilter(depth + 1, sf))
      case _ =>
    }
  }

}
