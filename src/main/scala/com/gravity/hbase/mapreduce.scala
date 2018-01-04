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

package com.gravity.hbase

import java.util.Random

package object mapreduce {
  type MapperFunc[MK,MV,MOK,MOV, S <: SettingsBase] = (HMapContext[MK,MV,MOK,MOV,S]) => Unit

  type ReducerFunc[MOK,MOV,ROK,ROV, S<:SettingsBase] = (HReduceContext[MOK,MOV,ROK,ROV,S]) => Unit

  def genTmpFile: String = "/tmp/htemp-" + new Random().nextInt(Int.MaxValue)
}