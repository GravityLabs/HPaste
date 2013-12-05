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

import sbt._

object Dependencies {

  object V {
    // Java
    val jodaTime    = "1.6.1"
    val hadoopCore  = "0.20.2"
    val hbase       = "0.90.4"
    val asyncHbase  = "1.4.0"
    val trove4j     = "3.0.3"
    // Java (test only)
    val junit       = "4.8.1"
    val hadoopTest  = "0.20.2"
  }

  object Libraries {
    // Java
    val jodaTime    = "joda-time"         % "joda-time"   % V.jodaTime
    val hadoopCore  = "org.apache.hadoop" % "hadoop-core" % V.hadoopCore
    val hbase       = "org.apache.hbase"  % "hbase"       % V.hbase
    val asyncHbase  = "org.hbase"         % "asynchbase"  % V.asyncHbase   
    val trove4j     = "net.sf.trove4j"    % "trove4j"     % V.trove4j
    // Java (test only)
    val junit       = "junit"             % "junit"       % V.junit        % "test"
    val hadoopTest  = "org.apache.hadoop" % "hadoop-test" % V.hadoopTest   % "test"
    val hbaseTests  = "org.apache.hbase"  % "hbase"       % V.hbase        classifier "tests"
    // To exclude. TODO: not yet implemented
    val exclusions = List(
                      "org.apache.thrift" , "thrift"           ,
                      "org.jruby"         , "jruby-complete"   ,
                      "org.slf4j"         , "log4j-over-slf4j" ,
                      "org.slf4j"         , "jcl-over-slf4j"   )

  }
}
