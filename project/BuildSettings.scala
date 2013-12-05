/*
 * Copyright (c) 2013 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
import sbt._
import Keys._

object BuildSettings {

  // Basic settings for our app
  lazy val basicSettings = Seq[Setting[_]](
    organization          :=  "Gravity",
    version               :=  "0.1.24",
    description           :=  "HBase DSL for Scala with MapReduce support",
    scalaVersion          :=  "2.10.3",
    crossScalaVersions    :=  Seq("2.9.2", "2.9.3", "2.10.0", "2.10.3"),
    scalacOptions         :=  Seq("-deprecation", "-encoding", "utf8"),
    scalacOptions in Test :=  Seq("-Yrangepos")
  )

  // TODO: add Maven Central publishing as per
  // http://www.scala-sbt.org/release/docs/Community/Using-Sonatype.html

  lazy val buildSettings = basicSettings // ++ more as required
}