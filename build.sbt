ghpages.settings

git.remoteRepo := "git@github.com:GravityLabs/HPaste.git"

enablePlugins(SiteScaladocPlugin)

organization := "com.gravity"

name := "gravity-hpaste_2.11"

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.9.2", "2.9.3", "2.10.0", "2.10.3", "2.11.7")

releaseCrossBuild := true

releasePublishArtifactsAction := PgpKeys.publishSigned.value

javacOptions ++= Seq("-Xlint:unchecked", "-source", "1.7", "-target", "1.7")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-target:jvm-1.7", "-feature", "-language:_")

resolvers += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "12.0.1",
  "joda-time" % "joda-time" % "2.3",
  "net.sf.trove4j" % "trove4j" % "3.0.3",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0-mr1-cdh5.5.2" % "provided",
  "org.apache.hbase" % "hbase-client" % "1.0.0-cdh5.5.2" % "provided",
  "org.apache.hbase" % "hbase-common" % "1.0.0-cdh5.5.2" % "provided",
  "org.apache.hbase" % "hbase-server" % "1.0.0-cdh5.5.2" % "provided",
  "org.hbase" % "asynchbase" % "1.7.2" exclude("org.slf4j", "log4j-over-slf4j") exclude("com.google.guava", "guava"),
  "junit" % "junit" % "4.8.1",
  "com.novocode" % "junit-interface" % "0.10" % "test",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0-cdh5.5.2" % "test",
  "org.apache.hadoop" % "hadoop-common" % "2.6.0-cdh5.5.2" % "test" classifier "tests",
  "org.apache.hadoop" % "hadoop-minicluster" % "2.6.0-mr1-cdh5.5.2" % "test",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0-cdh5.5.2" % "test",
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0-cdh5.5.2" % "test" classifier "tests",
  "org.apache.hbase" % "hbase-client" % "1.0.0-cdh5.5.2" % "test" classifier "tests",
  "org.apache.hbase" % "hbase-common" % "1.0.0-cdh5.5.2" % "test" classifier "tests",
  "org.apache.hbase" % "hbase-hadoop-compat" % "1.0.0-cdh5.5.2" % "test" classifier "tests",
  "org.apache.hbase" % "hbase-hadoop2-compat" % "1.0.0-cdh5.5.2" % "test" classifier "tests",
  "org.apache.hbase" % "hbase-server" % "1.0.0-cdh5.5.2" % "test" classifier "tests",
  "org.apache.hbase" % "hbase-testing-util" % "1.0.0-cdh5.5.2" % "test" exclude("org.jruby", "jruby-complete")
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF")

publishMavenStyle := true

publishArtifact in Test := false

autoAPIMappings := true

pomIncludeRepository := { _ => false }

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if(isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

pomExtra :=
  <url>https://github.com/GravityLabs/HPaste</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        <distribution>repo</distribution>
        <comments>A business-friendly OSS license</comments>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:GravityLabs/HPaste.git</url>
      <connection>scm:git:git@github.com:GravityLabs/HPaste.git</connection>
    </scm>
    <developers>
      <developer>
        <id>Lemmsjid</id>
        <name>Chris Bissell</name>
        <email>chris@gravity.com</email>
        <url>http://github.com/Lemmsjid</url>
        <organization>Gravity</organization>
        <organizationUrl>http://www.gravity.com/</organizationUrl>
        <roles>
          <role>developer</role>
          <role>architect</role>
        </roles>
        <timezone>-8</timezone>
      </developer>
      <developer>
        <id>erraggy</id>
        <name>Robbie Coleman</name>
        <email>robbie@gravity.com</email>
        <url>http://robbie.robnrob.com/</url>
        <organization>Gravity</organization>
        <organizationUrl>http://www.gravity.com/</organizationUrl>
        <roles>
          <role>developer</role>
        </roles>
        <timezone>-8</timezone>
        <properties>
          <picUrl>http://1.gravatar.com/avatar/dc77b368ec1f077dcc4aca3b9c003d2d</picUrl>
        </properties>
      </developer>
    </developers>