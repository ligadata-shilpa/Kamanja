import sbtassembly.AssemblyPlugin._

name := "ExtDependencyLibs2"

version := "1.0"
val kamanjaVersion = "1.4.0"


shellPrompt := { state => "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

assemblyOption in assembly ~= {
  _.copy(prependShellScript = Some(defaultShellScript))
}

//assemblyJarName in assembly := { s"${name.value}-${version.value}"}
assemblyJarName in assembly := {
  s"${name.value}_${scalaBinaryVersion.value}-${kamanjaVersion}.jar"
}


assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  // case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case PathList("META-INF", "maven", "jline", "jline", ps) if ps.startsWith("pom") => MergeStrategy.discard
  case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
  case x if x endsWith "google/common/annotations/GwtCompatible.class" => MergeStrategy.first
  case x if x endsWith "google/common/annotations/GwtIncompatible.class" => MergeStrategy.first
  case x if x endsWith "/apache/commons/beanutils/BasicDynaBean.class" => MergeStrategy.first
  //added from : JdbcDataCollector
  case x if x endsWith "com\\ligadata\\olep\\metadataload\\MetadataLoad.class" => MergeStrategy.first
  case x if x endsWith "com/ligadata/olep/metadataload/MetadataLoad.class" => MergeStrategy.first
  //
  case x if x endsWith "com\\ligadata\\kamanja\\metadataload\\MetadataLoad.class" => MergeStrategy.first
  case x if x endsWith "com/ligadata/kamanja/metadataload/MetadataLoad.class" => MergeStrategy.first
  case x if x endsWith "org/apache/commons/beanutils/BasicDynaBean.class" => MergeStrategy.last
  case x if x endsWith "com\\esotericsoftware\\minlog\\Log.class" => MergeStrategy.first
  case x if x endsWith "com\\esotericsoftware\\minlog\\Log$Logger.class" => MergeStrategy.first
  case x if x endsWith "com/esotericsoftware/minlog/Log.class" => MergeStrategy.first
  case x if x endsWith "com/esotericsoftware/minlog/Log$Logger.class" => MergeStrategy.first
  case x if x endsWith "com\\esotericsoftware\\minlog\\pom.properties" => MergeStrategy.first
  case x if x endsWith "com/esotericsoftware/minlog/pom.properties" => MergeStrategy.first
  case x if x contains "com.esotericsoftware.minlog\\minlog\\pom.properties" => MergeStrategy.first
  case x if x contains "com.esotericsoftware.minlog/minlog/pom.properties" => MergeStrategy.first
  case x if x contains "org\\objectweb\\asm\\" => MergeStrategy.last
  case x if x contains "org/objectweb/asm/" => MergeStrategy.last
  case x if x contains "org/apache/commons/collections" => MergeStrategy.last
  case x if x contains "org\\apache\\commons\\collections" => MergeStrategy.last
  case x if x contains "com.fasterxml.jackson.core" => MergeStrategy.first
  case x if x contains "com/fasterxml/jackson/core" => MergeStrategy.first
  // newly added
  case x if x contains "StaticLoggerBinder.class" => MergeStrategy.first
  case x if x contains "StaticMDCBinder.class" => MergeStrategy.first
  case x if x contains "StaticMarkerBinder.class" => MergeStrategy.first
  case x if x contains "package-info.class" => MergeStrategy.first
  case x if x contains "HTMLDOMImplementation.class" => MergeStrategy.first
  //
  case x if x contains "com\\fasterxml\\jackson\\core" => MergeStrategy.first
  case x if x contains "commons-logging" => MergeStrategy.first
  case "log4j.properties" => MergeStrategy.first
  case "unwanted.txt" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)

}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  val excludes = Set("commons-beanutils-1.7.0.jar", "google-collections-1.0.jar", "commons-collections4-4.0.jar", "log4j-1.2.17.jar", "commons-beanutils-1.8.3.jar")
  cp filter { jar => excludes(jar.data.getName) }
}
//"log4j-1.2.17.jar", "log4j-1.2.16.jar", "commons-collections-4-4.0.jar", "scalatest_2.11-2.2.0.jar"
//, "scala-reflect-2.11.0.jar", "akka-actor_2.11-2.3.2.jar", "scala-reflect-2.11.2.jar", "scalatest_2.11-2.2.4.jar", "joda-time-2.9.1-javadoc.jar", "voldemort-0.96.jar", "scala-compiler-2.11.0.jar", "guava-14.0.1.jar"
//,"minlog-1.2.jar"
//net.virtualvoid.sbt.graph.Plugin.graphSettings


////////////////////// ZooKeeperListener
libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-collections4" % "4.0",
  "commons-configuration" % "commons-configuration" % "1.7",
  "commons-logging" % "commons-logging" % "1.1.1",
  "org.apache.curator" % "curator-client" % "2.6.0",
  "org.apache.curator" % "curator-framework" % "2.6.0",
  "org.apache.curator" % "curator-recipes" % "2.6.0"
  //  "com.googlecode.json-simple" % "json-simple" % "1.1"
)



////////////////////// CleanUtil
libraryDependencies ++= Seq(
  "com.101tec" % "zkclient" % "0.6",
  "org.apache.curator" % "curator-test" % "2.8.0"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
)



////////////////////// OutputMsgDef
libraryDependencies ++= Seq("junit" % "junit" % "4.8.1" % "test")



////////////////////// MetadataAPIServiceClient
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")
libraryDependencies ++= Seq(
  //    "org.slf4j" % "slf4j-api" % "1.7.10",
  "ch.qos.logback" % "logback-core" % "1.0.13",
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "uk.co.bigbeeconsultants" %% "bee-client" % "0.28.0",
  "org.apache.httpcomponents" % "httpclient" % "4.1.2"
)
resolvers += "Big Bee Consultants" at "http://repo.bigbeeconsultants.co.uk/repo"



////////////////////// MetadataAPIService
//scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")
resolvers ++= Seq(
  "spray repo" at "http://repo.spray.io/"
)
libraryDependencies ++= {
  val sprayVersion = "1.3.3"
  val akkaVersion = "2.3.9"
  Seq(
    "io.spray" %% "spray-can" % sprayVersion,
    "io.spray" %% "spray-routing" % sprayVersion,
    "io.spray" %% "spray-testkit" % sprayVersion,
    "io.spray" %% "spray-client" % sprayVersion,
    "io.spray" %% "spray-json" % "1.3.2",
    "com.typesafe.akka" %% "akka-actor" % akkaVersion,
    //  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
    //    "ch.qos.logback" % "logback-classic" % "1.0.12",                       latest change
    "org.apache.camel" % "camel-core" % "2.9.2"
  )
}



////////////////////// Cassandra / StorageCassandra
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-parent" % "2.1.2"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.2"
libraryDependencies += "org.apache.cassandra" % "cassandra-thrift" % "2.0.3"
//libraryDependencies += "commons-dbcp" % "commons-dbcp" % "1.4"                                 //two
libraryDependencies += "org.apache.thrift" % "libthrift" % "0.9.2"



////////////////////// HashMap
libraryDependencies += "org.mapdb" % "mapdb" % "1.0.6"


////////////////////// SqlServer
libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.1" // one


//////////////////////  jtm
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" //% "test->default"
libraryDependencies += "com.google.code.gson" % "gson" % "2.5"
libraryDependencies += "org.rogach" %% "scallop" % "0.9.5"
//libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"        // use this instead ? "commons-io" % "commons-io" % "2.4"
libraryDependencies += "org.skyscreamer" % "jsonassert" % "1.3.0" //% "test->default"
libraryDependencies += "org.aicer.grok" % "grok" % "0.9.0"
//libraryDependencies += "com.novocode" % "junit-interface" % "0.9" //% "test->default"
//libraryDependencies += "junit" % "junit" % "4.11" % "test->default"


//////////////////////  PmmlTestTool
libraryDependencies += "org.jpmml" % "pmml-evaluator" % "1.2.9"



//////////////////////  SmartFileAdapter
libraryDependencies ++= {
  val sprayVersion = "1.3.3"
  val akkaVersion = "2.3.9"
  Seq(
    //"org.apache.kafka" %% "kafka" % "0.8.2.2",
    //"org.scala-lang" % "scala-actors" % scalaVersion,
    "org.apache.commons" % "commons-lang3" % "3.4",
    "org.apache.tika" % "tika-core" % "1.11",
    "jmimemagic" % "jmimemagic" % "0.1.2"
  )
}
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.1"
libraryDependencies += "org.apache.commons" % "commons-vfs2" % "2.0"
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.5"
libraryDependencies += "org.anarres.lzo" % "lzo-core" % "1.0.0"
