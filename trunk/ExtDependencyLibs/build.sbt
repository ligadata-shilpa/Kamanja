import sbtassembly.AssemblyPlugin._

name := "ExtDependencyLibs"

version := "1.0"
val kamanjaVersion = "1.4.0"


shellPrompt := { state => "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

assemblyOption in assembly ~= {
  _.copy(prependShellScript = Some(defaultShellScript))
}

//assemblyJarName in assembly := { s"${name.value}-${version.value}"}
assemblyJarName in assembly := {
  s"${name.value}_${scalaBinaryVersion.value}-${kamanjaVersion}"
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
  case x if x contains "com\\fasterxml\\jackson\\core" => MergeStrategy.first
  case x if x contains "commons-logging" => MergeStrategy.first
  case "log4j.properties" => MergeStrategy.first
  case "unwanted.txt" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)

}

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
  val excludes = Set("commons-beanutils-1.7.0.jar", "google-collections-1.0.jar", "commons-collections4-4.0.jar", "log4j-1.2.17.jar")
  cp filter { jar => excludes(jar.data.getName) }
}
//"log4j-1.2.17.jar", "log4j-1.2.16.jar", "commons-collections-4-4.0.jar", "scalatest_2.11-2.2.0.jar"
//, "scala-reflect-2.11.0.jar", "akka-actor_2.11-2.3.2.jar", "scala-reflect-2.11.2.jar", "scalatest_2.11-2.2.4.jar", "joda-time-2.9.1-javadoc.jar", "voldemort-0.96.jar", "scala-compiler-2.11.0.jar", "guava-14.0.1.jar"
//,"minlog-1.2.jar"
//net.virtualvoid.sbt.graph.Plugin.graphSettings


/////////////////////// KamanjaManager
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
libraryDependencies += "org.apache.logging.log4j" % "log4j-1.2-api" % "2.4.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.4.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.4.1"
libraryDependencies += "org.ow2.asm" % "asm-tree" % "4.0"
libraryDependencies += "org.ow2.asm" % "asm-commons" % "4.0"
libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-actors" % _) // ???
libraryDependencies += "org.scala-lang" % "scala-actors" % scalaVersion.value

/////////////////////// MetadataAPI
libraryDependencies += "org.joda" % "joda-convert" % "1.6"
libraryDependencies += "joda-time" % "joda-time" % "2.8.2"
//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.0"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.9"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.9"
libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.6"
libraryDependencies += "org.apache.curator" % "apache-curator" % "2.0.0-incubating"
libraryDependencies += "com.google.guava" % "guava" % "14.0.1"
//libraryDependencies += "org.jpmml" % "pmml-evaluator" % "1.2.4"                               // another version exists
//libraryDependencies += "org.jpmml" % "pmml-model" % "1.2.5"
//libraryDependencies += "org.jpmml" % "pmml-schema" % "1.2.5"
dependencyOverrides += "com.google.guava" % "guava" % "14.0.1"
libraryDependencies += "commons-codec" % "commons-codec" % "1.10"
libraryDependencies += "commons-io" % "commons-io" % "2.4"
libraryDependencies ++= Seq(
  "com.twitter" %% "chill" % "0.5.0",
  "org.apache.shiro" % "shiro-core" % "1.2.3",
  "org.apache.shiro" % "shiro-root" % "1.2.3"
)
//scalacOptions += "-deprecation"
//retrieveManaged := true
//parallelExecution := false


/////////////////////////////////// from /trunk/build.sbt
libraryDependencies := {
  CrossVersion.partialVersion(scalaVersion.value) match {
    // if scala 2.11+ is used, quasiquotes are merged into scala-reflect
    case Some((2, scalaMajor)) if scalaMajor >= 11 =>
      libraryDependencies.value ++ Seq("org.scalameta" %% "scalameta" % "0.0.3")
    // libraryDependencies.value
    // in Scala 2.10, quasiquotes are provided by macro paradise
    case Some((2, 10)) =>
      libraryDependencies.value ++ Seq("org.scalamacros" %% "quasiquotes" % "2.1.0")
    //libraryDependencies.value ++ Seq(
    //compilerPlugin("org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full),
    //"org.scalamacros" %% "quasiquotes" % "2.1.0" cross CrossVersion.binary)
  }
}

/////////////////////// SimpleKafkaProducer
resolvers += "Apache repo" at "https://repository.apache.org/content/repositories/releases"
libraryDependencies ++= Seq("org.apache.kafka" %% "kafka" % "0.8.2.2"
  exclude("javax.jms", "jms")
  exclude("com.sun.jdmk", "jmxtools")
  exclude("com.sun.jmx", "jmxri")
)


/////////////////////// GetComponent
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.0.2"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.0.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1"
libraryDependencies += "com.googlecode.json-simple" % "json-simple" % "1.1"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.3"
libraryDependencies += "log4j" % "log4j" % "1.2.17"
//scalacOptions += "-deprecation"


////////////////////// NodeInfoExtract


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
    "ch.qos.logback" % "logback-classic" % "1.0.12",
    "org.apache.camel" % "camel-core" % "2.9.2"
  )
}





/////////////////////// PmmlTestTool
// 1.2.9 is currently used in other engine... use same here
libraryDependencies += "org.jpmml" % "pmml-evaluator" % "1.2.9"
libraryDependencies += "org.jpmml" % "pmml-model" % "1.2.9"
libraryDependencies += "org.jpmml" % "pmml-schema" % "1.2.9"
libraryDependencies += "com.beust" % "jcommander" % "1.48"
libraryDependencies += "com.codahale.metrics" % "metrics-core" % "3.0.2"
//libraryDependencies += "org.glassfish.jaxb" % "jaxb-runtime" % "2.2.11"
//// Do not append Scala versions to the generated artifacts
//crossPaths := false
//// This forbids including Scala related libraries into the dependency
//autoScalaLibrary := false


/////////////////////// ClusterInstallerDriver
//// Do not append Scala versions to the generated artifacts
//crossPaths := false
//// This forbids including Scala related libraries into the dependency
//autoScalaLibrary := false


/////////////////////// MigrateManager
// should this be excluded from here ?
//// Do not append Scala versions to the generated artifacts
//crossPaths := false
//// This forbids including Scala related libraries into the dependency
//autoScalaLibrary := false
//libraryDependencies += "com.google.code.gson" % "gson" % "2.3.1"


/////////////////////// InstallerDriver
//already available
//scalacOptions += "-deprecation"


//////////////////////// KVInit
//already available


////////////////////// JdbcDataCollector
//already available


////////////////////// CleanUtil
//already available


////////////////////// FileDataConsumer
//already available
//libraryDependencies ++= {
//  val sprayVersion = "1.3.3"
//  val akkaVersion = "2.3.9"
//  Seq(
//    "org.apache.kafka" %% "kafka" % "0.8.2.2",
//    "org.scala-lang" % "scala-actors" % scalaVersion.value
//  )
//}


////////////////////// Metadata
//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.0" % "test"
libraryDependencies += "com.novocode" % "junit-interface" % "0.11-RC1" % "test"
//testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v")


////////////////////// KamanjaBase
libraryDependencies += "com.google.code.findbugs" % "jsr305" % "1.3.9"


////////////////////// Bootstrap
//scalacOptions += "-deprecation"
unmanagedSourceDirectories in Compile <+= (scalaVersion, sourceDirectory in Compile) {
  case (v, dir) if v startsWith "2.10" => dir / "scala_2.10"
  case (v, dir) if v startsWith "2.11" => dir / "scala_2.11"
}




////////////////////// Serialize
libraryDependencies ++= Seq(
  "com.twitter" %% "chill" % "0.5.0"
)
libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.6.0"
//scalacOptions += "-deprecation"


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




////////////////////// KafkaSimpleInputOutputAdapters
//already available


////////////////////// ZooKeeperLeaderLatch
//already available


////////////////////// Exceptions
//already available


////////////////////// KamanjaUtils
//already available


////////////////////// TransactionService
//already available


////////////////////// DataDelimiters
//already available


////////////////////// InputOutputAdapterBase
//already available


////////////////////// StorageManager
//already available


////////////////////// MessageDef
//not sure if needed
//libraryDependencies += "metadata" %% "metadata" % "1.0"


////////////////////// PmmlCompiler


////////////////////// ZooKeeperClient
//already available


////////////////////// OutputMsgDef
libraryDependencies ++= Seq("junit" % "junit" % "4.8.1" % "test")


////////////////////// SecurityAdapterBase
//already available


////////////////////// HeartBeat
//already available


////////////////////// JpmmlFactoryOfModelInstanceFactory
//already available


////////////////////// SimpleApacheShiroAdapter
//already available


////////////////////// KVInit
//already available


////////////////////// MetadataBootstrap
//unmanagedSourceDirectories in Compile <+= (scalaVersion, sourceDirectory in Compile) {
//  case (v, dir) if v startsWith "2.10" => dir / "scala_2.10"
//  case (v, dir) if v startsWith "2.11" => dir / "scala_2.11"
//}


////////////////////// BaseTypes
//already available


////////////////////// BaseFunctions
//already available


////////////////////// FileSimpleInputOutputAdapters"
//already available


////////////////////// SimpleEnvContextImpl
//already available


////////////////////// StorageBase
//already available


////////////////////// GenericMsgCompiler
//already available


////////////////////// PmmlRuntime


////////////////////// PmmlUdfs
//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4"


////////////////////// MethodExtractor


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





////////////////////// JsonDataGen
//already available


////////////////////// Controller
//already available


////////////////////// AuditAdapterBase / AuditAdapters
//already available


////////////////////// CustomUdfLib
//already available


////////////////////// ExtractData
//already available


//////////////////////InterfacesSamples
//already available


////////////////////// Cassandra / StorageCassandra
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-parent" % "2.1.2"
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.2"
libraryDependencies += "org.apache.cassandra" % "cassandra-thrift" % "2.0.3"
libraryDependencies += "commons-dbcp" % "commons-dbcp" % "1.4"
libraryDependencies += "org.apache.thrift" % "libthrift" % "0.9.2"




////////////////////// HashMap
libraryDependencies += "org.mapdb" % "mapdb" % "1.0.6"






////////////////////// HBase
//already available


////////////////////// TreeMap
//already available


////////////////////// SqlServer
libraryDependencies += "org.apache.commons" % "commons-dbcp2" % "2.1"





////////////////////// UtilityService
//already available


////////////////////// KvBase
//already available


////////////////////// SaveContainerDataComponent
//already available


////////////////////// UtilsForModels
//already available


////////////////////// JarFactoryOfModelInstanceFactory
//already available


////////////////////// MigrateBase
//already available


//////////////////////  InstallDriverBase
//already available


//////////////////////  PmmlTestTool
libraryDependencies += "org.jpmml" % "pmml-evaluator" % "1.2.9"



//////////////////////  InstallDriver
//already available


//////////////////////  MigrateTo_V_1_3
//already available


//////////////////////  MigrateTo_V_1_4
//already available


//////////////////////  jtm
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" //% "test->default"
libraryDependencies += "com.google.code.gson" % "gson" % "2.5"
libraryDependencies += "org.rogach" %% "scallop" % "0.9.5"
libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"        // use this instead ? "commons-io" % "commons-io" % "2.4"
libraryDependencies += "org.skyscreamer" % "jsonassert" % "1.3.0" //% "test->default"
libraryDependencies += "org.aicer.grok" % "grok" % "0.9.0"
//libraryDependencies += "com.novocode" % "junit-interface" % "0.9" //% "test->default"
//libraryDependencies += "junit" % "junit" % "4.11" % "test->default"