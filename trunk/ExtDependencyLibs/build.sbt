name := "ExtDependencyLibs"

version := "1.0"

// CustomUdfLib

// JarFactoryOfModelInstanceFactory


// BaseFunctions : found in KamanjaDependencyLibs
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

// KafkaSimpleInputOutputAdapters : found in KamanjaDependencyLibs
//resolvers += "Apache repo" at "https://repository.apache.org/content/repositories/releases"
//libraryDependencies += "org.apache.logging.log4j" % "log4j-1.2-api" % "2.4.1"
//libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.4.1"
//libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.4.1"
//libraryDependencies ++= Seq("org.apache.kafka" %% "kafka" % "0.8.2.2"
//  exclude("javax.jms", "jms")
//  exclude("com.sun.jdmk", "jmxtools")
//  exclude("com.sun.jmx", "jmxri")
//)
//libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-actors" % _)
//libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.2.9"
//libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.9"


// FileSimpleInputOutputAdapters


// SimpleEnvContextImpl
//libraryDependencies ++= Seq("junit" % "junit" % "4.8.1" % "test")             // found in OutputMessageDef:KamanjaDependencyLibs
//libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.0" % "test"      // found in metadata:KamanjaDependencyLibs


// GenericMsgCompiler
//libraryDependencies += "metadata" %% "metadata" % "1.0"                       // found in MessageDef:KamanjaDependencyLibs



// MetadataAPIServiceClient
scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")
resolvers += "Big Bee Consultants" at "http://repo.bigbeeconsultants.co.uk/repo"
libraryDependencies ++= Seq(
  //    "org.slf4j" % "slf4j-api" % "1.7.10",
  "ch.qos.logback" % "logback-core" % "1.0.13",
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "uk.co.bigbeeconsultants" %% "bee-client" % "0.28.0",
  "org.apache.httpcomponents" % "httpclient" % "4.1.2"
)
resolvers += "Big Bee Consultants" at "http://repo.bigbeeconsultants.co.uk/repo"




// AuditAdapters
resolvers += "spring-milestones" at "http://repo.springsource.org/libs-milestone"
//libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-parent" % "2.1.2"       //  found in Cassandra:KamanjaDependencyLibs
//libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.2"			//  found in Cassandra:KamanjaDependencyLibs
//libraryDependencies += "com.novocode" % "junit-interface" % "0.11-RC1" % "test"				// found in metadata:KamanjaDependencyLibs
//libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.0.2"     					// found in GetComponent
//libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.0.2"						// found in GetComponent
//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1"						// found in GetComponent
//libraryDependencies += "org.mapdb" % "mapdb" % "1.0.6"                                      // found in Hashmap
libraryDependencies += "commons-codec" % "commons-codec" % "1.9"							// % "1.10" in MetadataAPI
libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.3.0"					// % "2.6.0" in Serialize
libraryDependencies += "commons-lang" % "commons-lang" % "2.4"
libraryDependencies += "org.jdom" % "jdom" % "1.1"
libraryDependencies += "voldemort" % "voldemort" % "0.96"
libraryDependencies ++= Seq("net.debasishg" %% "redisclient" % "2.13")


// InterfacesSamples : MetadataAPI:KamanjaDependencyLibs
//libraryDependencies += "com.google.guava" % "guava" % "14.0.1"


////////////////// these are commented in assembly in EasyInstallKamanja.sh
// SaveContainerDataComponent
// MethodExtractor
// ExtractData
libraryDependencies += "com.google.code.gson" % "gson" % "2.3.1"

///////////////////////////////////////////////////////////



////////////////// assembly not found in EasyInstallKamanja.sh
// UtilsForModels
// Controller
// JsonDataGen


// UtilityService
//scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")
resolvers ++= Seq("spray repo" at "http://repo.spray.io/")
libraryDependencies ++= {
  val sprayVersion = "1.3.3"
  val akkaVersion = "2.3.9"
  Seq(
    "com.typesafe.akka" %% "akka-actor" % akkaVersion
//    "io.spray" %% "spray-can" % sprayVersion,			// MetadataAPIService
//    "io.spray" %% "spray-routing" % sprayVersion,			// MetadataAPIService
//    "io.spray" %% "spray-testkit" % sprayVersion,			// MetadataAPIService
//    "io.spray" %% "spray-client" % sprayVersion,			// MetadataAPIService
//    "io.spray" %%  "spray-json" % "1.3.2",			// MetadataAPIService
//    "ch.qos.logback" % "logback-classic" % "1.0.12",		// MetadataAPIService
//    "org.apache.camel" % "camel-core" % "2.9.2",			// MetadataAPIService
//    "org.apache.kafka" %% "kafka" % "0.8.2.2"				// SimpleKafkaProducer
//      exclude("javax.jms", "jms")						// SimpleKafkaProducer
//      exclude("com.sun.jdmk", "jmxtools")				// SimpleKafkaProducer
//      exclude("com.sun.jmx", "jmxri")					// SimpleKafkaProducer
  )
}