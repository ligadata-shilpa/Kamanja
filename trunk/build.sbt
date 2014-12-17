import sbt._
import Keys._

sbtPlugin := true

version := "0.0.0.1"

scalaVersion := "2.10.4"

shellPrompt := { state =>  "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

net.virtualvoid.sbt.graph.Plugin.graphSettings

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

resolvers += Resolver.file("Local repo", file(System.getProperty("user.home") + "/.ivy2/local"))(Resolver.ivyStylePatterns)

val Organization = "com.ligadata"

lazy val BaseTypes = project.in(file("BaseTypes")) dependsOn(Metadata)

lazy val BaseFunctions = project.in(file("BaseFunctions")) dependsOn(Metadata)

lazy val Serialize = project.in(file("Utils/Serialize")) dependsOn(Metadata)

lazy val ZooKeeperClient   = project.in(file("Utils/ZooKeeper/CuratorClient")) dependsOn(Serialize)

lazy val ZooKeeperListener = project.in(file("Utils/ZooKeeper/CuratorListener")) dependsOn(MetadataAPI, Serialize)

lazy val OnLEPBase = project.in(file("OnLEPBase")) dependsOn(Metadata)

lazy val OnLEPManager = project.in(file("OnLEPManager")) dependsOn(Metadata, OnLEPBase, MetadataBootstrap, MetadataAPI, Serialize, ZooKeeperListener, ZooKeeperLeaderLatch)

lazy val KafkaSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/KafkaSimpleInputOutputAdapters")) dependsOn(OnLEPBase)

lazy val FileSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/FileSimpleInputOutputAdapters")) dependsOn(OnLEPBase)

lazy val SimpleEnvContextImpl = project.in(file("EnvContexts/SimpleEnvContextImpl")) dependsOn(OnLEPBase, Storage, Serialize)

lazy val Storage = project.in(file("Storage"))

lazy val Metadata = project.in(file("Metadata")) 

lazy val MessageDef = project.in(file("MessageDef")) dependsOn(Metadata,MetadataBootstrap)

lazy val LoadtestCommon = project.in(file("Tools/LoadtestCommon")) dependsOn(Storage)

lazy val LoadtestRunner = project.in(file("Tools/LoadtestRunner")) dependsOn(LoadtestCommon)

lazy val LoadtestMaster = project.in(file("Tools/LoadtestMaster")) dependsOn(LoadtestCommon)

lazy val Loadtest = project.in(file("Tools/Loadtest")) dependsOn(Storage)

lazy val PmmlRuntime = project.in(file("Pmml/PmmlRuntime")) dependsOn(Metadata, OnLEPBase) 

lazy val PmmlCompiler = project.in(file("Pmml/PmmlCompiler")) dependsOn(PmmlRuntime, PmmlUdfs, Metadata, OnLEPBase, MetadataBootstrap)

lazy val PmmlUdfs = project.in(file("Pmml/PmmlUdfs")) dependsOn(Metadata, PmmlRuntime, OnLEPBase)

lazy val MethodExtractor = project.in(file("Pmml/MethodExtractor")) dependsOn(PmmlUdfs, Metadata, OnLEPBase, Serialize)

lazy val MetadataBootstrap = project.in(file("MetadataBootstrap/Bootstrap")) dependsOn(Metadata, OnLEPBase, BaseTypes)

lazy val MetadataAPI = project.in(file("MetadataAPI")) dependsOn(Storage,Metadata,MessageDef,PmmlCompiler,Serialize,ZooKeeperClient)

lazy val MetadataAPIService = project.in(file("MetadataAPIService")) dependsOn(OnLEPBase,MetadataAPI,ZooKeeperLeaderLatch)

lazy val MetadataAPIServiceClient = project.in(file("MetadataAPIServiceClient"))

lazy val SimpleKafkaProducer = project.in(file("Utils/SimpleKafkaProducer")) dependsOn(Metadata, OnLEPBase)

lazy val KVInit = project.in(file("Utils/KVInit")) dependsOn (Metadata, OnLEPBase, MetadataBootstrap, MetadataAPI, Storage)

lazy val ZooKeeperLeaderLatch = project.in(file("Utils/ZooKeeper/CuratorLeaderLatch")) dependsOn(ZooKeeperClient)

lazy val JsonDataGen = project.in(file("Utils/JsonDataGen"))

lazy val InstallScript  = project.in(file("SampleApplication/Medical/InstallScript")) dependsOn(MetadataAPI)


