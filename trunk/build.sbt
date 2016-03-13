import sbt._
import Keys._
import UnidocKeys._

sbtPlugin := true

version := "0.0.0.1"

//scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.11.7", "2.10.4")

shellPrompt := { state =>  "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

net.virtualvoid.sbt.graph.Plugin.graphSettings

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

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

resolvers += Resolver.file("Local repo", file(System.getProperty("user.home") + "/.ivy2/local"))(Resolver.ivyStylePatterns)

resolvers += Resolver.typesafeRepo("releases")

resolvers += Resolver.sonatypeRepo("releases")

coverageEnabled in ThisBuild := true

val Organization = "com.ligadata"

//newly added
lazy val KamanjaDependencyLibs = project.in(file("KamanjaDependencyLibs"))

lazy val ExtDependencyLibs = project.in(file("ExtDependencyLibs"))

lazy val BaseTypes = project.in(file("BaseTypes")) dependsOn(ExtDependencyLibs)

lazy val BaseFunctions = project.in(file("BaseFunctions")) dependsOn(ExtDependencyLibs)

lazy val Serialize = project.in(file("Utils/Serialize")) dependsOn(ExtDependencyLibs)

lazy val ZooKeeperClient = project.in(file("Utils/ZooKeeper/CuratorClient")) dependsOn(ExtDependencyLibs)

lazy val ZooKeeperListener = project.in(file("Utils/ZooKeeper/CuratorListener")) dependsOn(ExtDependencyLibs)

lazy val KamanjaVersion = project.in(file("KamanjaVersion"))

lazy val Exceptions = project.in(file("Exceptions")) dependsOn(KamanjaVersion)

lazy val KamanjaBase = project.in(file("KamanjaBase")) dependsOn(ExtDependencyLibs)

lazy val DataDelimiters = project.in(file("DataDelimiters"))

lazy val KamanjaManager = project.in(file("KamanjaManager")) dependsOn(KamanjaDependencyLibs, ExtDependencyLibs)

lazy val InputOutputAdapterBase = project.in(file("InputOutputAdapters/InputOutputAdapterBase")) dependsOn(ExtDependencyLibs)

// lazy val IbmMqSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/IbmMqSimpleInputOutputAdapters")) dependsOn(InputOutputAdapterBase, Exceptions, DataDelimiters)

lazy val KafkaSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/KafkaSimpleInputOutputAdapters")) dependsOn(ExtDependencyLibs)

lazy val FileSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/FileSimpleInputOutputAdapters")) dependsOn(ExtDependencyLibs)

lazy val SimpleEnvContextImpl = project.in(file("EnvContexts/SimpleEnvContextImpl")) dependsOn(ExtDependencyLibs)

lazy val StorageBase = project.in(file("Storage/StorageBase")) dependsOn(ExtDependencyLibs)

lazy val Metadata = project.in(file("Metadata")) dependsOn(ExtDependencyLibs)

lazy val OutputMsgDef  = project.in(file("OutputMsgDef")) dependsOn(ExtDependencyLibs)

lazy val MessageDef = project.in(file("MessageDef")) dependsOn(ExtDependencyLibs)

lazy val GenericMsgCompiler = project.in(file("GenericMsgCompiler")) dependsOn(ExtDependencyLibs)

// lazy val LoadtestCommon = project.in(file("Tools/LoadtestCommon")) dependsOn(StorageManager, Exceptions)

// lazy val LoadtestRunner = project.in(file("Tools/LoadtestRunner")) dependsOn(LoadtestCommon, Exceptions)

// lazy val LoadtestMaster = project.in(file("Tools/LoadtestMaster")) dependsOn(LoadtestCommon, Exceptions)

// lazy val Loadtest = project.in(file("Tools/Loadtest")) dependsOn(StorageManager, Exceptions)

lazy val PmmlRuntime = project.in(file("Pmml/PmmlRuntime")) dependsOn(ExtDependencyLibs)

lazy val PmmlCompiler = project.in(file("Pmml/PmmlCompiler")) dependsOn(ExtDependencyLibs)

lazy val PmmlUdfs = project.in(file("Pmml/PmmlUdfs")) dependsOn(ExtDependencyLibs)

lazy val MethodExtractor = project.in(file("Pmml/MethodExtractor")) dependsOn(ExtDependencyLibs)

lazy val MetadataAPI = project.in(file("MetadataAPI")) dependsOn(ExtDependencyLibs)

lazy val MetadataBootstrap = project.in(file("MetadataBootstrap/Bootstrap")) dependsOn(ExtDependencyLibs)

lazy val MetadataAPIService = project.in(file("MetadataAPIService")) dependsOn(KamanjaDependencyLibs, ExtDependencyLibs)

lazy val MetadataAPIServiceClient = project.in(file("MetadataAPIServiceClient")) dependsOn(ExtDependencyLibs)

lazy val SimpleKafkaProducer = project.in(file("Utils/SimpleKafkaProducer")) dependsOn(ExtDependencyLibs)

lazy val KVInit = project.in(file("Utils/KVInit")) dependsOn (KamanjaDependencyLibs, ExtDependencyLibs)

lazy val ZooKeeperLeaderLatch = project.in(file("Utils/ZooKeeper/CuratorLeaderLatch")) dependsOn(ExtDependencyLibs)

lazy val JsonDataGen = project.in(file("Utils/JsonDataGen")) dependsOn(ExtDependencyLibs)

lazy val NodeInfoExtract  = project.in(file("Utils/NodeInfoExtract")) dependsOn(KamanjaDependencyLibs, ExtDependencyLibs)

lazy val Controller = project.in(file("Utils/Controller")) dependsOn(ExtDependencyLibs)

lazy val SimpleApacheShiroAdapter = project.in(file("Utils/Security/SimpleApacheShiroAdapter")) dependsOn(ExtDependencyLibs)

lazy val AuditAdapters = project.in(file("Utils/Audit")) dependsOn(ExtDependencyLibs)

lazy val CustomUdfLib = project.in(file("SampleApplication/CustomUdfLib")) dependsOn(ExtDependencyLibs)

lazy val JdbcDataCollector = project.in(file("Utils/JdbcDataCollector")) dependsOn(ExtDependencyLibs)

lazy val ExtractData = project.in(file("Utils/ExtractData")) dependsOn(KamanjaDependencyLibs, ExtDependencyLibs)

lazy val InterfacesSamples = project.in(file("SampleApplication/InterfacesSamples")) dependsOn(KamanjaDependencyLibs, ExtDependencyLibs)

lazy val StorageCassandra = project.in(file("Storage/Cassandra")) dependsOn(ExtDependencyLibs)

lazy val StorageHashMap = project.in(file("Storage/HashMap")) dependsOn(ExtDependencyLibs)

lazy val StorageHBase = project.in(file("Storage/HBase")) dependsOn(ExtDependencyLibs)

// lazy val StorageRedis = project.in(file("Storage/Redis")) dependsOn(StorageBase, Exceptions, KamanjaUtils)

lazy val StorageTreeMap = project.in(file("Storage/TreeMap")) dependsOn(ExtDependencyLibs)

// lazy val StorageVoldemort = project.in(file("Storage/Voldemort")) dependsOn(StorageBase, Exceptions, KamanjaUtils)

lazy val StorageSqlServer = project.in(file("Storage/SqlServer")) dependsOn(ExtDependencyLibs)

// lazy val StorageMySql = project.in(file("Storage/MySql")) dependsOn(StorageBase, Serialize, Exceptions, KamanjaUtils)

lazy val StorageManager = project.in(file("Storage/StorageManager")) dependsOn(ExtDependencyLibs)

lazy val AuditAdapterBase = project.in(file("AuditAdapters/AuditAdapterBase")) dependsOn(ExtDependencyLibs)

lazy val SecurityAdapterBase = project.in(file("SecurityAdapters/SecurityAdapterBase")) dependsOn(ExtDependencyLibs)

lazy val KamanjaUtils = project.in(file("KamanjaUtils")) dependsOn(ExtDependencyLibs)

lazy val UtilityService = project.in(file("Utils/UtilitySerivce")) dependsOn(ExtDependencyLibs)

lazy val HeartBeat = project.in(file("HeartBeat")) dependsOn(ExtDependencyLibs)

lazy val TransactionService = project.in(file("TransactionService")) dependsOn(ExtDependencyLibs)

lazy val KvBase = project.in(file("KvBase"))

lazy val FileDataConsumer = project.in(file("FileDataConsumer")) dependsOn(KamanjaDependencyLibs, ExtDependencyLibs)

lazy val CleanUtil = project.in(file("Utils/CleanUtil")) dependsOn(KamanjaDependencyLibs, ExtDependencyLibs)

lazy val SaveContainerDataComponent = project.in(file("Utils/SaveContainerDataComponent")) dependsOn (KamanjaDependencyLibs, ExtDependencyLibs)

lazy val UtilsForModels = project.in(file("Utils/UtilsForModels"))

lazy val JarFactoryOfModelInstanceFactory = project.in(file("FactoriesOfModelInstanceFactory/JarFactoryOfModelInstanceFactory")) dependsOn (ExtDependencyLibs)

lazy val JpmmlFactoryOfModelInstanceFactory = project.in(file("FactoriesOfModelInstanceFactory/JpmmlFactoryOfModelInstanceFactory")) dependsOn (ExtDependencyLibs)

lazy val MigrateBase = project.in(file("Utils/Migrate/MigrateBase"))

lazy val MigrateFrom_V_1_1 = project.in(file("Utils/Migrate/SourceVersion/MigrateFrom_V_1_1")) dependsOn (MigrateBase)

lazy val MigrateManager = project.in(file("Utils/Migrate/MigrateManager")) dependsOn (MigrateBase, KamanjaVersion)

lazy val MigrateTo_V_1_3 = project.in(file("Utils/Migrate/DestinationVersion/MigrateTo_V_1_3")) dependsOn (MigrateBase, KamanjaManager)

lazy val MigrateFrom_V_1_2 = project.in(file("Utils/Migrate/SourceVersion/MigrateFrom_V_1_2")) dependsOn (MigrateBase)

lazy val MigrateTo_V_1_4 = project.in(file("Utils/Migrate/DestinationVersion/MigrateTo_V_1_4")) dependsOn (MigrateBase, KamanjaManager)

lazy val MigrateFrom_V_1_3 = project.in(file("Utils/Migrate/SourceVersion/MigrateFrom_V_1_3")) dependsOn (MigrateBase)

lazy val jtm = project.in(file("GenerateModels/jtm")) dependsOn(Metadata, KamanjaBase, Exceptions, MetadataBootstrap, MessageDef)

lazy val InstallDriverBase = project.in(file("Utils/ClusterInstaller/InstallDriverBase"))

lazy val InstallDriver = project.in(file("Utils/ClusterInstaller/InstallDriver")) dependsOn (InstallDriverBase, Serialize, KamanjaUtils)

lazy val ClusterInstallerDriver = project.in(file("Utils/ClusterInstaller/ClusterInstallerDriver")) dependsOn (InstallDriverBase, MigrateBase, MigrateManager)

lazy val GetComponent = project.in(file("Utils/ClusterInstaller/GetComponent"))

lazy val PmmlTestTool = project.in(file("Utils/PmmlTestTool")) dependsOn (KamanjaVersion)

/*

val commonSettings = Seq(
    scalaVersion := "2.11.7",
    autoAPIMappings := true
  )

val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(unidocSettings: _*).
  settings(
    name := "KamanjaManager",
    unidocProjectFilter in (ScalaUnidoc, unidoc) := inAnyProject -- inProjects(MigrateFrom_V_1_1, MigrateFrom_V_1_2)
  ).
  aggregate(BaseTypes, BaseFunctions, Serialize, ZooKeeperClient, ZooKeeperListener, Exceptions, KamanjaBase, DataDelimiters, KamanjaManager, InputOutputAdapterBase, KafkaSimpleInputOutputAdapters, FileSimpleInputOutputAdapters, SimpleEnvContextImpl, StorageBase, Metadata, OutputMsgDef, MessageDef, PmmlRuntime, PmmlCompiler, PmmlUdfs, MethodExtractor, MetadataAPI, MetadataBootstrap, MetadataAPIService, MetadataAPIServiceClient, SimpleKafkaProducer, KVInit, ZooKeeperLeaderLatch, JsonDataGen, NodeInfoExtract, Controller, SimpleApacheShiroAdapter, AuditAdapters, CustomUdfLib, JdbcDataCollector, ExtractData, InterfacesSamples, StorageCassandra, StorageHashMap, StorageHBase, StorageTreeMap, StorageSqlServer, StorageManager, AuditAdapterBase, SecurityAdapterBase, KamanjaUtils, UtilityService, HeartBeat, TransactionService, KvBase, FileDataConsumer, CleanUtil, SaveContainerDataComponent, UtilsForModels, JarFactoryOfModelInstanceFactory, JpmmlFactoryOfModelInstanceFactory, MigrateBase, MigrateManager)

*/

