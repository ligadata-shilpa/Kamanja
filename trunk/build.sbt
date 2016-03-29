import sbt._
import Keys._
import UnidocKeys._

sbtPlugin := true

version := "0.0.0.1"

//scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.11.7", "2.10.4")

shellPrompt := { state =>  "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

net.virtualvoid.sbt.graph.Plugin.graphSettings

coverageExcludedPackages in ThisBuild := "com.ligadata.Migrate.MdResolve;com.ligadata.Migrate.Migrate*;com.ligadata.MigrateBase;com.ligadata.pmml.udfs.Udfs;com.ligadata.pmml.udfs.UdfBase;com.ligadata.pmml.udfs.CustomUdfs"

coverageEnabled in ThisBuild := true

coverageMinimum := 80

coverageFailOnMinimum := false

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

lazy val BaseTypes = project.in(file("BaseTypes"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, Exceptions)

lazy val BaseFunctions = project.in(file("BaseFunctions"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, Exceptions)

lazy val Serialize = project.in(file("Utils/Serialize"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, AuditAdapterBase, Exceptions)

lazy val ZooKeeperClient = project.in(file("Utils/ZooKeeper/CuratorClient"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Serialize, Exceptions)

lazy val ZooKeeperListener = project.in(file("Utils/ZooKeeper/CuratorListener"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(ZooKeeperClient, Serialize, Exceptions)

lazy val KamanjaVersion = project.in(file("KamanjaVersion"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)

lazy val Exceptions = project.in(file("Exceptions"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(KamanjaVersion)

lazy val KamanjaBase = project.in(file("KamanjaBase"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, Exceptions, KamanjaUtils, HeartBeat, KvBase, DataDelimiters, BaseTypes)

lazy val DataDelimiters = project.in(file("DataDelimiters"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)

lazy val KamanjaManager = project.in(file("KamanjaManager"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, KamanjaBase, MetadataBootstrap, MetadataAPI, Serialize, ZooKeeperListener, ZooKeeperLeaderLatch, Exceptions, KamanjaUtils, TransactionService, DataDelimiters, InputOutputAdapterBase)

lazy val InputOutputAdapterBase = project.in(file("InputOutputAdapters/InputOutputAdapterBase"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Exceptions, DataDelimiters, HeartBeat, KamanjaBase)

// lazy val IbmMqSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/IbmMqSimpleInputOutputAdapters")) dependsOn(InputOutputAdapterBase, Exceptions, DataDelimiters)

lazy val KafkaSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/KafkaSimpleInputOutputAdapters"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(InputOutputAdapterBase, Exceptions, DataDelimiters)

lazy val SmartFileAdapter = project.in(file("InputOutputAdapters/SmartFileAdapter"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(InputOutputAdapterBase, Exceptions, DataDelimiters, MetadataAPI)

lazy val FileSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/FileSimpleInputOutputAdapters"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(InputOutputAdapterBase, Exceptions, DataDelimiters)

lazy val SimpleEnvContextImpl = project.in(file("EnvContexts/SimpleEnvContextImpl"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(KamanjaBase, StorageManager, Serialize, Exceptions)

lazy val StorageBase = project.in(file("Storage/StorageBase"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Exceptions, KamanjaUtils, KvBase, KamanjaBase)

lazy val Metadata = project.in(file("Metadata"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Exceptions)

// lazy val MessageDef = project.in(file("MessageDef")).configs(TestConfigs.all: _*).settings(TestSettings.settings: _*).dependsOn(Metadata, MetadataBootstrap, Exceptions)

lazy val MessageCompiler = project.in(file("MessageCompiler"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, MetadataBootstrap, Exceptions)

// lazy val LoadtestCommon = project.in(file("Tools/LoadtestCommon")) dependsOn(StorageManager, Exceptions)

// lazy val LoadtestRunner = project.in(file("Tools/LoadtestRunner")) dependsOn(LoadtestCommon, Exceptions)

// lazy val LoadtestMaster = project.in(file("Tools/LoadtestMaster")) dependsOn(LoadtestCommon, Exceptions)

// lazy val Loadtest = project.in(file("Tools/Loadtest")) dependsOn(StorageManager, Exceptions)

lazy val PmmlRuntime = project.in(file("Pmml/PmmlRuntime"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, KamanjaBase, Exceptions)

lazy val PmmlCompiler = project.in(file("Pmml/PmmlCompiler"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(PmmlRuntime, PmmlUdfs, Metadata, KamanjaBase, MetadataBootstrap, Exceptions)

lazy val PmmlUdfs = project.in(file("Pmml/PmmlUdfs"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, PmmlRuntime, KamanjaBase, Exceptions)

lazy val MethodExtractor = project.in(file("Pmml/MethodExtractor"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(PmmlUdfs, Metadata, KamanjaBase, Serialize, Exceptions)

lazy val MetadataAPI = project.in(file("MetadataAPI"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(StorageManager,Metadata,MessageCompiler,PmmlCompiler,Serialize,ZooKeeperClient,ZooKeeperListener,Exceptions, SecurityAdapterBase, KamanjaUtils, HeartBeat, KamanjaBase, JpmmlFactoryOfModelInstanceFactory, SimpleApacheShiroAdapter % "test")

lazy val MetadataBootstrap = project.in(file("MetadataBootstrap/Bootstrap"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, KamanjaBase, BaseTypes, Exceptions)

lazy val MetadataAPIService = project.in(file("MetadataAPIService"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(KamanjaBase,MetadataAPI,ZooKeeperLeaderLatch, Exceptions)

lazy val MetadataAPIServiceClient = project.in(file("MetadataAPIServiceClient"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Serialize, Exceptions, KamanjaBase)

lazy val SimpleKafkaProducer = project.in(file("Utils/SimpleKafkaProducer"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, KamanjaBase, Exceptions)

lazy val KVInit = project.in(file("Utils/KVInit"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, KamanjaBase, MetadataBootstrap, MetadataAPI, StorageManager, Exceptions, TransactionService)

lazy val ContainersUtility = project.in(file("Utils/ContainersUtility"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn (Metadata, KamanjaBase, MetadataBootstrap, MetadataAPI, StorageManager, Exceptions, TransactionService)

lazy val ZooKeeperLeaderLatch = project.in(file("Utils/ZooKeeper/CuratorLeaderLatch"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(ZooKeeperClient, Exceptions, KamanjaUtils)

lazy val JsonDataGen = project.in(file("Utils/JsonDataGen"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Exceptions, KamanjaBase)

lazy val JsonChecker = project.in(file("Utils/JsonChecker"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Exceptions)

lazy val NodeInfoExtract  = project.in(file("Utils/NodeInfoExtract"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(MetadataAPI, Exceptions)

lazy val Controller = project.in(file("Utils/Controller"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(ZooKeeperClient,ZooKeeperListener,KafkaSimpleInputOutputAdapters, Exceptions)

lazy val SimpleApacheShiroAdapter = project.in(file("Utils/Security/SimpleApacheShiroAdapter"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, Exceptions, SecurityAdapterBase)

lazy val AuditAdapters = project.in(file("Utils/Audit"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(StorageManager, Exceptions, AuditAdapterBase,Serialize)

lazy val CustomUdfLib = project.in(file("SampleApplication/CustomUdfLib"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(PmmlUdfs, Exceptions)

lazy val JdbcDataCollector = project.in(file("Utils/JdbcDataCollector"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Exceptions)

lazy val ExtractData = project.in(file("Utils/ExtractData"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, KamanjaBase, MetadataBootstrap, MetadataAPI, StorageManager, Exceptions)

// lazy val InterfacesSamples = project.in(file("SampleApplication/InterfacesSamples")).configs(TestConfigs.all: _*).settings(TestSettings.settings: _*).dependsOn(Metadata, KamanjaBase, MetadataBootstrap, MetadataAPI, StorageBase, Exceptions)

lazy val StorageCassandra = project.in(file("Storage/Cassandra"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(StorageBase, Serialize, Exceptions, KamanjaUtils, KvBase)

lazy val StorageHashMap = project.in(file("Storage/HashMap"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(StorageBase, Serialize, Exceptions, KamanjaUtils, KvBase)

lazy val StorageHBase = project.in(file("Storage/HBase"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(StorageBase, Serialize, Exceptions, KamanjaUtils, KvBase)

// lazy val StorageRedis = project.in(file("Storage/Redis")) dependsOn(StorageBase, Exceptions, KamanjaUtils)

lazy val StorageTreeMap = project.in(file("Storage/TreeMap"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(StorageBase, Serialize, Exceptions, KamanjaUtils, KvBase)

// lazy val StorageVoldemort = project.in(file("Storage/Voldemort")) dependsOn(StorageBase, Exceptions, KamanjaUtils)

lazy val StorageSqlServer = project.in(file("Storage/SqlServer"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(StorageBase, Serialize, Exceptions, KamanjaUtils)

// lazy val StorageMySql = project.in(file("Storage/MySql")) dependsOn(StorageBase, Serialize, Exceptions, KamanjaUtils)

lazy val StorageManager = project.in(file("Storage/StorageManager"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(StorageBase, Exceptions, KamanjaBase, KamanjaUtils, StorageSqlServer, StorageCassandra, StorageHashMap, StorageTreeMap, StorageHBase)

lazy val AuditAdapterBase = project.in(file("AuditAdapters/AuditAdapterBase"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Exceptions)

lazy val SecurityAdapterBase = project.in(file("SecurityAdapters/SecurityAdapterBase"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Exceptions)

lazy val KamanjaUtils = project.in(file("KamanjaUtils"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Exceptions)

lazy val UtilityService = project.in(file("Utils/UtilitySerivce"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Exceptions, KamanjaUtils)

lazy val HeartBeat = project.in(file("HeartBeat"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(ZooKeeperListener, ZooKeeperLeaderLatch, Exceptions)

lazy val TransactionService = project.in(file("TransactionService"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Exceptions, KamanjaBase, ZooKeeperClient, StorageBase, StorageManager)

lazy val KvBase = project.in(file("KvBase"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)

lazy val FileDataConsumer = project.in(file("FileDataConsumer"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Exceptions, MetadataAPI)

lazy val CleanUtil = project.in(file("Utils/CleanUtil"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(MetadataAPI)

lazy val SaveContainerDataComponent = project.in(file("Utils/SaveContainerDataComponent"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, KamanjaBase, MetadataBootstrap, MetadataAPI, StorageManager, Exceptions, TransactionService)

lazy val UtilsForModels = project.in(file("Utils/UtilsForModels"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)

lazy val JarFactoryOfModelInstanceFactory = project.in(file("FactoriesOfModelInstanceFactory/JarFactoryOfModelInstanceFactory"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, KamanjaBase, Exceptions)

lazy val JpmmlFactoryOfModelInstanceFactory = project.in(file("FactoriesOfModelInstanceFactory/JpmmlFactoryOfModelInstanceFactory"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, KamanjaBase, Exceptions)

lazy val MigrateBase = project.in(file("Utils/Migrate/MigrateBase"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)

lazy val MigrateFrom_V_1_1 = project.in(file("Utils/Migrate/SourceVersion/MigrateFrom_V_1_1"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(MigrateBase)

lazy val MigrateManager = project.in(file("Utils/Migrate/MigrateManager"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(MigrateBase, KamanjaVersion)

lazy val MigrateFrom_V_1_2 = project.in(file("Utils/Migrate/SourceVersion/MigrateFrom_V_1_2"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(MigrateBase)

lazy val MigrateTo_V_1_4 = project.in(file("Utils/Migrate/DestinationVersion/MigrateTo_V_1_4"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(MigrateBase, KamanjaManager)

lazy val MigrateFrom_V_1_3 = project.in(file("Utils/Migrate/SourceVersion/MigrateFrom_V_1_3"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(MigrateBase)

lazy val jtm = project.in(file("GenerateModels/jtm"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(Metadata, KamanjaBase, Exceptions, MetadataBootstrap, MessageCompiler)

lazy val runtime = project.in(file("GenerateModels/Runtime")) dependsOn(Metadata, KamanjaBase, Exceptions, MetadataBootstrap, MessageCompiler)

lazy val InstallDriverBase = project.in(file("Utils/ClusterInstaller/InstallDriverBase"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)

lazy val InstallDriver = project.in(file("Utils/ClusterInstaller/InstallDriver"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(InstallDriverBase, Serialize, KamanjaUtils)

lazy val ClusterInstallerDriver = project.in(file("Utils/ClusterInstaller/ClusterInstallerDriver"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(InstallDriverBase, MigrateBase, MigrateManager)

lazy val GetComponent = project.in(file("Utils/ClusterInstaller/GetComponent"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)

lazy val PmmlTestTool = project.in(file("Utils/PmmlTestTool"))
  .configs(TestConfigs.all: _*)
  .settings(TestSettings.settings: _*)
  .dependsOn(KamanjaVersion)

lazy val Dag = project.in(file("Utils/Dag")) dependsOn (KamanjaUtils, Exceptions)

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
  aggregate(BaseTypes, BaseFunctions, Serialize, ZooKeeperClient, ZooKeeperListener, Exceptions, KamanjaBase, DataDelimiters, KamanjaManager, InputOutputAdapterBase, KafkaSimpleInputOutputAdapters, FileSimpleInputOutputAdapters, SimpleEnvContextImpl, StorageBase, Metadata, MessageCompiler, PmmlRuntime, PmmlCompiler, PmmlUdfs, MethodExtractor, MetadataAPI, MetadataBootstrap, MetadataAPIService, MetadataAPIServiceClient, SimpleKafkaProducer, KVInit, ZooKeeperLeaderLatch, JsonDataGen, NodeInfoExtract, Controller, SimpleApacheShiroAdapter, AuditAdapters, CustomUdfLib, JdbcDataCollector, ExtractData, StorageCassandra, StorageHashMap, StorageHBase, StorageTreeMap, StorageSqlServer, StorageManager, AuditAdapterBase, SecurityAdapterBase, KamanjaUtils, UtilityService, HeartBeat, TransactionService, KvBase, FileDataConsumer, CleanUtil, SaveContainerDataComponent, UtilsForModels, JarFactoryOfModelInstanceFactory, JpmmlFactoryOfModelInstanceFactory, MigrateBase, MigrateManager)

*/