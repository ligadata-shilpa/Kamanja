import sbt.Keys._
import sbt._

sbtPlugin := true

version := "0.0.0.1"

//lazy val kamanjaVersion = settingKey[String]("kamanjaVersion")
//lazy val kamanjaVersion = "1.4.0"


//scalaVersion := "2.11.7"


crossScalaVersions := Seq("2.11.7", "2.10.4")

shellPrompt := { state => "sbt (%s)> ".format(Project.extract(state).currentProject.id) }

net.virtualvoid.sbt.graph.Plugin.graphSettings

//libraryDependencies := {
//  CrossVersion.partialVersion(scalaVersion.value) match {
//    // if scala 2.11+ is used, quasiquotes are merged into scala-reflect
//    case Some((2, scalaMajor)) if scalaMajor >= 11 =>
//      libraryDependencies.value ++ Seq("org.scalameta" %% "scalameta" % "0.0.3")
//    // libraryDependencies.value
//    // in Scala 2.10, quasiquotes are provided by macro paradise
//    case Some((2, 10)) =>
//      libraryDependencies.value ++ Seq("org.scalamacros" %% "quasiquotes" % "2.1.0")
//    //libraryDependencies.value ++ Seq(
//    //compilerPlugin("org.scalamacros" % "paradise" % "2.0.0" cross CrossVersion.full),
//    //"org.scalamacros" %% "quasiquotes" % "2.1.0" cross CrossVersion.binary)
//  }
//}

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

resolvers += Resolver.file("Local repo", file(System.getProperty("user.home") + "/.ivy2/local"))(Resolver.ivyStylePatterns)

resolvers += Resolver.typesafeRepo("releases")

resolvers += Resolver.sonatypeRepo("releases")

coverageEnabled in ThisBuild := true

val Organization = "com.ligadata"

//newly added
lazy val ExtDependencyLibs = project.in(file("ExtDependencyLibs")) //dependsOn( Metadata) because of libraryDependencies += "metadata" %% "metadata" % "1.0"
lazy val KamanjaInternalDeps = project.in(file("KamanjaInternalDeps")) dependsOn(ExtDependencyLibs % "provided", InputOutputAdapterBase, Exceptions, DataDelimiters, Metadata, KamanjaBase, MetadataBootstrap, Serialize, ZooKeeperListener,
    ZooKeeperLeaderLatch, KamanjaUtils, TransactionService, StorageManager, MessageDef, PmmlCompiler, ZooKeeperClient, OutputMsgDef, SecurityAdapterBase, HeartBeat, JpmmlFactoryOfModelInstanceFactory,
    SimpleApacheShiroAdapter, MigrateBase, KamanjaVersion, InstallDriverBase,BaseFunctions,KafkaSimpleInputOutputAdapters,FileSimpleInputOutputAdapters,SimpleEnvContextImpl,GenericMsgCompiler,MethodExtractor,MetadataAPIServiceClient,JsonDataGen,Controller,AuditAdapters,CustomUdfLib,ExtractData,InterfacesSamples,UtilityService,SaveContainerDataComponent,UtilsForModels,JarFactoryOfModelInstanceFactory)

//lazy val KamanjaInternalDeps = project.in(file("KamanjaInternalDeps")) dependsOn(ExtDependencyLibs % "provided", InputOutputAdapterBase, Exceptions, DataDelimiters, Metadata, KamanjaBase, MetadataBootstrap, Serialize, ZooKeeperListener,
//  ZooKeeperLeaderLatch, KamanjaUtils, TransactionService, StorageManager, MessageDef, PmmlCompiler, ZooKeeperClient, OutputMsgDef, SecurityAdapterBase, HeartBeat, JpmmlFactoryOfModelInstanceFactory,
//  SimpleApacheShiroAdapter, MigrateBase, KamanjaVersion, InstallDriverBase)

//lazy val KamanjaDependencyLibs = project.in(file("KamanjaDependencyLibs")) dependsOn(KamanjaManager, MetadataAPI, KVInit, NodeInfoExtract, MetadataAPIService, JdbcDataCollector, FileDataConsumer, CleanUtil, InstallDriver, GetComponent, SimpleKafkaProducer)

////////////////////////

lazy val BaseTypes = project.in(file("BaseTypes")) dependsOn(ExtDependencyLibs % "provided", Metadata, Exceptions)

lazy val BaseFunctions = project.in(file("BaseFunctions")) dependsOn(ExtDependencyLibs % "provided", Metadata, Exceptions) // has only resolvers , no dependencies

lazy val Serialize = project.in(file("Utils/Serialize")) dependsOn(ExtDependencyLibs % "provided", Metadata, AuditAdapterBase, Exceptions)


lazy val ZooKeeperClient = project.in(file("Utils/ZooKeeper/CuratorClient")) dependsOn(ExtDependencyLibs % "provided", Serialize, Exceptions)

lazy val ZooKeeperListener = project.in(file("Utils/ZooKeeper/CuratorListener")) dependsOn(ExtDependencyLibs % "provided", ZooKeeperClient, Serialize, Exceptions)

lazy val KamanjaVersion = project.in(file("KamanjaVersion")) //dependsOn(ExtDependencyLibs) ~no external dependencies

lazy val Exceptions = project.in(file("Exceptions")) dependsOn(ExtDependencyLibs % "provided", KamanjaVersion)

lazy val KamanjaBase = project.in(file("KamanjaBase")) dependsOn(ExtDependencyLibs % "provided", Metadata, Exceptions, KamanjaUtils, HeartBeat, KvBase, DataDelimiters)

lazy val DataDelimiters = project.in(file("DataDelimiters")) dependsOn (ExtDependencyLibs % "provided")

lazy val KamanjaManager = project.in(file("KamanjaManager")) dependsOn(ExtDependencyLibs % "provided", Metadata, KamanjaBase, MetadataBootstrap, MetadataAPI, Serialize, ZooKeeperListener, ZooKeeperLeaderLatch, Exceptions, KamanjaUtils, TransactionService, DataDelimiters, InputOutputAdapterBase)

lazy val InputOutputAdapterBase = project.in(file("InputOutputAdapters/InputOutputAdapterBase")) dependsOn(ExtDependencyLibs % "provided", Exceptions, DataDelimiters, HeartBeat)


// lazy val IbmMqSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/IbmMqSimpleInputOutputAdapters")) dependsOn(InputOutputAdapterBase, Exceptions, DataDelimiters)

lazy val KafkaSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/KafkaSimpleInputOutputAdapters")) dependsOn(ExtDependencyLibs % "provided", InputOutputAdapterBase, Exceptions, DataDelimiters)

lazy val FileSimpleInputOutputAdapters = project.in(file("InputOutputAdapters/FileSimpleInputOutputAdapters")) dependsOn(ExtDependencyLibs % "provided", InputOutputAdapterBase, Exceptions, DataDelimiters)

lazy val SimpleEnvContextImpl = project.in(file("EnvContexts/SimpleEnvContextImpl")) dependsOn(ExtDependencyLibs % "provided", KamanjaBase, StorageManager, Serialize, Exceptions)

lazy val StorageBase = project.in(file("Storage/StorageBase")) dependsOn(ExtDependencyLibs % "provided", Exceptions, KamanjaUtils, KvBase)

lazy val Metadata = project.in(file("Metadata")) dependsOn(Exceptions, ExtDependencyLibs % "provided")

lazy val OutputMsgDef = project.in(file("OutputMsgDef")) dependsOn(ExtDependencyLibs % "provided", Metadata, KamanjaBase, BaseTypes)


lazy val MessageDef = project.in(file("MessageDef")) dependsOn(ExtDependencyLibs % "provided", Metadata, MetadataBootstrap, Exceptions)

lazy val GenericMsgCompiler = project.in(file("GenericMsgCompiler")) dependsOn(ExtDependencyLibs % "provided", Metadata, MetadataBootstrap, Exceptions)

// lazy val LoadtestCommon = project.in(file("Tools/LoadtestCommon")) dependsOn(StorageManager, Exceptions)

// lazy val LoadtestRunner = project.in(file("Tools/LoadtestRunner")) dependsOn(LoadtestCommon, Exceptions)

// lazy val LoadtestMaster = project.in(file("Tools/LoadtestMaster")) dependsOn(LoadtestCommon, Exceptions)

// lazy val Loadtest = project.in(file("Tools/Loadtest")) dependsOn(StorageManager, Exceptions)

lazy val PmmlRuntime = project.in(file("Pmml/PmmlRuntime")) dependsOn(ExtDependencyLibs % "provided", Metadata, KamanjaBase, Exceptions)


lazy val PmmlCompiler = project.in(file("Pmml/PmmlCompiler")) dependsOn(ExtDependencyLibs % "provided", PmmlRuntime, PmmlUdfs, Metadata, KamanjaBase, MetadataBootstrap, Exceptions)


lazy val PmmlUdfs = project.in(file("Pmml/PmmlUdfs")) dependsOn(ExtDependencyLibs % "provided", Metadata, PmmlRuntime, KamanjaBase, Exceptions)


lazy val MethodExtractor = project.in(file("Pmml/MethodExtractor")) dependsOn(ExtDependencyLibs % "provided", PmmlUdfs, Metadata, KamanjaBase, Serialize, Exceptions) // added
// no external dependencies

lazy val MetadataAPI = project.in(file("MetadataAPI")) dependsOn(ExtDependencyLibs % "provided", StorageManager, Metadata, MessageDef, PmmlCompiler, Serialize, ZooKeeperClient, ZooKeeperListener, OutputMsgDef, Exceptions, SecurityAdapterBase, KamanjaUtils, HeartBeat, KamanjaBase, JpmmlFactoryOfModelInstanceFactory, SimpleApacheShiroAdapter % "test")

lazy val MetadataBootstrap = project.in(file("MetadataBootstrap/Bootstrap")) dependsOn(ExtDependencyLibs % "provided", Metadata, KamanjaBase, BaseTypes, Exceptions)

lazy val MetadataAPIService = project.in(file("MetadataAPIService")) dependsOn(ExtDependencyLibs % "provided", KamanjaBase, MetadataAPI, ZooKeeperLeaderLatch, Exceptions)

lazy val MetadataAPIServiceClient = project.in(file("MetadataAPIServiceClient")) dependsOn(ExtDependencyLibs % "provided", Serialize, Exceptions, KamanjaBase)

lazy val SimpleKafkaProducer = project.in(file("Utils/SimpleKafkaProducer")) dependsOn(ExtDependencyLibs % "provided", Metadata, KamanjaBase, Exceptions)

lazy val KVInit = project.in(file("Utils/KVInit")) dependsOn(ExtDependencyLibs % "provided", Metadata, KamanjaBase, MetadataBootstrap, MetadataAPI, StorageManager, Exceptions, TransactionService)

lazy val ZooKeeperLeaderLatch = project.in(file("Utils/ZooKeeper/CuratorLeaderLatch")) dependsOn(ExtDependencyLibs % "provided", ZooKeeperClient, Exceptions)

lazy val JsonDataGen = project.in(file("Utils/JsonDataGen")) dependsOn(ExtDependencyLibs % "provided", Exceptions, KamanjaBase)

lazy val NodeInfoExtract = project.in(file("Utils/NodeInfoExtract")) dependsOn(ExtDependencyLibs % "provided", MetadataAPI, Exceptions)

lazy val Controller = project.in(file("Utils/Controller")) dependsOn(ExtDependencyLibs % "provided", ZooKeeperClient, ZooKeeperListener, KafkaSimpleInputOutputAdapters, Exceptions)

lazy val SimpleApacheShiroAdapter = project.in(file("Utils/Security/SimpleApacheShiroAdapter")) dependsOn(ExtDependencyLibs % "provided", Metadata, Exceptions, SecurityAdapterBase)

lazy val AuditAdapters = project.in(file("Utils/Audit")) dependsOn(ExtDependencyLibs % "provided", StorageManager, Exceptions, AuditAdapterBase, Serialize)

lazy val CustomUdfLib = project.in(file("SampleApplication/CustomUdfLib")) dependsOn(ExtDependencyLibs % "provided", PmmlUdfs, Exceptions)
// no external dependencies

lazy val JdbcDataCollector = project.in(file("Utils/JdbcDataCollector")) dependsOn(ExtDependencyLibs % "provided", Exceptions)

lazy val ExtractData = project.in(file("Utils/ExtractData")) dependsOn(ExtDependencyLibs % "provided", Metadata, KamanjaBase, MetadataBootstrap, MetadataAPI, StorageManager, Exceptions)

lazy val InterfacesSamples = project.in(file("SampleApplication/InterfacesSamples")) dependsOn(ExtDependencyLibs % "provided", Metadata, KamanjaBase, MetadataBootstrap, MetadataAPI, StorageBase, Exceptions)

lazy val StorageCassandra = project.in(file("Storage/Cassandra")) dependsOn(ExtDependencyLibs % "provided", StorageBase, Serialize, Exceptions, KamanjaUtils, KvBase)

lazy val StorageHashMap = project.in(file("Storage/HashMap")) dependsOn(ExtDependencyLibs % "provided", StorageBase, Serialize, Exceptions, KamanjaUtils, KvBase)

lazy val StorageHBase = project.in(file("Storage/HBase")) dependsOn(ExtDependencyLibs % "provided", StorageBase, Serialize, Exceptions, KamanjaUtils, KvBase)


// lazy val StorageRedis = project.in(file("Storage/Redis")) dependsOn(StorageBase, Exceptions, KamanjaUtils)

lazy val StorageTreeMap = project.in(file("Storage/TreeMap")) dependsOn(ExtDependencyLibs % "provided", StorageBase, Serialize, Exceptions, KamanjaUtils, KvBase)

// lazy val StorageVoldemort = project.in(file("Storage/Voldemort")) dependsOn(StorageBase, Exceptions, KamanjaUtils)

lazy val StorageSqlServer = project.in(file("Storage/SqlServer")) dependsOn(ExtDependencyLibs % "provided", StorageBase, Serialize, Exceptions, KamanjaUtils)

// lazy val StorageMySql = project.in(file("Storage/MySql")) dependsOn(StorageBase, Serialize, Exceptions, KamanjaUtils)

lazy val StorageManager = project.in(file("Storage/StorageManager")) dependsOn(ExtDependencyLibs % "provided", StorageBase, Exceptions, KamanjaBase, KamanjaUtils, StorageSqlServer, StorageCassandra, StorageHashMap, StorageTreeMap, StorageHBase)

lazy val AuditAdapterBase = project.in(file("AuditAdapters/AuditAdapterBase")) dependsOn(ExtDependencyLibs % "provided", Exceptions)

lazy val SecurityAdapterBase = project.in(file("SecurityAdapters/SecurityAdapterBase")) dependsOn(ExtDependencyLibs % "provided", Exceptions)


lazy val KamanjaUtils = project.in(file("KamanjaUtils")) dependsOn(ExtDependencyLibs % "provided", Exceptions)


lazy val UtilityService = project.in(file("Utils/UtilitySerivce")) dependsOn(ExtDependencyLibs % "provided", Exceptions, KamanjaUtils)

lazy val HeartBeat = project.in(file("HeartBeat")) dependsOn(ExtDependencyLibs % "provided", ZooKeeperListener, ZooKeeperLeaderLatch, Exceptions)


lazy val TransactionService = project.in(file("TransactionService")) dependsOn(ExtDependencyLibs % "provided", Exceptions, KamanjaBase, ZooKeeperClient, StorageBase, StorageManager)


lazy val KvBase = project.in(file("KvBase"))
//no external dependencies

lazy val FileDataConsumer = project.in(file("FileDataConsumer")) dependsOn(ExtDependencyLibs % "provided", Exceptions, MetadataAPI)

lazy val CleanUtil = project.in(file("Utils/CleanUtil")) dependsOn(ExtDependencyLibs % "provided", MetadataAPI)

lazy val SaveContainerDataComponent = project.in(file("Utils/SaveContainerDataComponent")) dependsOn(ExtDependencyLibs % "provided", Metadata, KamanjaBase, MetadataBootstrap, MetadataAPI, StorageManager, Exceptions, TransactionService)

lazy val UtilsForModels = project.in(file("Utils/UtilsForModels")) dependsOn (ExtDependencyLibs % "provided")

lazy val JarFactoryOfModelInstanceFactory = project.in(file("FactoriesOfModelInstanceFactory/JarFactoryOfModelInstanceFactory")) dependsOn(ExtDependencyLibs % "provided", Metadata, KamanjaBase, Exceptions)

lazy val JpmmlFactoryOfModelInstanceFactory = project.in(file("FactoriesOfModelInstanceFactory/JpmmlFactoryOfModelInstanceFactory")) dependsOn(ExtDependencyLibs % "provided", Metadata, KamanjaBase, Exceptions)

lazy val MigrateBase = project.in(file("Utils/Migrate/MigrateBase")) dependsOn (ExtDependencyLibs % "provided")
// no external dependencies

lazy val MigrateFrom_V_1_1 = project.in(file("Utils/Migrate/SourceVersion/MigrateFrom_V_1_1")) dependsOn (MigrateBase)

lazy val MigrateManager = project.in(file("Utils/Migrate/MigrateManager")) dependsOn(MigrateBase, KamanjaVersion)

lazy val MigrateTo_V_1_3 = project.in(file("Utils/Migrate/DestinationVersion/MigrateTo_V_1_3")) dependsOn(ExtDependencyLibs % "provided",MigrateBase, KamanjaManager)

lazy val MigrateFrom_V_1_2 = project.in(file("Utils/Migrate/SourceVersion/MigrateFrom_V_1_2")) dependsOn (MigrateBase)

lazy val MigrateTo_V_1_4 = project.in(file("Utils/Migrate/DestinationVersion/MigrateTo_V_1_4")) dependsOn(ExtDependencyLibs % "provided",MigrateBase, KamanjaManager)

lazy val MigrateFrom_V_1_3 = project.in(file("Utils/Migrate/SourceVersion/MigrateFrom_V_1_3")) dependsOn (MigrateBase)

//lazy val jtm = project.in(file("GenerateModels/jtm")) dependsOn(ExtDependencyLibs % "provided",Metadata, KamanjaBase, Exceptions, MetadataBootstrap, MessageDef)

lazy val InstallDriverBase = project.in(file("Utils/ClusterInstaller/InstallDriverBase")) dependsOn (ExtDependencyLibs % "provided")

lazy val InstallDriver = project.in(file("Utils/ClusterInstaller/InstallDriver")) dependsOn(ExtDependencyLibs % "provided",InstallDriverBase, Serialize, KamanjaUtils)

lazy val ClusterInstallerDriver = project.in(file("Utils/ClusterInstaller/ClusterInstallerDriver")) dependsOn(InstallDriverBase, MigrateBase, MigrateManager)

lazy val GetComponent = project.in(file("Utils/ClusterInstaller/GetComponent"))

lazy val PmmlTestTool = project.in(file("Utils/PmmlTestTool")) dependsOn(ExtDependencyLibs % "provided", KamanjaVersion)

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