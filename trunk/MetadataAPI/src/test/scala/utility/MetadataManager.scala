package com.ligadata.MetadataAPI.utility.test

import java.io.File
import org.apache.commons.io.FileUtils
import com.ligadata.MetadataAPI.{MetadataAPIImpl => md}
import com.ligadata.Exceptions.AlreadyExistsException
import com.ligadata.kamanja.metadata.MdMgr
/**
 * Created by dhavalkolapkar on 3/23/16.
 */

 case class MetadataAPIProperties(var database: String = "hashmap", var databaseHost: String = MetadataDefaults.databaseLocation, var databaseSchema: String = "metadata",
                                   var classPath: String = MetadataDefaults.metadataClasspath, var znodeBasePath: String = "/kamanja",
                                   var zkConnStr: String = "localhost:2181", var modelExecLog: String = "true", var adapterSpecificConfig: String = "")

  case class MetadataManagerException(message: String) extends Exception(message)

  class MetadataDataStore(storeType: String, schemaName: String, location: String) {
    override def toString = "{\"StoreType\": \"" + storeType + "\", \"SchemaName\": \"" + schemaName + "\", \"Location\": \"" + location + "\"}"
  }

class MetadataManager {

  private val metadataDir = new File(getClass.getResource("/Metadata").getPath)
  private val scalaHome = System.getenv("SCALA_HOME")
  private val javaHome = System.getenv("JAVA_HOME")
  private val userId = "metadataapi"

  if(scalaHome == null || javaHome == null) {
    throw new MetadataManagerException("Environment Variable SCALA_HOME or JAVA_HOME not set")
  }
  /// Init is kept separate rather than as a part of the construction of the class in order to prevent execution of certain operations too soon.
  def initMetadataCfg(config: MetadataAPIProperties): Unit = {
    //println(config.classPath)
    if(scalaHome != null)
      md.GetMetadataAPIConfig.setProperty("SCALA_HOME", scalaHome)
    else {
      throw new MetadataManagerException("Failed to retrieve environmental variable 'SCALA_HOME'. You must set this variable in order to run tests.")
    }

    if(javaHome != null)
      md.GetMetadataAPIConfig.setProperty("JAVA_HOME", javaHome)
    else {
      throw new MetadataManagerException("Failed to retrieve environmental variable 'JAVA_HOME'. You must set this variable in order to run tests.")
    }

      val jarPathSystem: String = getClass.getResource("/jars/lib/system").getPath
    val jarPathApp: String = getClass.getResource("/jars/lib/application").getPath
    val mdDataStore = new MetadataDataStore(config.database, config.databaseSchema,config.databaseHost)

    md.metadataAPIConfig.setProperty("CONCEPT_FILES_DIR", getClass.getResource("/Metadata/concept").getPath)
    md.metadataAPIConfig.setProperty("CONFIG_FILES_DIR", getClass.getResource("/Metadata/inputConfig").getPath)
    md.metadataAPIConfig.setProperty("CONTAINER_FILES_DIR", getClass.getResource("/Metadata/inputContainer").getPath)
    md.metadataAPIConfig.setProperty("FUNCTION_FILES_DIR", getClass.getResource("/Metadata/function").getPath)
    md.metadataAPIConfig.setProperty("MODEL_FILES_DIR", getClass.getResource("/Metadata/inputModel").getPath)
    md.metadataAPIConfig.setProperty("MESSAGE_FILES_DIR", getClass.getResource("/Metadata/inputMessage").getPath)
    md.metadataAPIConfig.setProperty("ROOT_DIR", "")
    md.metadataAPIConfig.setProperty("GIT_ROOT", "")
    md.metadataAPIConfig.setProperty("METADATA_DATASTORE", mdDataStore.toString)
    md.metadataAPIConfig.setProperty("JAR_TARGET_DIR", jarPathApp)
    md.metadataAPIConfig.setProperty("JAR_PATHS",  jarPathSystem +"," + jarPathApp)
    md.metadataAPIConfig.setProperty("MANIFEST_PATH", metadataDir.getAbsoluteFile + "/manifest.mf")
    md.metadataAPIConfig.setProperty("CLASSPATH", config.classPath)
    md.metadataAPIConfig.setProperty("NOTIFY_ENGINE", "NO")
    md.metadataAPIConfig.setProperty("ZNODE_PATH", config.znodeBasePath)
    md.metadataAPIConfig.setProperty("ZOOKEEPER_CONNECT_STRING", config.zkConnStr)
    md.metadataAPIConfig.setProperty("COMPILER_WORK_DIR", getClass.getResource("/jars/lib/workingdir").getPath)
    md.metadataAPIConfig.setProperty("API_LEADER_SELECTION_ZK_NODE", "/ligadata/metadata")
    md.metadataAPIConfig.setProperty("MODEL_EXEC_LOG", config.modelExecLog)
    md.metadataAPIConfig.setProperty("SECURITY_IMPL_JAR", jarPathSystem + "/simpleapacheshiroadapter_2.11-1.0.jar")
    md.metadataAPIConfig.setProperty("SECURITY_IMPL_CLASS", "com.ligadata.Security.SimpleApacheShiroAdapter")
    md.metadataAPIConfig.setProperty("DO_AUTH", "NO")
    md.metadataAPIConfig.setProperty("AUDIT_IMPL_JAR", jarPathSystem + "/auditadapters_2.11-1.0.jar")
    md.metadataAPIConfig.setProperty("AUDIT_IMPL_CLASS", "com.ligadata.audit.adapters.AuditCassandraAdapter")
    md.metadataAPIConfig.setProperty("DO_AUDIT", "NO")
    md.metadataAPIConfig.setProperty("ADAPTER_SPECIFIC_CONFIG", config.adapterSpecificConfig)
    //md.metadataAPIConfig.setProperty("SSL_PASSWD", config.sslPassword)

    try {
      //md.InitMdMgr(MdMgr.mdMgr, config.database, config.databaseHost, config.databaseSchema, config.databaseHost, config.adapterSpecificConfig)
      MdMgr.mdMgr.truncate
     // FileUtils.deleteDirectory(new File("/Users/dhavalkolapkar/Documents/Workspace/Kamanja/trunk/MetadataAPI/src/test/resources/storage"))
      md.InitMdMgr(MdMgr.mdMgr, md.metadataAPIConfig.get("JAR_PATHS").toString, md.metadataAPIConfig.get("METADATA_DATASTORE").toString)
    }
    catch {
      case e: AlreadyExistsException =>
      case e: Exception => throw new MetadataManagerException("AUTOMATION-METADATA: Failed to initialize MetadataAPI with the following exception:\n" + e)
    }
  }

}
