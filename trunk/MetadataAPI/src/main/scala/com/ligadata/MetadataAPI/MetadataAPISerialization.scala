package com.ligadata.MetadataAPI

import java.util.Date

import com.ligadata.Exceptions._
import com.ligadata.Serialize.TypeDef
import com.ligadata.kamanja.metadata._
import com.ligadata.kamanja.metadataload.MetadataLoad
import org.apache.logging.log4j.LogManager
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


/**
  * Created by Ahmed-Work on 3/15/2016.
  */


object MetadataAPISerialization {

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)
  implicit val jsonFormats: Formats = DefaultFormats


  def serializeObjectToJson(mdObj: Any): String = {

    try {
      mdObj match {
        case o: ModelDef => {
          val json = "Model" ->
            ("Name" -> o.name) ~
              ("PhysicalName" -> o.PhysicalName) ~
              ("JarName" -> getEmptyIfNull(o.jarName)) ~
              ("NameSpace" -> o.nameSpace) ~
              ("ModelType" -> o.miningModelType.toString) ~
              ("DependencyJars" -> o.CheckAndGetDependencyJarNames.toList) ~
              ("ModelRep" -> o.modelRepresentation.toString) ~
              ("OrigDef" -> o.OrigDef) ~
              ("OwnerId" -> o.OwnerId) ~
              ("TenantId" -> o.TenantId) ~
              ("ObjectDefinition" -> o.ObjectDefinition) ~
              ("ObjectFormat" -> ObjFormatType.asString(o.ObjectFormat)) ~
              ("Description" -> o.Description) ~
              ("ModelConfig" -> o.modelConfig) ~
              ("Author" -> o.Author) ~
              ("inputMsgSets" -> o.inputMsgSets.toList.map(m => m.toList.map(f => ("Origin" -> f.origin) ~ ("Message" -> f.message) ~ ("Attributes" -> f.attributes.toList)))) ~
              ("OutputMsgs" -> o.outputMsgs.toList) ~
              ("NumericTypes" -> ("Version" -> o.Version) ~ ("TransId" -> o.TranId) ~ ("UniqId" -> o.UniqId) ~ ("CreationTime" -> o.CreationTime) ~ ("ModTime" -> o.ModTime) ~ ("MdElemStructVer" -> o.MdElemStructVer) ~ ("MdElementId" -> o.MdElementId)) ~
              ("BooleanTypes" -> ("IsActive" -> o.IsActive) ~ ("IsReusable" -> o.isReusable) ~ ("IsDeleted" -> o.IsDeleted) ~ ("SupportsInstanceSerialization" -> o.SupportsInstanceSerialization)
                )
          compact(render(json))
        }
        case o: MessageDef => {

          var primaryKeys = List[(String, List[String])]()
          var foreignKeys = List[(String, List[String], String, List[String])]()

          o.cType.Keys.toList.foreach(m => {
            if (m.KeyType == RelationKeyType.tPrimary) {
              val pr = m.asInstanceOf[PrimaryKey]
              primaryKeys ::=(pr.constraintName, pr.key.toList)
            } else {
              val fr = m.asInstanceOf[ForeignKey]
              foreignKeys ::=(fr.constraintName, fr.key.toList, fr.forignContainerName, fr.forignKey.toList)
            }
          })

          val json = "Message" ->
            ("Name" -> o.Name) ~
              ("PhysicalName" -> o.PhysicalName) ~
              ("JarName" -> getEmptyIfNull(o.JarName)) ~
              ("NameSpace" -> o.NameSpace) ~
              ("DependencyJars" -> o.CheckAndGetDependencyJarNames.toList) ~
              ("OrigDef" -> o.OrigDef) ~
              ("ObjectDefinition" -> o.ObjectDefinition) ~
              ("ObjectFormat" -> ObjFormatType.asString(o.ObjectFormat)) ~
              ("CreationTime" -> o.CreationTime) ~
              ("Author" -> o.Author) ~
              ("OwnerId" -> o.OwnerId) ~
              ("TenantId" -> o.TenantId) ~
              ("SchemaId" -> o.cType.schemaId) ~
              ("AvroSchema" -> o.cType.avroSchema) ~
              ("PartitionKey" -> o.cType.PartitionKey.toList) ~
              ("IsActive" -> o.IsActive) ~
              ("IsDeleted" -> o.IsDeleted) ~
              ("Persist" -> o.cType.Persist) ~
              ("Description" -> o.Description) ~
              ("NumericTypes" -> ("Version" -> o.Version) ~ ("TransId" -> o.TranId) ~ ("UniqId" -> o.UniqId) ~ ("CreationTime" -> o.CreationTime) ~ ("ModTime" -> o.ModTime) ~ ("MdElemStructVer" -> o.MdElemStructVer) ~ ("MdElementId" -> o.MdElementId)) ~
              ("PrimaryKeys" -> primaryKeys.map(m => ("constraintName" -> m._1) ~ ("key" -> m._2))) ~
              ("ForeignKeys" -> foreignKeys.map(m => ("constraintName" -> m._1) ~ ("key" -> m._2) ~ ("forignContainerName" -> m._3) ~ ("forignKey" -> m._4)))
          compact(render(json))
        }
        case o: ContainerDef => {

          var primaryKeys = List[(String, List[String])]()
          var foreignKeys = List[(String, List[String], String, List[String])]()
          o.cType.Keys.toList.foreach(m => {
            if (m.KeyType == RelationKeyType.tPrimary) {
              val pr = m.asInstanceOf[PrimaryKey]
              primaryKeys ::=(pr.constraintName, pr.key.toList)
            } else {
              val fr = m.asInstanceOf[ForeignKey]
              foreignKeys ::=(fr.constraintName, fr.key.toList, fr.forignContainerName, fr.forignKey.toList)
            }
          })

          val json = "Container" ->
            ("Name" -> o.name) ~
              ("PhysicalName" -> o.physicalName) ~
              ("JarName" -> getEmptyIfNull(o.jarName)) ~
              ("NameSpace" -> o.nameSpace) ~
              ("DependencyJars" -> o.CheckAndGetDependencyJarNames.toList) ~
              ("OrigDef" -> o.OrigDef) ~
              ("ObjectDefinition" -> o.ObjectDefinition) ~
              ("ObjectFormat" -> ObjFormatType.asString(o.ObjectFormat)) ~
              ("CreationTime" -> o.CreationTime) ~
              ("Author" -> o.Author) ~
              ("OwnerId" -> o.OwnerId) ~
              ("TenantId" -> o.TenantId) ~
              ("SchemaId" -> o.cType.schemaId) ~
              ("AvroSchema" -> o.cType.avroSchema) ~
              ("Persist" -> o.cType.Persist) ~
              ("PartitionKey" -> o.cType.PartitionKey.toList) ~
              ("IsActive" -> o.IsActive) ~
              ("IsDeleted" -> o.IsDeleted) ~
              ("Description" -> o.Description) ~
              ("NumericTypes" -> ("Version" -> o.Version) ~ ("TransId" -> o.TranId) ~ ("UniqId" -> o.UniqId) ~ ("CreationTime" -> o.CreationTime) ~ ("ModTime" -> o.ModTime) ~ ("MdElemStructVer" -> o.MdElemStructVer) ~ ("MdElementId" -> o.MdElementId)) ~
              ("PrimaryKeys" -> primaryKeys.map(m => ("constraintName" -> m._1) ~ ("key" -> m._2))) ~
              ("ForeignKeys" -> foreignKeys.map(m => ("constraintName" -> m._1) ~ ("key" -> m._2) ~ ("forignContainerName" -> m._3) ~ ("forignKey" -> m._4)))
          compact(render(json))
        }
        case o: FunctionDef => {

          val json = "Function" ->
            ("Name" -> o.name) ~
              ("PhysicalName" -> o.physicalName) ~
              ("JarName" -> getEmptyIfNull(o.jarName)) ~
              ("NameSpace" -> o.nameSpace) ~
              ("DependencyJars" -> o.CheckAndGetDependencyJarNames.toList) ~
              ("OrigDef" -> o.OrigDef) ~
              ("ObjectDefinition" -> o.ObjectDefinition) ~
              ("ObjectFormat" -> ObjFormatType.asString(o.ObjectFormat)) ~
              ("Author" -> o.Author) ~
              ("OwnerId" -> o.OwnerId) ~
              ("TenantId" -> o.TenantId) ~
              ("Arguments" -> o.args.toList.map { arg => (("ArgName" -> arg.name) ~ ("ArgTypeNameSpace" -> arg.Type.nameSpace) ~ ("ArgTypeName" -> arg.Type.name)) }) ~
              ("Features" -> o.features.toList.map(m => m.toString)) ~
              ("ReturnTypeNameSpace" -> o.retType.nameSpace) ~
              ("ReturnTypeName" -> o.retType.name) ~
              ("ClassName" -> o.className) ~
              ("Description" -> o.Description) ~
              ("NumericTypes" -> ("Version" -> o.Version) ~ ("TransId" -> o.TranId) ~ ("UniqId" -> o.UniqId) ~ ("CreationTime" -> o.CreationTime) ~ ("ModTime" -> o.ModTime) ~ ("MdElemStructVer" -> o.MdElemStructVer) ~ ("MdElementId" -> o.MdElementId)) ~
              ("IsActive" -> o.IsActive) ~
              ("IsDeleted" -> o.IsDeleted)
          compact(render(json))
        }
        case o: MapTypeDef => {

          val json = "MapType" ->
            ("Name" -> o.Name) ~
              ("NameSpace" -> o.NameSpace) ~
              ("PhysicalName" -> o.PhysicalName) ~
              ("TypeTypeName" -> ObjTypeType.asString(o.tTypeType)) ~
              ("JarName" -> getEmptyIfNull(o.JarName)) ~
              ("ObjectFormat" -> ObjFormatType.asString(o.ObjectFormat)) ~
              ("DependencyJars" -> o.CheckAndGetDependencyJarNames.toList) ~
              ("Implementation" -> o.implementationName) ~
              ("KeyTypeNameSpace" -> o.keyDef.nameSpace) ~
              ("KeyTypeName" -> ObjType.asString(o.keyDef.tType)) ~
              ("ValueTypeNameSpace" -> o.valDef.nameSpace) ~
              ("ValueTypeName" -> ObjType.asString(o.valDef.tType)) ~
              ("ObjectDefinition" -> o.ObjectDefinition) ~
              ("OrigDef" -> o.OrigDef) ~
              ("Author" -> o.Author) ~
              ("OwnerId" -> o.OwnerId) ~
              ("TenantId" -> o.TenantId) ~
              ("Description" -> o.Description) ~
              ("NumericTypes" -> ("Version" -> o.Version) ~ ("TransId" -> o.TranId) ~ ("UniqId" -> o.UniqId) ~ ("CreationTime" -> o.CreationTime) ~ ("ModTime" -> o.ModTime) ~ ("MdElemStructVer" -> o.MdElemStructVer) ~ ("MdElementId" -> o.MdElementId)) ~
              ("IsActive" -> o.IsActive) ~
              ("IsFixed" -> o.IsFixed) ~
              ("IsDeleted" -> o.IsDeleted)
          compact(render(json))

        }
        case o: ArrayTypeDef => {

          val json = "ArrayType" ->
            ("Name" -> o.Name) ~
              ("NameSpace" -> o.NameSpace) ~
              ("PhysicalName" -> o.PhysicalName) ~
              ("TypeTypeName" -> ObjTypeType.asString(o.elemDef.tTypeType)) ~
              ("TypeName" -> ObjType.asString(o.elemDef.tType)) ~
              ("TypeNameSpace" -> o.elemDef.nameSpace) ~
              ("NumberOfDimensions" -> o.arrayDims) ~
              ("JarName" -> getEmptyIfNull(o.JarName)) ~
              ("ObjectFormat" -> ObjFormatType.asString(o.ObjectFormat)) ~
              ("DependencyJars" -> o.CheckAndGetDependencyJarNames.toList) ~
              ("Implementation" -> o.implementationName) ~
              ("ObjectDefinition" -> o.ObjectDefinition) ~
              ("OrigDef" -> o.OrigDef) ~
              ("OwnerId" -> o.OwnerId) ~
              ("TenantId" -> o.TenantId) ~
              ("Author" -> o.Author) ~
              ("Description" -> o.Description) ~
              ("NumericTypes" -> ("Version" -> o.Version) ~ ("TransId" -> o.TranId) ~ ("UniqId" -> o.UniqId) ~ ("CreationTime" -> o.CreationTime) ~ ("ModTime" -> o.ModTime) ~ ("MdElemStructVer" -> o.MdElemStructVer) ~ ("MdElementId" -> o.MdElementId)) ~
              ("IsActive" -> o.IsActive) ~
              ("IsFixed" -> o.IsFixed) ~
              ("IsDeleted" -> o.IsDeleted)
          compact(render(json))

        }
        case o: JarDef => {
          val json = "Jar" ->
            ("IsActive" -> o.IsActive) ~
              ("IsDeleted" -> o.IsDeleted) ~
              ("OrigDef" -> o.OrigDef) ~
              ("ObjectDefinition" -> o.ObjectDefinition) ~
              ("ObjectFormat" -> ObjFormatType.asString(o.ObjectFormat)) ~
              ("NameSpace" -> o.NameSpace) ~
              ("Name" -> o.Name) ~
              ("Author" -> o.Author) ~
              ("OwnerId" -> o.OwnerId) ~
              ("TenantId" -> o.TenantId) ~
              ("PhysicalName" -> o.PhysicalName) ~
              ("JarName" -> getEmptyIfNull(o.jarName)) ~
              ("DependencyJars" -> o.CheckAndGetDependencyJarNames.toList) ~
              ("NumericTypes" -> ("Version" -> o.Version) ~ ("TransId" -> o.TranId) ~ ("UniqId" -> o.UniqId) ~ ("CreationTime" -> o.CreationTime) ~ ("ModTime" -> o.ModTime) ~ ("MdElemStructVer" -> o.MdElemStructVer) ~ ("MdElementId" -> o.MdElementId)) ~
              ("Description" -> o.description)
          compact(render(json))
        }
        case o: ConfigDef => {
          val json = "Config" ->
            ("IsActive" -> o.IsActive) ~
              ("IsDeleted" -> o.IsDeleted) ~
              ("OrigDef" -> o.OrigDef) ~
              ("ObjectDefinition" -> o.ObjectDefinition) ~
              ("ObjectFormat" -> ObjFormatType.asString(o.ObjectFormat)) ~
              ("NameSpace" -> o.NameSpace) ~
              ("Contents" -> o.contents) ~
              ("OwnerId" -> o.OwnerId) ~
              ("TenantId" -> o.TenantId) ~
              ("Name" -> o.Name) ~
              ("Author" -> o.Author) ~
              ("PhysicalName" -> o.PhysicalName) ~
              ("JarName" -> getEmptyIfNull(o.jarName)) ~
              ("DependencyJars" -> o.CheckAndGetDependencyJarNames.toList) ~
              ("NumericTypes" -> ("Version" -> o.Version) ~ ("TransId" -> o.TranId) ~ ("UniqId" -> o.UniqId) ~ ("CreationTime" -> o.CreationTime) ~ ("ModTime" -> o.ModTime) ~ ("MdElemStructVer" -> o.MdElemStructVer) ~ ("MdElementId" -> o.MdElementId)) ~
              ("Description" -> o.description)
          compact(render(json))
        }
        case o: NodeInfo => {
          val json = "Node" ->
            ("NodeId" -> o.NodeId) ~
              ("NodePort" -> o.NodePort) ~
              ("NodeIpAddr" -> o.NodeIpAddr) ~
              ("JarPaths" -> o.JarPaths.toList) ~
              ("Scala_home" -> o.Scala_home) ~
              ("Java_home" -> o.Java_home) ~
              ("Classpath" -> o.Classpath) ~
              ("ClusterId" -> o.ClusterId) ~
              ("Power" -> o.Power) ~
              ("Roles" -> o.Roles.toList) ~
              ("Description" -> o.Description)
          compact(render(json))
        }
        case o: ClusterInfo => {
          val json = "Cluster" ->
            ("ClusterId" -> o.ClusterId) ~
              ("Description" -> o.Description) ~
              ("Privileges" -> o.Privileges)
          compact(render(json))
        }
        case o: ClusterCfgInfo => {
          val json = "ClusterCfg" ->
            ("ClusterId" -> o.ClusterId) ~
              ("CfgMap" -> o.CfgMap.toList.map(m => ("Key" -> m._1) ~ ("Value" -> m._2))) ~
              ("ModifiedTime" -> o.ModifiedTime.getTime) ~
              ("CreatedTime" -> o.CreatedTime.getTime) ~
              ("UsrConfigs" -> o.getUsrConfigs.toList.map(m => ("Key" -> m._1) ~ ("Value" -> m._2)))
          compact(render(json))
        }
        case o: AdapterInfo => {
          val json = "Adapter" ->
            ("Name" -> o.Name) ~
              ("TypeString" -> o.TypeString) ~
              ("DataFormat" -> o.DataFormat) ~
              ("ClassName" -> o.ClassName) ~
              ("JarName" -> o.JarName) ~
              ("DependencyJars" -> o.DependencyJars.toList) ~
              ("AdapterSpecificCfg" -> o.AdapterSpecificCfg)~
              ("TenantId" -> o.TenantId)
          compact(render(json))
        }
        case o: TenantInfo => {
          val primaryDataStore = if (o.primaryDataStore == null) "" else o.primaryDataStore
          val cacheConfig = if (o.cacheConfig == null) "" else o.cacheConfig
          val json = "Tenant" ->
            ("TenantId" -> o.tenantId) ~
              ("Description" -> o.description) ~
              ("PrimaryDataStore" -> primaryDataStore) ~
              ("CacheConfig" -> cacheConfig)
          compact(render(json))
        }
        case o: UserPropertiesInfo => {
          val json = "UserProperties" ->
            ("ClusterId" -> o.ClusterId) ~
              ("Props" -> o.Props.toList.map(m => ("Key" -> m._1) ~ ("Value" -> m._2)))
          compact(render(json))
        }
        case _ => {
          throw new Exception("serializeObjectToJson doesn't support the objects of type " + mdObj.getClass().getName() + " yet.")
        }
      }
    } catch {
      case e: Exception => {
        logger.debug("Failed to serialize", e)
        throw e
      }
    }
  }

  def deserializeMetadata(metadataJson: String): Any = {

    logger.debug("Parsing the json : " + metadataJson)

    val json = parse(metadataJson)

    val key = json.mapField(k => {
      (k._1, JString(""))
    }).extract[(String, String)]._1

    try {
      key match {
        case "Model" => parseModelDef(json)
        case "Message" => parseMessageDef(json)
        case "Container" => parseContainerDef(json)
        case "Function" => parseFunctionDef(json)
        case "MapType" => parseMapTypeDef(json)
        case "ArrayType" => parseArrayTypeDef(json)
        case "Jar" => parseJarDef(json)
        case "Config" => parseConfigDef(json)
        case "Node" => parseNodeInfo(json)
        case "Cluster" => parseClusterInfo(json)
        case "ClusterCfg" => parseClusterCfgInfo(json)
        case "Adapter" => parseAdapterInfo(json)
        case "Tenant" => parseTenantInfo(json)
        case "UserProperties" => parseUserPropertiesInfo(json)
        case _ => throw new Exception("deserializeMetadata doesn't support the objects of type objectType of " + key + " yet.")
      }
    } catch {
      case e: Exception => {
        logger.debug("Failed to deserialize", e)
        throw e
      }
    }
  }

  private def parseModelDef(modDefJson: JValue): ModelDef = {
    try {

      logger.debug("Parsed the json : " + modDefJson)

      val ModDefInst = modDefJson.extract[ModelDefinition]

      val inputMsgSets = ModDefInst.Model.inputMsgSets.map(m => m.map(k => {
        val msgAndAttrib = new MessageAndAttributes()
        msgAndAttrib.message = k.Message
        msgAndAttrib.origin = k.Origin
        msgAndAttrib.attributes = k.Attributes.toArray
        msgAndAttrib
      }).toArray).toArray


      val modDef = MdMgr.GetMdMgr.MakeModelDef(ModDefInst.Model.NameSpace
        , ModDefInst.Model.Name
        , ModDefInst.Model.PhysicalName
        , ModDefInst.Model.OwnerId
        , ModDefInst.Model.TenantId
        , ModDefInst.Model.NumericTypes.UniqId
        , ModDefInst.Model.NumericTypes.MdElementId
        , ModelRepresentation.modelRep(ModDefInst.Model.ModelRep)
        , inputMsgSets
        , ModDefInst.Model.OutputMsgs.toArray
        , ModDefInst.Model.BooleanTypes.IsReusable
        , ModDefInst.Model.ObjectDefinition
        , MiningModelType.modelType(ModDefInst.Model.ModelType)
        , ModDefInst.Model.NumericTypes.Version
        , ModDefInst.Model.JarName
        , ModDefInst.Model.DependencyJars.toArray
        , false
        , ModDefInst.Model.BooleanTypes.SupportsInstanceSerialization,
        ModDefInst.Model.ModelConfig)


      val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(ModDefInst.Model.ObjectFormat)
      modDef.ObjectFormat(objFmt)
      modDef.tranId = ModDefInst.Model.NumericTypes.TransId
      modDef.origDef = ModDefInst.Model.OrigDef
      modDef.creationTime = ModDefInst.Model.NumericTypes.CreationTime
      modDef.modTime = ModDefInst.Model.NumericTypes.ModTime
      modDef.description = ModDefInst.Model.Description
      modDef.author = ModDefInst.Model.Author
      modDef.mdElemStructVer = ModDefInst.Model.NumericTypes.MdElemStructVer
      modDef.active = ModDefInst.Model.BooleanTypes.IsActive
      modDef.deleted = ModDefInst.Model.BooleanTypes.IsDeleted

      modDef
    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw ModelDefParsingException(e.getMessage(), e)
      }
    }
  }

  private def parseMessageDef(msgDefJson: JValue): MessageDef = {
    try {
      logger.debug("Parsed the json : " + msgDefJson)

      val MsgDefInst = msgDefJson.extract[MessageDefin]
      val attrList = MsgDefInst.Message.Attributes
      var attrList1 = List[(String, String, String, String, Boolean, String)]()
      for (attr <- attrList) {
        attrList1 ::=(attr.NameSpace, attr.Name, attr.Type.TypeNameSpace, attr.Type.TypeName, false, attr.CollectionType.get)
      }

      var primaryKeys = List[(String, List[String])]()
      val pr = MsgDefInst.Message.PrimaryKeys

      pr.foreach(f => {
        primaryKeys ::=(f.constraintName, f.key)
      })

      var foreignKeys = List[(String, List[String], String, List[String])]()
      val fr = MsgDefInst.Message.ForeignKeys

      fr.foreach(f => {
        foreignKeys ::=(f.constraintName, f.key, f.forignContainerName, f.forignKey)
      })
      val msgDef = MdMgr.GetMdMgr.MakeFixedMsg(
        MsgDefInst.Message.NameSpace,
        MsgDefInst.Message.Name,
        MsgDefInst.Message.PhysicalName,
        attrList1,
        MsgDefInst.Message.OwnerId,
        MsgDefInst.Message.TenantId,
        MsgDefInst.Message.NumericTypes.UniqId,
        MsgDefInst.Message.NumericTypes.MdElementId,
        MsgDefInst.Message.SchemaId,
        MsgDefInst.Message.AvroSchema,
        MsgDefInst.Message.NumericTypes.Version,
        MsgDefInst.Message.JarName,
        MsgDefInst.Message.DependencyJars.toArray,
        primaryKeys,
        foreignKeys,
        MsgDefInst.Message.PartitionKey.toArray,
        false, MsgDefInst.Message.Persist
      )

      msgDef.tranId = MsgDefInst.Message.NumericTypes.TransId
      msgDef.origDef = MsgDefInst.Message.OrigDef
      msgDef.ObjectDefinition(MsgDefInst.Message.ObjectDefinition)
      val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(MsgDefInst.Message.ObjectFormat)
      msgDef.ObjectFormat(objFmt)
      msgDef.creationTime = MsgDefInst.Message.NumericTypes.CreationTime
      msgDef.modTime = MsgDefInst.Message.NumericTypes.ModTime
      msgDef.description = MsgDefInst.Message.Description
      msgDef.author = MsgDefInst.Message.Author
      msgDef.mdElemStructVer = MsgDefInst.Message.NumericTypes.MdElemStructVer
      msgDef.cType.persist = MsgDefInst.Message.Persist
      msgDef.active = MsgDefInst.Message.IsActive
      msgDef.deleted = MsgDefInst.Message.IsDeleted
      msgDef
    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw MessageDefParsingException(e.getMessage(), e)
      }
    }
  }

  private def parseContainerDef(contDefJson: JValue): ContainerDef = {
    try {

      logger.debug("Parsed the json : " + contDefJson)

      val ContDefInst = contDefJson.extract[ContainerDefin]
      val attrList = ContDefInst.Container.Attributes
      var attrList1 = List[(String, String, String, String, Boolean, String)]()
      for (attr <- attrList) {
        attrList1 ::=(attr.NameSpace, attr.Name, attr.Type.TypeNameSpace, attr.Type.TypeName, false, attr.CollectionType.get)
      }

      var primaryKeys = List[(String, List[String])]()
      val pr = ContDefInst.Container.PrimaryKeys

      pr.foreach(m => {
        primaryKeys ::=(m.constraintName, m.key)
      })
      var foreignKeys = List[(String, List[String], String, List[String])]()
      val fr = ContDefInst.Container.ForeignKeys

      fr.foreach(f => {
        foreignKeys ::=(f.constraintName, f.key, f.forignContainerName, f.forignKey)
      })


      val contDef = MdMgr.GetMdMgr.MakeFixedContainer(
        ContDefInst.Container.NameSpace,
        ContDefInst.Container.Name,
        ContDefInst.Container.PhysicalName,
        attrList1,
        ContDefInst.Container.OwnerId,
        ContDefInst.Container.TenantId,
        ContDefInst.Container.NumericTypes.UniqId,
        ContDefInst.Container.NumericTypes.MdElementId,
        ContDefInst.Container.SchemaId,
        ContDefInst.Container.AvroSchema,
        ContDefInst.Container.NumericTypes.Version,
        ContDefInst.Container.JarName,
        ContDefInst.Container.DependencyJars.toArray,
        primaryKeys,
        foreignKeys,
        ContDefInst.Container.PartitionKey.toArray,
        false, ContDefInst.Container.Persist
      )

      contDef.tranId = ContDefInst.Container.NumericTypes.TransId
      contDef.origDef = ContDefInst.Container.OrigDef
      contDef.ObjectDefinition(ContDefInst.Container.ObjectDefinition)
      val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(ContDefInst.Container.ObjectFormat)
      contDef.ObjectFormat(objFmt)
      contDef.creationTime = ContDefInst.Container.NumericTypes.CreationTime
      contDef.modTime = ContDefInst.Container.NumericTypes.ModTime
      contDef.description = ContDefInst.Container.Description
      contDef.author = ContDefInst.Container.Author
      contDef.mdElemStructVer = ContDefInst.Container.NumericTypes.MdElemStructVer
      contDef.cType.persist = ContDefInst.Container.Persist
      contDef.active = ContDefInst.Container.IsActive
      contDef.deleted = ContDefInst.Container.IsDeleted
      contDef
    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw ContainerDefParsingException(e.getMessage(), e)
      }
    }
  }

  private def parseFunctionDef(functionDefJson: JValue): FunctionDef = {
    try {

      logger.debug("Parsed the json : " + functionDefJson)

      val functionInst = functionDefJson.extract[Function]
      val argList = functionInst.Function.Arguments.map(arg => (arg.ArgName, arg.ArgTypeNameSpace, arg.ArgTypeName))
      var featureSet: scala.collection.mutable.Set[FcnMacroAttr.Feature] = scala.collection.mutable.Set[FcnMacroAttr.Feature]()
      if (functionInst.Function.Features != null) {
        functionInst.Function.Features.foreach(arg => featureSet += FcnMacroAttr.fromString(arg))
      }

      val functionDef = MdMgr.GetMdMgr.MakeFunc(functionInst.Function.NameSpace,
        functionInst.Function.Name,
        functionInst.Function.PhysicalName,
        (functionInst.Function.ReturnTypeNameSpace, functionInst.Function.ReturnTypeName),
        argList,
        featureSet,
        functionInst.Function.OwnerId,
        functionInst.Function.TenantId,
        functionInst.Function.NumericTypes.UniqId,
        functionInst.Function.NumericTypes.MdElementId,
        functionInst.Function.NumericTypes.Version,
        functionInst.Function.JarName,
        functionInst.Function.DependencyJars.toArray)
      functionDef.origDef = functionInst.Function.OrigDef
      val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(functionInst.Function.ObjectFormat)
      functionDef.ObjectFormat(objFmt)
      functionDef.ObjectDefinition(functionInst.Function.ObjectDefinition)
      functionDef.author = functionInst.Function.Author
      functionDef.className = functionInst.Function.ClassName
      functionDef.description = functionInst.Function.Description
      functionDef.tranId = functionInst.Function.NumericTypes.TransId
      functionDef.creationTime = functionInst.Function.NumericTypes.CreationTime
      functionDef.modTime = functionInst.Function.NumericTypes.ModTime
      functionDef.mdElemStructVer = functionInst.Function.NumericTypes.MdElemStructVer
      functionDef.active = functionInst.Function.IsActive
      functionDef.deleted = functionInst.Function.IsDeleted

      functionDef
    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw FunctionParsingException(e.getMessage(), e)
      }
    }
  }

  private def parseMapTypeDef(mapTypeDefJson: JValue): MapTypeDef = {
    try {

      logger.debug("Parsed the json : " + mapTypeDefJson)

      val mapTypeInst = mapTypeDefJson.extract[MapType]

      val key = (mapTypeInst.MapType.KeyTypeNameSpace, mapTypeInst.MapType.KeyTypeName)
      val value = (mapTypeInst.MapType.ValueTypeNameSpace, mapTypeInst.MapType.ValueTypeName)

      val mapTypeDef = MdMgr.GetMdMgr.MakeMap(mapTypeInst.MapType.NameSpace,
        mapTypeInst.MapType.Name,
        key,
        value,
        mapTypeInst.MapType.NumericTypes.Version,
        mapTypeInst.MapType.OwnerId,
        mapTypeInst.MapType.TenantId,
        mapTypeInst.MapType.NumericTypes.UniqId,
        mapTypeInst.MapType.NumericTypes.MdElementId,
        false
      )

      mapTypeDef.origDef = mapTypeInst.MapType.OrigDef
      val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(mapTypeInst.MapType.ObjectFormat)
      mapTypeDef.ObjectFormat(objFmt)
      mapTypeDef.author = mapTypeInst.MapType.Author
      mapTypeDef.description = mapTypeInst.MapType.Description
      mapTypeDef.tranId = mapTypeInst.MapType.NumericTypes.TransId
      mapTypeDef.creationTime = mapTypeInst.MapType.NumericTypes.CreationTime
      mapTypeDef.modTime = mapTypeInst.MapType.NumericTypes.ModTime
      mapTypeDef.mdElemStructVer = mapTypeInst.MapType.NumericTypes.MdElemStructVer
      mapTypeDef.active = mapTypeInst.MapType.IsActive
      mapTypeDef.deleted = mapTypeInst.MapType.IsDeleted
      mapTypeDef.implementationName(mapTypeInst.MapType.Implementation)
      mapTypeDef.dependencyJarNames = mapTypeInst.MapType.DependencyJars.toArray
      mapTypeDef.jarName = mapTypeInst.MapType.JarName
      mapTypeDef.PhysicalName(mapTypeInst.MapType.PhysicalName)
      mapTypeDef.ObjectDefinition(mapTypeInst.MapType.ObjectDefinition)

      mapTypeDef

    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw TypeParsingException(e.getMessage(), e)
      }
    }
  }

  private def parseArrayTypeDef(arrayTypeDefJson: JValue): ArrayTypeDef = {
    try {

      logger.debug("Parsed the json : " + arrayTypeDefJson)

      val arrayTypeInst = arrayTypeDefJson.extract[ArrayType]

      val arrayTypeDef = MdMgr.GetMdMgr.MakeArray(arrayTypeInst.ArrayType.NameSpace,
        arrayTypeInst.ArrayType.Name,
        arrayTypeInst.ArrayType.TypeNameSpace,
        arrayTypeInst.ArrayType.TypeName,
        arrayTypeInst.ArrayType.NumberOfDimensions,
        arrayTypeInst.ArrayType.OwnerId,
        arrayTypeInst.ArrayType.TenantId,
        arrayTypeInst.ArrayType.NumericTypes.UniqId,
        arrayTypeInst.ArrayType.NumericTypes.MdElementId,
        arrayTypeInst.ArrayType.NumericTypes.Version,
        false
      )

      arrayTypeDef.origDef = arrayTypeInst.ArrayType.OrigDef
      val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(arrayTypeInst.ArrayType.ObjectFormat)
      arrayTypeDef.ObjectFormat(objFmt)
      arrayTypeDef.author = arrayTypeInst.ArrayType.Author
      arrayTypeDef.description = arrayTypeInst.ArrayType.Description
      arrayTypeDef.tranId = arrayTypeInst.ArrayType.NumericTypes.TransId
      arrayTypeDef.creationTime = arrayTypeInst.ArrayType.NumericTypes.CreationTime
      arrayTypeDef.modTime = arrayTypeInst.ArrayType.NumericTypes.ModTime
      arrayTypeDef.mdElemStructVer = arrayTypeInst.ArrayType.NumericTypes.MdElemStructVer
      arrayTypeDef.active = arrayTypeInst.ArrayType.IsActive
      arrayTypeDef.deleted = arrayTypeInst.ArrayType.IsDeleted
      arrayTypeDef.implementationName(arrayTypeInst.ArrayType.Implementation)
      arrayTypeDef.dependencyJarNames = arrayTypeInst.ArrayType.DependencyJars.toArray
      arrayTypeDef.jarName = arrayTypeInst.ArrayType.JarName
      arrayTypeDef.PhysicalName(arrayTypeInst.ArrayType.PhysicalName)
      arrayTypeDef.ObjectDefinition(arrayTypeInst.ArrayType.ObjectDefinition)

      arrayTypeDef

    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw TypeParsingException(e.getMessage(), e)
      }
    }
  }

  private def parseJarDef(jarDefJson: JValue): JarDef = {
    try {
      logger.debug("Parsed the json : " + jarDefJson)

      val jarInst = jarDefJson.extract[Jar]

      val jarDef = MdMgr.GetMdMgr.MakeJarDef(jarInst.Jar.NameSpace,
        jarInst.Jar.Name,
        jarInst.Jar.NumericTypes.Version.toString,
        jarInst.Jar.OwnerId,
        jarInst.Jar.TenantId,
        jarInst.Jar.NumericTypes.UniqId,
        jarInst.Jar.NumericTypes.MdElementId
      )

      jarDef.origDef = jarInst.Jar.OrigDef
      val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(jarInst.Jar.ObjectFormat)
      jarDef.ObjectFormat(objFmt)
      jarDef.author = jarInst.Jar.Author
      jarDef.description = jarInst.Jar.Description
      jarDef.tranId = jarInst.Jar.NumericTypes.TransId
      jarDef.creationTime = jarInst.Jar.NumericTypes.CreationTime
      jarDef.modTime = jarInst.Jar.NumericTypes.ModTime
      jarDef.mdElemStructVer = jarInst.Jar.NumericTypes.MdElemStructVer
      jarDef.active = jarInst.Jar.IsActive
      jarDef.deleted = jarInst.Jar.IsDeleted
      jarDef.dependencyJarNames = jarInst.Jar.DependencyJars.toArray
      jarDef.jarName = jarInst.Jar.JarName
      jarDef.PhysicalName(jarInst.Jar.PhysicalName)
      jarDef.ObjectDefinition(jarInst.Jar.ObjectDefinition)

      jarDef

    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.debug("", e)
        throw JarDefParsingException(e.getMessage(), e)
      }
    }
  }

  private def parseConfigDef(configDefJson: JValue): ConfigDef = {
    try {
      logger.debug("Parsed the json : " + configDefJson)

      val configInst = configDefJson.extract[Config]

      val configDef = new ConfigDef

      configDef.nameSpace = configInst.Config.NameSpace;
      configDef.name = configInst.Config.Name;
      configDef.ver = configInst.Config.NumericTypes.Version;
      configDef.origDef = configInst.Config.OrigDef
      val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(configInst.Config.ObjectFormat)
      configDef.ObjectFormat(objFmt)
      configDef.author = configInst.Config.Author
      configDef.description = configInst.Config.Description
      configDef.tranId = configInst.Config.NumericTypes.TransId
      configDef.uniqueId = configInst.Config.NumericTypes.UniqId
      configDef.creationTime = configInst.Config.NumericTypes.CreationTime
      configDef.modTime = configInst.Config.NumericTypes.ModTime
      configDef.mdElemStructVer = configInst.Config.NumericTypes.MdElemStructVer
      configDef.active = configInst.Config.IsActive
      configDef.deleted = configInst.Config.IsDeleted
      configDef.ownerId = configInst.Config.OwnerId
      configDef.tenantId= configInst.Config.TenantId
      configDef.mdElementId = configInst.Config.NumericTypes.MdElementId
      configDef.dependencyJarNames = configInst.Config.DependencyJars.toArray
      configDef.jarName = configInst.Config.JarName
      configDef.contents = configInst.Config.Contents
      configDef.PhysicalName(configInst.Config.PhysicalName)
      configDef.ObjectDefinition(configInst.Config.ObjectDefinition)

      configDef

    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.error("Failed to parse JSON" + configDefJson, e)
        throw e
      }
    }
  }

  private def parseNodeInfo(nodeInfoJson: JValue): NodeInfo = {
    try {
      logger.debug("Parsed the json : " + nodeInfoJson)
      val nodeInst = nodeInfoJson.extract[Node]

      val nodeInfo = MdMgr.GetMdMgr.MakeNode(
        nodeInst.Node.NodeId,
        nodeInst.Node.NodePort,
        nodeInst.Node.NodeIpAddr,
        nodeInst.Node.JarPaths,
        nodeInst.Node.Scala_home,
        nodeInst.Node.Java_home,
        nodeInst.Node.Classpath,
        nodeInst.Node.ClusterId,
        nodeInst.Node.Power,
        nodeInst.Node.Roles.toArray,
        nodeInst.Node.Description)

      nodeInfo

    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.error("Failed to parse JSON" + nodeInfoJson, e)
        throw e
      }
    }
  }

  private def parseClusterInfo(clusterInfoJson: JValue): ClusterInfo = {
    try {
      logger.debug("Parsed the json : " + clusterInfoJson)

      val clusterInst = clusterInfoJson.extract[Cluster]

      val clusterInfo = MdMgr.GetMdMgr.MakeCluster(
        clusterInst.Cluster.ClusterId,
        clusterInst.Cluster.Description,
        clusterInst.Cluster.Privileges
      )
      clusterInfo

    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.error("Failed to parse JSON" + clusterInfoJson, e)
        throw e
      }
    }
  }

  private def parseClusterCfgInfo(clusterCfgInfoJson: JValue): ClusterCfgInfo = {
    try {
      logger.debug("Parsed the json : " + clusterCfgInfoJson)

      val clusterCfgInst = clusterCfgInfoJson.extract[ClusterCfg]

      val cfgMap = new scala.collection.mutable.HashMap[String, String]
      for (m <- clusterCfgInst.ClusterCfg.CfgMap) {
        cfgMap.put(m.Key, m.Value)
      }

      val clusterCfgInfo = MdMgr.GetMdMgr.MakeClusterCfg(
        clusterCfgInst.ClusterCfg.ClusterId,
        cfgMap,
        new Date(clusterCfgInst.ClusterCfg.ModifiedTime),
        new Date(clusterCfgInst.ClusterCfg.CreatedTime)
      )
      val usrConfigs = new scala.collection.mutable.HashMap[String, String]

      for (m <- clusterCfgInst.ClusterCfg.UsrConfigs) {
        usrConfigs.put(m.Key, m.Value)
      }

      clusterCfgInfo.usrConfigs = usrConfigs

      clusterCfgInfo

    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.error("Failed to parse JSON" + clusterCfgInfoJson, e)
        throw e
      }
    }
  }

  private def parseTenantInfo(tenantInfoJson: JValue): TenantInfo = {
    try {
      logger.debug("Parsed the json : " + tenantInfoJson)

      val tenantInst = tenantInfoJson.extract[Tenant]

      val tenantInfo = MdMgr.GetMdMgr.MakeTenantInfo(
        tenantInst.Tenant.TenantId,
        tenantInst.Tenant.Description,
        tenantInst.Tenant.PrimaryDataStore,
        tenantInst.Tenant.CacheConfig
      )
      tenantInfo
    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.error("Failed to parse JSON" + tenantInfoJson, e)
        throw e
      }
    }
  }

  private def parseAdapterInfo(adapterInfoJson: JValue): AdapterInfo = {
    try {
      logger.debug("Parsed the json : " + adapterInfoJson)

      val adapterInst = adapterInfoJson.extract[Adapter]

      val adapterInfo = MdMgr.GetMdMgr.MakeAdapter(
        adapterInst.Adapter.Name,
        adapterInst.Adapter.TypeString,
        adapterInst.Adapter.DataFormat,
        adapterInst.Adapter.ClassName,
        adapterInst.Adapter.JarName,
        adapterInst.Adapter.DependencyJars,
        adapterInst.Adapter.AdapterSpecificCfg,
        adapterInst.Adapter.TenantId
      )
      adapterInfo

    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.error("Failed to parse JSON" + adapterInfoJson, e)
        throw e
      }
    }
  }

  private def parseUserPropertiesInfo(userPropertiesInfoJson: JValue): UserPropertiesInfo = {
    try {
      logger.debug("Parsed the json : " + userPropertiesInfoJson)

      val userPropertiesInst = userPropertiesInfoJson.extract[UserProperties]

      val props = new scala.collection.mutable.HashMap[String, String]
      for (m <- userPropertiesInst.UserProperties.Props) {
        props.put(m.Key, m.Value)
      }

      val upi = new UserPropertiesInfo
      upi.clusterId = userPropertiesInst.UserProperties.ClusterId
      upi.props = props
      upi

    } catch {
      case e: MappingException => {
        logger.debug("", e)
        throw Json4sParsingException(e.getMessage(), e)
      }
      case e: Exception => {
        logger.error("Failed to parse JSON" + userPropertiesInfoJson, e)
        throw e
      }
    }
  }

  private def getEmptyIfNull(jarName: String): String = {
    if (jarName != null) jarName else ""
  }

}

case class UserPropertiesInformation(ClusterId: String, Props: List[KeyVale])

case class UserProperties(UserProperties: UserPropertiesInformation)

case class AdapterInformation(Name: String, TypeString: String, DataFormat: String, ClassName: String, JarName: String, DependencyJars: List[String], AdapterSpecificCfg: String, TenantId: String)

case class Adapter(Adapter: AdapterInformation)

case class TenantInformation(TenantId: String, Description: String, PrimaryDataStore: String, CacheConfig: String)

case class Tenant(Tenant: TenantInformation)

case class KeyVale(Key: String, Value: String)

case class ClusterCfgInformation(ClusterId: String, CfgMap: List[KeyVale], ModifiedTime: Long, CreatedTime: Long, UsrConfigs: List[KeyVale])

case class ClusterCfg(ClusterCfg: ClusterCfgInformation)

case class ClusterInformation(ClusterId: String, Description: String, Privileges: String)

case class Cluster(Cluster: ClusterInformation)

case class NodeInformation(NodeId: String, NodePort: Int, NodeIpAddr: String, JarPaths: List[String], Scala_home: String, Java_home: String, Classpath: String, ClusterId: String, Power: Int, Roles: List[String], Description: String)

case class Node(Node: NodeInformation)

case class Argument(ArgName: String, ArgTypeNameSpace: String, ArgTypeName: String)

case class ConfigInfo(Name: String, PhysicalName: String, JarName: String, NameSpace: String, ObjectDefinition: String, DependencyJars: List[String], OrigDef: String, ObjectFormat: String, Author: String, OwnerId: String, IsActive: Boolean, IsDeleted: Boolean, Description: String, Contents: String, NumericTypes: NumericTypes, TenantId: String)

case class Config(Config: ConfigInfo)

case class JarInfo(Name: String, PhysicalName: String, JarName: String, NameSpace: String, ObjectDefinition: String, DependencyJars: List[String], OrigDef: String, ObjectFormat: String, Author: String, OwnerId: String, IsActive: Boolean, IsDeleted: Boolean, Description: String, NumericTypes: NumericTypes, TenantId: String)

case class Jar(Jar: JarInfo)

case class ArrayTypeInfo(Name: String, PhysicalName: String, JarName: String, NameSpace: String, TypeTypeName: String, Implementation: String, NumberOfDimensions: Int, TypeName: String, TypeNameSpace: String, ObjectDefinition: String, DependencyJars: List[String], OrigDef: String, ObjectFormat: String, Author: String, OwnerId: String, IsActive: Boolean, IsDeleted: Boolean, IsFixed: Boolean, Description: String, NumericTypes: NumericTypes, TenantId: String)

case class ArrayType(ArrayType: ArrayTypeInfo)

case class MapTypeInfo(Name: String, PhysicalName: String, JarName: String, NameSpace: String, TypeTypeName: String, Implementation: String, KeyTypeNameSpace: String, KeyTypeName: String, ValueTypeNameSpace: String, ValueTypeName: String, ObjectDefinition: String, DependencyJars: List[String], OrigDef: String, ObjectFormat: String, Author: String, OwnerId: String, IsActive: Boolean, IsDeleted: Boolean, IsFixed: Boolean, Description: String, NumericTypes: NumericTypes, TenantId: String)

case class MapType(MapType: MapTypeInfo)

case class FunctionInfo(Name: String, PhysicalName: String, JarName: String, NameSpace: String, DependencyJars: List[String], OrigDef: String, ObjectFormat: String, Author: String, Arguments: List[Argument], Features: List[String], ReturnTypeNameSpace: String, ObjectDefinition: String, ReturnTypeName: String, ClassName: String, OwnerId: String, IsActive: Boolean, IsDeleted: Boolean, Description: String, NumericTypes: NumericTypes, TenantId: String)

case class Function(Function: FunctionInfo)

case class Attr(NameSpace: String, Name: String, Version: Long, CollectionType: Option[String], Type: TypeDef)

case class MessageInfo(NameSpace: String, Name: String, JarName: String, PhysicalName: String, DependencyJars: List[String], Attributes: List[Attr], OrigDef: String, ObjectDefinition: String, ObjectFormat: String, Description: String, OwnerId: String, Author: String, PartitionKey: List[String], Persist: Boolean, IsActive: Boolean, IsDeleted: Boolean, SchemaId: Int, AvroSchema: String, PrimaryKeys: List[PrimaryKeys], ForeignKeys: List[ForeignKeys], NumericTypes: NumericTypes, TenantId: String)

case class PrimaryKeys(constraintName: String, key: List[String])

case class ForeignKeys(constraintName: String, key: List[String], forignContainerName: String, forignKey: List[String])

case class MessageDefin(Message: MessageInfo)

case class ContainerDefin(Container: MessageInfo)

case class ModelDefinition(Model: ModelInfo)

case class MsgAndAttrib(Origin: String, Message: String, Attributes: List[String])

case class ModelInfo(Name: String, PhysicalName: String, JarName: String, NameSpace: String, ObjectFormat: String, BooleanTypes: BooleanTypes, OwnerId: String, OutputMsgs: List[String], ModelType: String, DependencyJars: List[String], ModelRep: String, OrigDef: String, ObjectDefinition: String, NumericTypes: NumericTypes, Description: String, Author: String, ModelConfig: String, inputMsgSets: List[List[MsgAndAttrib]], TenantId: String)

case class NumericTypes(Version: Long, TransId: Long, UniqId: Long, CreationTime: Long, ModTime: Long, MdElemStructVer: Int, MdElementId: Long)

case class BooleanTypes(IsReusable: Boolean, IsActive: Boolean, SupportsInstanceSerialization: Boolean, IsDeleted: Boolean)