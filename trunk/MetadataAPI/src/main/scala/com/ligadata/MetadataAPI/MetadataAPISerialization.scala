package com.ligadata.MetadataAPI

import com.ligadata.Exceptions.{Json4sParsingException, MessageDefParsingException, ModelDefParsingException}
import com.ligadata.Serialize.TypeDef
import com.ligadata.kamanja.metadata._
import org.apache.logging.log4j.LogManager
import org.json4s.{DefaultFormats, Formats, MappingException}
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


/**
  * Created by Ahmed-Work on 3/15/2016.
  */


object MetadataAPISerialization {

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)

  def serializeObjectToJson(mdObj: BaseElem): (String, String) = {
    //logger.debug("mdObj.Version before conversion =>" + mdObj.Version)
    //val ver = MdMgr.ConvertLongVersionToString(mdObj.Version)
    //logger.debug("mdObj.Version after conversion  =>" + ver)

    val ver = mdObj.Version
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
              ("InputAttributes" -> o.inputVars.toList.map(m => ("NameSpace" -> m.NameSpace) ~ ("Name" -> m.Name) ~ ("Version" -> m.Version))) ~
              ("OutputAttributes" -> o.outputVars.toList.map(m => ("NameSpace" -> m.NameSpace) ~ ("Name" -> m.Name) ~ ("Version" -> m.Version))) ~
              ("ModelRep" -> o.modelRepresentation.toString) ~
              ("OrigDef" -> o.OrigDef) ~
              ("MsgConsumed" -> "") ~
              ("JpmmlStr" -> "") ~
              ("ObjectDefinition" -> o.ObjectDefinition) ~
              ("ObjectFormat" -> ObjFormatType.asString(o.ObjectFormat)) ~
              ("Description" -> o.Description) ~
              ("Author" -> o.Author) ~
              ("NumericTypes" -> ("Version" -> ver) ~ ("TransId" -> o.TranId) ~ ("UniqID" -> o.UniqID) ~ ("CreationTime" -> o.CreationTime) ~ ("ModTime" -> o.ModTime) ~ ("MdElemStructVer" -> o.MdElemStructVer)) ~
              ("BooleanTypes" -> ("IsActive" -> o.IsActive) ~ ("IsReusable" -> o.isReusable) ~ ("IsDeleted" -> o.IsDeleted) ~ ("Recompile" -> false) ~ ("SupportsInstanceSerialization" -> o.SupportsInstanceSerialization)
                )
          ("ModelDef", compact(render(json)))
        }
        case o: MessageDef => {

          var primaryKeys = List[(String, List[String])]()
          var foreignKeys = List[(String, List[String], String, List[String])]()

          o.cType.Keys.toList.map(m => {
            if (m.KeyType == RelationKeyType.tPrimary) {
              var pr = m.asInstanceOf[PrimaryKey]
              primaryKeys ::=(pr.constraintName, pr.key.toList)
            } else {
              var fr = m.asInstanceOf[ForeignKey]
              foreignKeys ::=(fr.constraintName, fr.key.toList, fr.forignContainerName, fr.forignKey.toList)
            }
          })

          val json = "Message" ->
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
              ("PartitionKey" -> o.cType.PartitionKey.toList) ~
              ("Persist" -> o.cType.Persist) ~
              ("IsActive" -> o.IsActive) ~
              ("IsDeleted" -> o.IsDeleted) ~
              ("Recompile" -> false) ~
              ("Description" -> o.Description) ~
              ("NumericTypes" -> ("Version" -> ver) ~ ("TransId" -> o.TranId) ~ ("UniqID" -> o.UniqID) ~ ("CreationTime" -> o.CreationTime) ~ ("ModTime" -> o.ModTime) ~ ("MdElemStructVer" -> o.MdElemStructVer)) ~
              ("PrimaryKeys" -> primaryKeys.map(m => ("constraintName" -> m._1) ~ ("key" -> m._2))) ~
              ("ForeignKeys" -> foreignKeys.map(m => ("constraintName" -> m._1) ~ ("key" -> m._2) ~ ("forignContainerName" -> m._3) ~ ("forignKey" -> m._4)))
          ("MessageDef", compact(render(json)))
        }
        case o: ContainerDef => {

          var primaryKeys = List[(String, List[String])]()
          var foreignKeys = List[(String, List[String], String, List[String])]()
          o.cType.Keys.toList.map(m => {
            if (m.KeyType == RelationKeyType.tPrimary) {
              var pr = m.asInstanceOf[PrimaryKey]
              primaryKeys ::=(pr.constraintName, pr.key.toList)
            } else {
              var fr = m.asInstanceOf[ForeignKey]
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
              ("PartitionKey" -> o.cType.PartitionKey.toList) ~
              ("Persist" -> o.cType.Persist) ~
              ("IsActive" -> o.IsActive) ~
              ("IsDeleted" -> o.IsDeleted) ~
              ("Recompile" -> false) ~
              ("Description" -> o.Description) ~
              ("NumericTypes" -> ("Version" -> ver) ~ ("TransId" -> o.TranId) ~ ("UniqID" -> o.UniqID) ~ ("CreationTime" -> o.CreationTime) ~ ("ModTime" -> o.ModTime) ~ ("MdElemStructVer" -> o.MdElemStructVer)) ~
              ("PrimaryKeys" -> primaryKeys.map(m => ("constraintName" -> m._1) ~ ("key" -> m._2))) ~
              ("ForeignKeys" -> foreignKeys.map(m => ("constraintName" -> m._1) ~ ("key" -> m._2) ~ ("forignContainerName" -> m._3) ~ ("forignKey" -> m._4)))
          ("ContainerDef", compact(render(json)))
        }
        case _ => {
          throw new Exception("serializeObjectToJson doesn't support the objects of type objectType of " + mdObj.getClass().getName() + " yet.")
        }
      }
    } catch {
      case e: Exception => {
        logger.debug("Failed to serialize", e)
        throw e
      }
    }
  }


  def parseModelDef(modDefJson: String, formatType: String): ModelDef = {
    try {
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(modDefJson)

      logger.debug("Parsed the json : " + modDefJson)

      val ModDefInst = json.extract[ModelDefinition]

      val inputAttrList = ModDefInst.Model.InputAttributes
      var inputAttrList1 = List[(String, String, String, String, Boolean, String)]()
      for (attr <- inputAttrList) {
        inputAttrList1 ::=(attr.NameSpace, attr.Name, attr.Type.NameSpace, attr.Type.Name, false, attr.CollectionType.get)
      }

      val outputAttrList = ModDefInst.Model.OutputAttributes
      var outputAttrList1 = List[(String, String, String)]()
      for (attr <- outputAttrList) {
        outputAttrList1 ::=(attr.Name, attr.Type.NameSpace, attr.Type.Name)
      }

      val modDef = MdMgr.GetMdMgr.MakeModelDef(ModDefInst.Model.NameSpace
        , ModDefInst.Model.Name
        , ModDefInst.Model.PhysicalName
        , ModelRepresentation.modelRep(ModDefInst.Model.ModelRep)
        , ModDefInst.Model.BooleanTypes.IsReusable
        , ModDefInst.Model.MsgConsumed
        , ModDefInst.Model.JpmmlStr
        , MiningModelType.modelType(ModDefInst.Model.ModelType)
        , inputAttrList1
        , outputAttrList1
        , ModDefInst.Model.NumericTypes.Version
        , ModDefInst.Model.JarName
        , ModDefInst.Model.DependencyJars.toArray
        , ModDefInst.Model.BooleanTypes.Recompile
        , ModDefInst.Model.BooleanTypes.SupportsInstanceSerialization)


      modDef.ObjectDefinition(ModDefInst.Model.ObjectDefinition)
      val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(ModDefInst.Model.ObjectFormat)
      modDef.ObjectFormat(objFmt)
      modDef.tranId = ModDefInst.Model.NumericTypes.TransId
      modDef.origDef = ModDefInst.Model.OrigDef
      modDef.uniqueId = ModDefInst.Model.NumericTypes.UniqID
      modDef.creationTime = ModDefInst.Model.NumericTypes.CreationTime
      modDef.modTime = ModDefInst.Model.NumericTypes.ModTime
      modDef.description = ModDefInst.Model.Description
      modDef.author = ModDefInst.Model.Author
      modDef.mdElemStructVer = ModDefInst.Model.NumericTypes.MdElemStructVer
      modDef.active = ModDefInst.Model.BooleanTypes.IsActive
      modDef.deleted = ModDefInst.Model.BooleanTypes.IsDeleted
      //modDef.isReusable = ModDefInst.Model.BooleanTypes.IsReusable
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

  def parseMessageDef(msgDefJson: String, formatType: String): MessageDef = {
    try {
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(msgDefJson)

      logger.debug("Parsed the json : " + msgDefJson)

      val MsgDefInst = json.extract[MessageDefin]
      val attrList = MsgDefInst.Message.Attributes
      var attrList1 = List[(String, String, String, String, Boolean, String)]()
      for (attr <- attrList) {
        attrList1 ::=(attr.NameSpace, attr.Name, attr.Type.TypeNameSpace, attr.Type.TypeName, false, attr.CollectionType.get)
      }

      var primaryKeys = List[(String, List[String])]()
      val pr = MsgDefInst.Message.PrimaryKeys
      if (pr.isDefined)
        primaryKeys ::=(pr.get.constraintName, pr.get.key)
      var foreignKeys = List[(String, List[String], String, List[String])]()
      val fr = MsgDefInst.Message.ForeignKeys
      if (fr.isDefined)
        foreignKeys ::=(fr.get.constraintName, fr.get.key, fr.get.forignContainerName, fr.get.forignKey)


      val msgDef = MdMgr.GetMdMgr.MakeFixedMsg(
        MsgDefInst.Message.NameSpace,
        MsgDefInst.Message.Name,
        MsgDefInst.Message.PhysicalName,
        attrList1,
        MsgDefInst.Message.NumericTypes.Version,
        MsgDefInst.Message.JarName,
        MsgDefInst.Message.DependencyJars.toArray,
        primaryKeys,
        foreignKeys,
        MsgDefInst.Message.PartitionKey.toArray,
        MsgDefInst.Message.Recompile,
        MsgDefInst.Message.Persist
      )

      msgDef.tranId = MsgDefInst.Message.NumericTypes.TransId
      msgDef.origDef = MsgDefInst.Message.OrigDef
      msgDef.ObjectDefinition(MsgDefInst.Message.ObjectDefinition)
      val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(MsgDefInst.Message.ObjectFormat)
      msgDef.ObjectFormat(objFmt)
      msgDef.uniqueId = MsgDefInst.Message.NumericTypes.UniqID
      msgDef.creationTime = MsgDefInst.Message.NumericTypes.CreationTime
      msgDef.modTime = MsgDefInst.Message.NumericTypes.ModTime
      msgDef.description = MsgDefInst.Message.Description
      msgDef.author = MsgDefInst.Message.Author
      msgDef.mdElemStructVer = MsgDefInst.Message.NumericTypes.MdElemStructVer
      msgDef.cType.partitionKey = MsgDefInst.Message.PartitionKey.toArray
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


  def parseContainerDef(contDefJson: String, formatType: String): ContainerDef = {
    try {
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(contDefJson)

      logger.debug("Parsed the json : " + contDefJson)

      val ContDefInst = json.extract[ContainerDefin]
      val attrList = ContDefInst.Container.Attributes
      var attrList1 = List[(String, String, String, String, Boolean, String)]()
      for (attr <- attrList) {
        attrList1 ::=(attr.NameSpace, attr.Name, attr.Type.TypeNameSpace, attr.Type.TypeName, false, attr.CollectionType.get)
      }

      var primaryKeys = List[(String, List[String])]()
      val pr = ContDefInst.Container.PrimaryKeys
      if (pr.isDefined)
        primaryKeys ::=(pr.get.constraintName, pr.get.key)
      var foreignKeys = List[(String, List[String], String, List[String])]()
      val fr = ContDefInst.Container.ForeignKeys
      if (fr.isDefined)
        foreignKeys ::=(fr.get.constraintName, fr.get.key, fr.get.forignContainerName, fr.get.forignKey)


      val msgDef = MdMgr.GetMdMgr.MakeFixedContainer(
        ContDefInst.Container.NameSpace,
        ContDefInst.Container.Name,
        ContDefInst.Container.PhysicalName,
        attrList1,
        ContDefInst.Container.NumericTypes.Version,
        ContDefInst.Container.JarName,
        ContDefInst.Container.DependencyJars.toArray,
        primaryKeys,
        foreignKeys,
        ContDefInst.Container.PartitionKey.toArray,
        ContDefInst.Container.Recompile,
        ContDefInst.Container.Persist
      )

      msgDef.tranId = ContDefInst.Container.NumericTypes.TransId
      msgDef.origDef = ContDefInst.Container.OrigDef
      msgDef.ObjectDefinition(ContDefInst.Container.ObjectDefinition)
      val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(ContDefInst.Container.ObjectFormat)
      msgDef.ObjectFormat(objFmt)
      msgDef.uniqueId = ContDefInst.Container.NumericTypes.UniqID
      msgDef.creationTime = ContDefInst.Container.NumericTypes.CreationTime
      msgDef.modTime = ContDefInst.Container.NumericTypes.ModTime
      msgDef.description = ContDefInst.Container.Description
      msgDef.author = ContDefInst.Container.Author
      msgDef.mdElemStructVer = ContDefInst.Container.NumericTypes.MdElemStructVer
      msgDef.cType.partitionKey = ContDefInst.Container.PartitionKey.toArray
      msgDef.active = ContDefInst.Container.IsActive
      msgDef.deleted = ContDefInst.Container.IsDeleted
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

  private def getEmptyIfNull(jarName: String): String = {
    if (jarName != null) jarName else ""
  }

}

case class Attr(NameSpace: String, Name: String, Version: Long, CollectionType: Option[String], Type: TypeDef)

case class MessageInfo(NameSpace: String,
                       Name: String,
                       JarName: String,
                       PhysicalName: String,
                       DependencyJars: List[String],
                       Attributes: List[Attr],
                       OrigDef: String,
                       ObjectDefinition: String,
                       ObjectFormat: String,
                       Description: String,
                       Author: String,
                       PartitionKey: List[String],
                       Persist: Boolean,
                       IsActive: Boolean,
                       IsDeleted: Boolean,
                       Recompile: Boolean,
                       PrimaryKeys: Option[PrimaryKeys],
                       ForeignKeys: Option[ForeignKeys],
                       NumericTypes: NumericTypes
                      )

case class PrimaryKeys(constraintName: String, key: List[String])

case class ForeignKeys(constraintName: String, key: List[String], forignContainerName: String, forignKey: List[String])

case class MessageDefin(Message: MessageInfo)

case class ContainerDefin(Container: MessageInfo)

case class ModelDefinition(Model: ModelInfo)

case class ModelInfo(Name: String,
                     PhysicalName: String,
                     JarName: String,
                     NameSpace: String,
                     ObjectFormat: String,
                     BooleanTypes: BooleanTypes,
                     MsgConsumed: String,
                     JpmmlStr: String,
                     ModelType: String,
                     DependencyJars: List[String],
                     InputAttributes: List[Attr],
                     OutputAttributes: List[Attr],
                     ModelRep: String,
                     OrigDef: String,
                     ObjectDefinition: String,
                     NumericTypes: NumericTypes,
                     Description: String,
                     Author: String
                    )

case class NumericTypes(Version: Long, TransId: Long, UniqID: Long, CreationTime: Long, ModTime: Long, MdElemStructVer: Int)

case class BooleanTypes(IsReusable: Boolean,
                        IsActive: Boolean,
                        SupportsInstanceSerialization: Boolean,
                        IsDeleted: Boolean,
                        Recompile: Boolean)
