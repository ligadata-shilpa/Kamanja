/**
  * Created by Ahmed-Work on 3/17/2016.
  */


import java.util.Date

import com.ligadata.kamanja.metadata.{ObjFormatType, _}
import org.scalatest.FlatSpec
import com.ligadata.MetadataAPI.MetadataAPISerialization
import com.ligadata.kamanja.metadataload.MetadataLoad

class MetadataAPISerializationTest extends FlatSpec {


  "Serialize" should "return serialized modelDefJson" in {
    //input
    val modelDef = getModlDef
    //expected
    val expected: String =
      """{"Model":{"Name":"modelname","PhysicalName":"/opt/kamanja/model/modelname.json","JarName":"JarName","NameSpace":"com.ligadata.modeldef","ModelType":"SCALA","DependencyJars":["Jar1","Jar2"],"InputAttributes":[],"OutputAttributes":[],"ModelRep":"JAR","OrigDef":"OrigDef","MsgConsumed":"","JpmmlStr":"","ObjectDefinition":"ObjectDefinition","ObjectFormat":"Unknown","Description":"Description","Author":"Author","NumericTypes":{"Version":123456789,"TransId":123123123123,"UniqId":987654321,"CreationTime":2222222222,"ModTime":33333333333,"MdElemStructVer":1},"BooleanTypes":{"IsActive":true,"IsReusable":false,"IsDeleted":false,"Recompile":false,"SupportsInstanceSerialization":false}}}"""

    //actual
    val serializedJson1 = MetadataAPISerialization.serializeObjectToJson(modelDef)

    val actual = serializedJson1._2

    assert(expected === actual)
  }

  "It" should "return serialized messageDefJson" in {
    //input
    var mssgDef = getMsgDef
    //expected
    val expected: String =
      """{"Message":{"Name":"msgname","PhysicalName":"/opt/kamanja/message/message.json","JarName":"JarName","NameSpace":"com.ligadata.messagedef","DependencyJars":["Jar1","Jar2"],"OrigDef":"OrigDef","ObjectDefinition":"ObjectDefinition","ObjectFormat":"Unknown","CreationTime":2222222222,"Author":"Author","PartitionKey":["key1","key2"],"Persist":true,"IsActive":true,"IsDeleted":false,"Recompile":false,"Description":"Description","NumericTypes":{"Version":123456789,"TransId":123123123123,"UniqId":987654321,"CreationTime":2222222222,"ModTime":33333333333,"MdElemStructVer":1},"PrimaryKeys":[{"constraintName":"prim","key":["key2","key1"]}],"ForeignKeys":[{"constraintName":"forign","key":["key2","key1"],"forignContainerName":"forr","forignKey":["key2","key1"]}]}}"""

    val serializedJson1 = MetadataAPISerialization.serializeObjectToJson(mssgDef)

    //actual
    val actual = serializedJson1._2

    assert(expected === actual)
  }

  "It" should "return serialized containerDefJson" in {
    //input
    var conDef = getContainerDef
    //expected
    val expected: String =
      """{"Container":{"Name":"msgname","PhysicalName":"/opt/kamanja/Container/Container.json","JarName":"JarName","NameSpace":"com.ligadata.containerdef","DependencyJars":["Jar1","Jar2"],"OrigDef":"OrigDef","ObjectDefinition":"ObjectDefinition","ObjectFormat":"Unknown","CreationTime":2222222222,"Author":"Author","PartitionKey":["key1","key2"],"Persist":true,"IsActive":true,"IsDeleted":false,"Recompile":false,"Description":"Description","NumericTypes":{"Version":123456789,"TransId":123123123123,"UniqId":987654321,"CreationTime":2222222222,"ModTime":33333333333,"MdElemStructVer":1},"PrimaryKeys":[{"constraintName":"prim","key":["key2","key1"]}],"ForeignKeys":[{"constraintName":"forign","key":["key2","key1"],"forignContainerName":"forr","forignKey":["key2","key1"]}]}}"""

    val serializedJson1 = MetadataAPISerialization.serializeObjectToJson(conDef)

    //actual
    val actual = serializedJson1._2

    assert(expected === actual)
  }

  "It" should "return serialized functionDefJson" in {
    //input
    var funDef = getFunctionDef
    //expected
    val expected: String =
      """{"Function":{"Name":"msgname","PhysicalName":"/opt/kamanja/Container/Container.json","JarName":"JarName","NameSpace":"com.ligadata.containerdef","DependencyJars":["Jar1","basetypes_2.10-0.1.0.jar","metadata_2.10-1.0.jar","Jar2"],"OrigDef":"OrigDef","ObjectDefinition":"ObjectDefinition","ObjectFormat":"Unknown","Author":"Author","Arguments":[{"ArgName":"type2","ArgTypeNameSpace":"system","ArgTypeName":"int"},{"ArgName":"type1","ArgTypeNameSpace":"system","ArgTypeName":"int"}],"Features":["CLASSUPDATE","HAS_INDEFINITE_ARITY"],"ReturnTypeNameSpace":"system","ReturnTypeName":"string","ClassName":"className","Recompile":false,"Description":"Description","NumericTypes":{"Version":1000000,"TransId":123123123123,"UniqId":987654321,"CreationTime":2222222222,"ModTime":33333333333,"MdElemStructVer":1},"IsActive":true,"IsDeleted":false}}"""

    //actual
    val serializedJson1 = MetadataAPISerialization.serializeObjectToJson(funDef)

    val actual = serializedJson1._2

    assert(expected === actual)
  }

  "It" should "return serialized MapTypeDefJson" in {
    //input
    var mapTypeDef = getMapTypeDef
    //expected
    val expected: String =
      """{"MapType":{"Name":"maptypename","NameSpace":"com.ligadata.maptypedef","PhysicalName":"/opt/kamanja/type/MapType.json","TypeTypeName":"tContainer","JarName":"MapTypeJarName","ObjectFormat":"Unknown","DependencyJars":["Jar1","Jar2"],"Implementation":"implementationName","KeyTypeNameSpace":"system","KeyTypeName":"String","ValueTypeNameSpace":"system","ValueTypeName":"String","ObjectDefinition":"ObjectDefinition","OrigDef":"OrigDef","Author":"Author","Recompile":false,"Persist":false,"Description":"Description","NumericTypes":{"Version":1000000,"TransId":123123123123,"UniqId":987654321,"CreationTime":2222222222,"ModTime":33333333333,"MdElemStructVer":1},"IsActive":true,"IsFixed":false,"IsDeleted":false}}"""

    val serializedJson1 = MetadataAPISerialization.serializeObjectToJson(mapTypeDef)

    //actual
    val actual = serializedJson1._2

    assert(expected === actual)
  }

  "It" should "return serialized ArrayTypeDefJson" in {
    //input
    var arrayType = getArrayTypeDef
    //expected
    val expected: String =
      """{"ArrayType":{"Name":"maptypename","NameSpace":"com.ligadata.arraytypedef","PhysicalName":"/opt/kamanja/type/ArrayTyp.json","TypeTypeName":"tScalar","TypeName":"Int","TypeNameSpace":"system","NumberOfDimensions":2,"JarName":"ArrayTypeJarName","ObjectFormat":"Unknown","DependencyJars":["Jar1","Jar2"],"Implementation":"implementationName","ObjectDefinition":"ObjectDefinition","OrigDef":"OrigDef","Author":"Author","Recompile":false,"Persist":false,"Description":"Description","NumericTypes":{"Version":1000000,"TransId":123123123123,"UniqId":987654321,"CreationTime":2222222222,"ModTime":33333333333,"MdElemStructVer":1},"IsActive":true,"IsFixed":false,"IsDeleted":false}}"""

    val serializedJson1 = MetadataAPISerialization.serializeObjectToJson(arrayType)

    //actual
    val actual = serializedJson1._2

    assert(expected === actual)
  }

  "It" should "return serialized JarDefJson" in {
    //input
    var jar = getJarDef
    //expected
    val expected: String =
      """{"Jar":{"IsActive":true,"IsDeleted":false,"OrigDef":"OrigDef","ObjectDefinition":"ObjectDefinition","ObjectFormat":"Unknown","NameSpace":"com.ligadata.jardef","Name":"jarname","Author":"Author","PhysicalName":"/opt/kamanja/jar/Jar.jar","JarName":"Jar.jar","DependencyJars":["Jar1","Jar2"],"NumericTypes":{"Version":1000000,"TransId":123123123123,"UniqId":987654321,"CreationTime":2222222222,"ModTime":33333333333,"MdElemStructVer":1},"Description":"Description"}}"""

    val serializedJson1 = MetadataAPISerialization.serializeObjectToJson(jar)

    //actual
    val actual = serializedJson1._2

    assert(expected === actual)
  }

  "It" should "return serialized ConfigDefJson" in {
    //input
    var config = getConfigDef
    //expected
    val expected: String =
      """{"Config":{"IsActive":true,"IsDeleted":false,"OrigDef":"OrigDef","ObjectDefinition":"ObjectDefinition","ObjectFormat":"Unknown","NameSpace":"com.ligadata.ConfigDef","Contents":"Contents","Name":"ConfigName","Author":"Author","PhysicalName":"/opt/kamanja/Config/Config.json","JarName":"Config.jar","DependencyJars":["Jar1","Jar2"],"NumericTypes":{"Version":1000000,"TransId":123123123123,"UniqId":987654321,"CreationTime":2222222222,"ModTime":33333333333,"MdElemStructVer":1},"Description":"Description"}}"""

    val serializedJson1 = MetadataAPISerialization.serializeObjectToJson(config)

    //actual
    val actual = serializedJson1._2

    assert(expected === actual)
  }

  "It" should "return serialized NodeInfoJson" in {
    //input
    val nodeInfo = getNodeInfo
    //expected
    val expected: String =
      """{"Node":{"NodeId":"1","NodePort":2021,"NodeIpAddr":"localhost","JarPaths":["path1","path2"],"Scala_home":"/usr/bin","Java_home":"/usr/bin","Classpath":"/class/path","ClusterId":"1","Power":2,"Roles":["role1","role2"],"Description":"description"}}"""

    val serializedJson1 = MetadataAPISerialization.serializeConfig(nodeInfo)

    //actual
    val actual = serializedJson1._2

    assert(expected === actual)
  }

  "It" should "return serialized ClusterInfoJson" in {
    //input
    val clusterInfo = getClusterInfo
    //expected
    val expected: String =
      """{"Cluster":{"ClusterId":"1","Description":"description","Privileges":"privilges"}}"""

    val serializedJson1 = MetadataAPISerialization.serializeConfig(getClusterInfo)

    //actual
    val actual = serializedJson1._2

    assert(expected === actual)
  }

  "It" should "return serialized ClusterCfgInfoJson" in {
    //input
    val clusterCfgInfo = getClusterCfgInfo
    //expected
    val expected: String =
      """{"ClusterCfg":{"ClusterId":"clusterId","CfgMap":[{"Key":"key2","Value":"value2"},{"Key":"key1","Value":"value1"}],"ModifiedTime":1458952764,"CreatedTime":1459039164,"UsrConfigs":[{"Key":"key2","Value":"value2"},{"Key":"key1","Value":"value1"}]}}"""

    val serializedJson1 = MetadataAPISerialization.serializeConfig(clusterCfgInfo)

    //actual
    val actual = serializedJson1._2

    assert(expected === actual)
  }

  "It" should "return serialized AdapterInfoJson" in {
    //input
    val adapterInfo = getAdapterInfo
    //expected
    val expected: String =
      """{"Adapter":{"Name":"name","TypeString":"typeString","DataFormat":"dataformat","ClassName":"ClassName","JarName":"jarName","DependencyJars":["Jar1","Jar2"],"AdapterSpecificCfg":"AdapterSpecificCfg","InputAdapterToValidate":"InputAdapterToValidate","FailedEventsAdapter":"FailedEventsAdapter","DelimiterString1":"FieldDelimiter","AssociatedMessage":"AssociatedMessage","KeyAndValueDelimiter":"KeyAndValueDelimiter","FieldDelimiter":"FieldDelimiter","ValueDelimiter":"ValueDelimiter"}}"""

    val serializedJson1 = MetadataAPISerialization.serializeConfig(adapterInfo)

    //actual
    val actual = serializedJson1._2

    assert(expected === actual)
  }

  "It" should "return serialized UserPropertiesInfoJson" in {
    //input
    val userPropertiesInfo = getUserPropertiesInfo
    //expected
    val expected: String =
      """{"UserProperties":{"ClusterId":"ClusterId","Props":[{"Key":"key2","Value":"value2"},{"Key":"key1","Value":"value1"}]}}"""

    val serializedJson1 = MetadataAPISerialization.serializeConfig(userPropertiesInfo)

    //actual
    val actual = serializedJson1._2

    assert(expected === actual)
  }

  private def getModlDef: ModelDef = {
    val nameSpace = "com.ligadata.modelDef"
    val name = "modelName"
    val physicalNameString = "/opt/kamanja/model/modelname.json"
    val modelType = "scala"
    val inputVars = Array.empty[(String, String, String, String, Boolean, String)]
    val outputVars = Array.empty[(String, String, String)]
    val version = 123456789L
    val jarName = "JarName"
    val dependencyJar = new Array[String](2)
    dependencyJar(0) = "Jar1"
    dependencyJar(1) = "Jar2"
    val recompile = false
    val supportsInstanceSerialization = false
    val transId = 123123123123L
    val origDef = "OrigDef"
    val objectDefinition = "ObjectDefinition"
    val objectFormat = "ObjectFormat"
    val uniqID = 987654321L
    val creationTime = 2222222222L
    val modTime = 33333333333L
    val description = "Description"
    val author = "Author"
    val mdElemStructVer = 1
    val partitionKey = new Array[String](2)
    partitionKey(0) = "key1"
    partitionKey(1) = "key2"

    val isActive = true
    val isDeleted = false
    val modelDef = MdMgr.GetMdMgr.MakeModelDef(nameSpace, name, physicalNameString, modelType, inputVars.toList, outputVars.toList, version, jarName, dependencyJar, recompile, supportsInstanceSerialization)


    modelDef.ObjectDefinition(objectDefinition)
    val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(objectFormat)
    modelDef.ObjectFormat(objFmt)
    modelDef.tranId = transId
    modelDef.origDef = origDef
    modelDef.uniqueId = uniqID
    modelDef.creationTime = creationTime
    modelDef.modTime = modTime
    modelDef.description = description
    modelDef.author = author
    modelDef.mdElemStructVer = mdElemStructVer
    modelDef.active = isActive
    modelDef.deleted = isDeleted
    modelDef

  }

  private def getMsgDef: MessageDef = {

    val nameSpace = "com.ligadata.MessageDef"
    val name = "msgName"
    val physicalNameString = "/opt/kamanja/message/message.json"

    val version = 123456789L
    val jarName = "JarName"
    val dependencyJar = new Array[String](2)
    dependencyJar(0) = "Jar1"
    dependencyJar(1) = "Jar2"

    val transId = 123123123123L
    val origDef = "OrigDef"
    val objectDefinition = "ObjectDefinition"
    val objectFormat = "ObjectFormat"
    val uniqID = 987654321L
    val creationTime = 2222222222L
    val modTime = 33333333333L
    val description = "Description"
    val author = "Author"
    val mdElemStructVer = 1
    val attrList1 = Array.empty[(String, String, String, String, Boolean, String)]
    val partitionKey = new Array[String](2)
    partitionKey(0) = "key1"
    partitionKey(1) = "key2"

    var key = List[String]()
    key ::= "key1"
    key ::= "key2"
    var primaryKeys = List[(String, List[String])]()
    primaryKeys ::=("prim", key)
    var foreignKeys = List[(String, List[String], String, List[String])]()
    foreignKeys ::=("forign", key, "forr", key)

    val persist = true
    val isActive = true
    val isDeleted = false
    val recompile = false
    val msgDef = MdMgr.GetMdMgr.MakeFixedMsg(
      nameSpace,
      name,
      physicalNameString,
      attrList1.toList,
      version,
      jarName,
      dependencyJar,
      primaryKeys,
      foreignKeys,
      partitionKey,
      recompile,
      persist
    )

    msgDef.tranId = transId
    msgDef.origDef = origDef
    msgDef.ObjectDefinition(objectDefinition)
    val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(objectFormat)
    msgDef.ObjectFormat(objFmt)
    msgDef.uniqueId = uniqID
    msgDef.creationTime = creationTime
    msgDef.modTime = modTime
    msgDef.description = description
    msgDef.author = author
    msgDef.mdElemStructVer = mdElemStructVer
    msgDef.cType.partitionKey = partitionKey
    msgDef.active = isActive
    msgDef.deleted = isDeleted
    msgDef
  }

  private def getContainerDef: ContainerDef = {

    val nameSpace = "com.ligadata.ContainerDef"
    val name = "msgName"
    val physicalNameString = "/opt/kamanja/Container/Container.json"

    val version = 123456789L
    val jarName = "JarName"
    val dependencyJar = new Array[String](2)
    dependencyJar(0) = "Jar1"
    dependencyJar(1) = "Jar2"

    val transId = 123123123123L
    val origDef = "OrigDef"
    val objectDefinition = "ObjectDefinition"
    val objectFormat = "ObjectFormat"
    val uniqID = 987654321L
    val creationTime = 2222222222L
    val modTime = 33333333333L
    val description = "Description"
    val author = "Author"
    val mdElemStructVer = 1
    val attrList1 = Array.empty[(String, String, String, String, Boolean, String)]
    val partitionKey = new Array[String](2)
    partitionKey(0) = "key1"
    partitionKey(1) = "key2"

    var key = List[String]()
    key ::= "key1"
    key ::= "key2"
    var primaryKeys = List[(String, List[String])]()
    primaryKeys ::=("prim", key)
    var foreignKeys = List[(String, List[String], String, List[String])]()
    foreignKeys ::=("forign", key, "forr", key)

    val persist = true
    val isActive = true
    val isDeleted = false
    val recompile = false
    val contDef = MdMgr.GetMdMgr.MakeFixedContainer(
      nameSpace,
      name,
      physicalNameString,
      attrList1.toList,
      version,
      jarName,
      dependencyJar,
      primaryKeys,
      foreignKeys,
      partitionKey,
      recompile,
      persist
    )

    contDef.tranId = transId
    contDef.origDef = origDef
    contDef.ObjectDefinition(objectDefinition)
    val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(objectFormat)
    contDef.ObjectFormat(objFmt)
    contDef.uniqueId = uniqID
    contDef.creationTime = creationTime
    contDef.modTime = modTime
    contDef.description = description
    contDef.author = author
    contDef.mdElemStructVer = mdElemStructVer
    contDef.cType.partitionKey = partitionKey
    contDef.active = isActive
    contDef.deleted = isDeleted
    contDef
  }

  private def getFunctionDef: FunctionDef = {

    val nameSpace = "com.ligadata.ContainerDef"
    val name = "msgName"
    val physicalNameString = "/opt/kamanja/Container/Container.json"

    val version = 1000000
    val jarName = "JarName"
    val dependencyJar = new Array[String](2)
    dependencyJar(0) = "Jar1"
    dependencyJar(1) = "Jar2"

    val transId = 123123123123L
    val origDef = "OrigDef"
    val objectDefinition = "ObjectDefinition"
    val objectFormat = "ObjectFormat"
    val uniqID = 987654321L
    val creationTime = 2222222222L
    val modTime = 33333333333L
    val description = "Description"
    val author = "Author"
    val mdElemStructVer = 1
val className= "className"
    val persist = true
    val isActive = true
    val isDeleted = false
    val recompile = false


    var argList = List[(String, String, String)]()
    argList ::=("type1", "system", "int")
    argList ::=("type2", "system", "int")


    //  val ITERABLE, CLASSUPDATE, HAS_INDEFINITE_ARITY = Value

    var featureSet: scala.collection.mutable.Set[FcnMacroAttr.Feature] = scala.collection.mutable.Set[FcnMacroAttr.Feature]()

    featureSet += FcnMacroAttr.fromString("CLASSUPDATE")
    featureSet += FcnMacroAttr.fromString("HAS_INDEFINITE_ARITY")

    val mdmgr: MdMgr = new MdMgr
    var lodder: MetadataLoad = new MetadataLoad(mdmgr, "", "", "", "")
    lodder.initialize

    val functionDef = mdmgr.MakeFunc(nameSpace,
      name,
      physicalNameString,
      ("system", "string"),
      argList,
      featureSet,
      version,
      jarName,
      dependencyJar)

    functionDef.tranId = transId
    functionDef.origDef = origDef
    functionDef.ObjectDefinition(objectDefinition)
    val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(objectFormat)
    functionDef.ObjectFormat(objFmt)
    functionDef.uniqueId = uniqID
    functionDef.creationTime = creationTime
    functionDef.modTime = modTime
    functionDef.description = description
    functionDef.author = author
    functionDef.mdElemStructVer = mdElemStructVer
    functionDef.className=className
    functionDef.active = isActive
    functionDef.deleted = isDeleted
    functionDef
  }

  private def getMapTypeDef: MapTypeDef = {

    val nameSpace = "com.ligadata.MapTypeDef"
    val name = "MapTypeName"
    val physicalNameString = "/opt/kamanja/type/MapType.json"

    val version = 1000000
    val jarName = "MapTypeJarName"
    val dependencyJar = new Array[String](2)
    dependencyJar(0) = "Jar1"
    dependencyJar(1) = "Jar2"

    val transId = 123123123123L
    val origDef = "OrigDef"
    val objectDefinition = "ObjectDefinition"
    val objectFormat = "ObjectFormat"
    val uniqID = 987654321L
    val creationTime = 2222222222L
    val modTime = 33333333333L
    val description = "Description"
    val author = "Author"
    val mdElemStructVer = 1
    val implementationName = "implementationName"
    val persist = true
    val isActive = true
    val isDeleted = false
    val recompile = false




    val mdmgr: MdMgr = new MdMgr
    var lodder: MetadataLoad = new MetadataLoad(mdmgr, "", "", "", "")
    lodder.initialize

    val key = ("system", "string")
    val value = ("system", "string")

    val mapType = mdmgr.MakeMap(nameSpace, name, key, value, version, recompile, persist)

    mapType.tranId = transId
    mapType.origDef = origDef
    mapType.ObjectDefinition(objectDefinition)
    val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(objectFormat)
    mapType.ObjectFormat(objFmt)
    mapType.uniqueId = uniqID
    mapType.creationTime = creationTime
    mapType.modTime = modTime
    mapType.description = description
    mapType.author = author
    mapType.mdElemStructVer = mdElemStructVer
    mapType.implementationName("implementationName")
    mapType.active = isActive
    mapType.deleted = isDeleted
    mapType.dependencyJarNames = dependencyJar
    mapType.jarName = jarName
    mapType.physicalName = physicalNameString

    mapType
  }

  private def getJarDef: JarDef = {

    val nameSpace = "com.ligadata.JarDef"
    val name = "JarName"
    val physicalNameString = "/opt/kamanja/jar/Jar.jar"

    val version = 1000000
    val jarName = "Jar.jar"
    val dependencyJar = new Array[String](2)
    dependencyJar(0) = "Jar1"
    dependencyJar(1) = "Jar2"

    val transId = 123123123123L
    val origDef = "OrigDef"
    val objectDefinition = "ObjectDefinition"
    val objectFormat = "ObjectFormat"
    val uniqID = 987654321L
    val creationTime = 2222222222L
    val modTime = 33333333333L
    val description = "Description"
    val author = "Author"
    val mdElemStructVer = 1
    val isActive = true
    val isDeleted = false
    val recompile = false



    val mdmgr: MdMgr = new MdMgr
    var lodder: MetadataLoad = new MetadataLoad(mdmgr, "", "", "", "")
    lodder.initialize

    val key = ("system", "string")
    val value = ("system", "string")

    val jar = mdmgr.MakeJarDef(
      nameSpace,
      name,
      version.toString
    )

    jar.tranId = transId
    jar.origDef = origDef
    jar.ObjectDefinition(objectDefinition)
    val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(objectFormat)
    jar.ObjectFormat(objFmt)
    jar.uniqueId = uniqID
    jar.creationTime = creationTime
    jar.modTime = modTime
    jar.description = description
    jar.author = author
    jar.mdElemStructVer = mdElemStructVer
    jar.active = isActive
    jar.deleted = isDeleted
    jar.dependencyJarNames = dependencyJar
    jar.jarName = jarName
    jar.physicalName = physicalNameString

    jar
  }

  private def getConfigDef: ConfigDef = {

    val nameSpace = "com.ligadata.ConfigDef"
    val name = "ConfigName"
    val physicalNameString = "/opt/kamanja/Config/Config.json"

    val version = 1000000
    val jarName = "Config.jar"
    val dependencyJar = new Array[String](2)
    dependencyJar(0) = "Jar1"
    dependencyJar(1) = "Jar2"

    val transId = 123123123123L
    val origDef = "OrigDef"
    val objectDefinition = "ObjectDefinition"
    val objectFormat = "ObjectFormat"
    val uniqID = 987654321L
    val creationTime = 2222222222L
    val modTime = 33333333333L
    val description = "Description"
    val author = "Author"
    val mdElemStructVer = 1
    val isActive = true
    val isDeleted = false
    val contents = "Contents"

    val config = new ConfigDef

    config.nameSpace = nameSpace;
    config.name = name;
    config.ver = version;
    config.tranId = transId
    config.origDef = origDef
    config.ObjectDefinition(objectDefinition)
    val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(objectFormat)
    config.ObjectFormat(objFmt)
    config.uniqueId = uniqID
    config.creationTime = creationTime
    config.modTime = modTime
    config.description = description
    config.author = author
    config.mdElemStructVer = mdElemStructVer
    config.active = isActive
    config.deleted = isDeleted
    config.dependencyJarNames = dependencyJar
    config.jarName = jarName
    config.physicalName = physicalNameString
    config.contents = contents
    config
  }

  private def getArrayTypeDef: ArrayTypeDef = {

    val nameSpace = "com.ligadata.ArrayTypeDef"
    val name = "MapTypeName"
    val physicalNameString = "/opt/kamanja/type/ArrayTyp.json"

    val version = 1000000
    val jarName = "ArrayTypeJarName"
    val dependencyJar = new Array[String](2)
    dependencyJar(0) = "Jar1"
    dependencyJar(1) = "Jar2"

    val transId = 123123123123L
    val origDef = "OrigDef"
    val objectDefinition = "ObjectDefinition"
    val objectFormat = "ObjectFormat"
    val uniqID = 987654321L
    val creationTime = 2222222222L
    val modTime = 33333333333L
    val description = "Description"
    val author = "Author"
    val mdElemStructVer = 1
    val implementationName = "implementationName"
    val numberOfDimention = 2
    val persist = true
    val isActive = true
    val isDeleted = false
    val recompile = false



    val mdmgr: MdMgr = new MdMgr
    var lodder: MetadataLoad = new MetadataLoad(mdmgr, "", "", "", "")
    lodder.initialize

    val key = ("system", "string")
    val value = ("system", "string")

    val arrayType = mdmgr.MakeArray(
      nameSpace,
      name,
      "System",
      "int",
      numberOfDimention,
      version,
      recompile,
      persist)

    arrayType.tranId = transId
    arrayType.origDef = origDef
    arrayType.ObjectDefinition(objectDefinition)
    val objFmt: ObjFormatType.FormatType = ObjFormatType.fromString(objectFormat)
    arrayType.ObjectFormat(objFmt)
    arrayType.uniqueId = uniqID
    arrayType.creationTime = creationTime
    arrayType.modTime = modTime
    arrayType.description = description
    arrayType.author = author
    arrayType.mdElemStructVer = mdElemStructVer
    arrayType.implementationName("implementationName")
    arrayType.active = isActive
    arrayType.deleted = isDeleted
    arrayType.dependencyJarNames = dependencyJar
    arrayType.jarName = jarName
    arrayType.physicalName = physicalNameString

    arrayType
  }

  private def getNodeInfo: NodeInfo = {

    val nodeId = "1"
    val nodePort = 2021
    val nodeIpAddr = "localhost"
    val jarPaths = new Array[String](2)
    jarPaths(0) = "path1"
    jarPaths(1) = "path2"

    val scala_home = "/usr/bin"
    val java_home = "/usr/bin"
    val classpath = "/class/path"
    val clusterId = "1"
    val power = 2
    val roles = new Array[String](2)
    roles(0) = "role1"
    roles(1) = "role2"
    val description = "description"

    val nodeInfo = MdMgr.GetMdMgr.MakeNode(
      nodeId,
      nodePort,
      nodeIpAddr,
      jarPaths.toList,
      scala_home,
      java_home,
      classpath,
      clusterId,
      power,
      roles,
      description)
    nodeInfo
  }

  private def getClusterInfo: ClusterInfo = {

    val clusterId = "1"
    val description = "description"
    val privilges = "privilges"

    val clusterInfo = MdMgr.GetMdMgr.MakeCluster(
      clusterId,
      description,
      privilges
    )
    clusterInfo
  }

  private def getClusterCfgInfo: ClusterCfgInfo = {

    val clusterId = "clusterId"

    val cfgMap = new scala.collection.mutable.HashMap[String, String]
    cfgMap.put("key1", "value1")
    cfgMap.put("key2", "value2")

    val clusterCfgInfo = MdMgr.GetMdMgr.MakeClusterCfg(
      clusterId,
      cfgMap,
      new Date(1458952764),
      new Date(1459039164)
    )
    val usrConfigs = new scala.collection.mutable.HashMap[String, String]

    usrConfigs.put("key1", "value1")
    usrConfigs.put("key2", "value2")

    clusterCfgInfo.usrConfigs = usrConfigs

    clusterCfgInfo

  }

  private def getAdapterInfo: AdapterInfo = {
    val name = "name"
    val typeString = "typeString"
    val dataFormat = "dataformat"
    val className = "ClassName"
    val jarName = "jarName"
    val dependencyJars = new Array[String](2)
    dependencyJars(0) = "Jar1"
    dependencyJars(1) = "Jar2"

    val adapterSpecificCfg = "AdapterSpecificCfg"
    val inputAdapterToValidate = "InputAdapterToValidate"
    val failedEventsAdapter = "FailedEventsAdapter"
    val associatedMessage = "AssociatedMessage"
    val keyAndValueDelimiter = "KeyAndValueDelimiter"
    val fieldDelimiter = "FieldDelimiter"
    val valueDelimiter = "ValueDelimiter"

    val adapterInfo = MdMgr.GetMdMgr.MakeAdapter(
      name,
      typeString,
      dataFormat,
      className,
      jarName,
      dependencyJars.toList,
      adapterSpecificCfg,
      inputAdapterToValidate,
      keyAndValueDelimiter,
      fieldDelimiter,
      valueDelimiter,
      associatedMessage,
      failedEventsAdapter
    )
    //adapterInfo.DelimiterString1 = adapterInst.Adapter.DelimiterString1
    adapterInfo

  }

  private def getUserPropertiesInfo: UserPropertiesInfo = {
    val clusterId = "ClusterId"

    val props = new scala.collection.mutable.HashMap[String, String]
    props.put("key1", "value1")
    props.put("key2", "value2")

    val upi = new UserPropertiesInfo
    upi.clusterId = clusterId
    upi.props = props
    upi

  }

}
