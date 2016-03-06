package com.ligadata.messagedef

import org.scalatest.FunSuite


class MessageDefImplTest extends FunSuite {

  test("testRddHandler") {
    // method should be edited to throw an exception if msgType is not in ("message","container")
    val rDDHandler = new RDDHandler()
    val result1 = rDDHandler.javaMessageFactory(new Message("message", "nameSpace", "name", "physicalname", "version", "description", "true", true, null, true, null, null, "pkg", null, "Ctype", "ccollection", null, null, null, 2, null))
    val result2 = rDDHandler.javaMessageFactory(new Message("container", "nameSpace", "name", "physicalname", "version", "description", "true", true, null, true, null, null, "pkg", null, "Ctype", "ccollection", null, null, null, 2, null))
    println(result1)
    println(result2)
    assert(result1 != null && result2 != null)
  }

  test("testHandleRDD") {
    // not checking if messageName isNull
    val rDDHandler = new RDDHandler()
    val result = rDDHandler.HandleRDD("messageName")
    println(result)
    assert(result != null)

  }


  test("testHandleRDD_NullInput") {
    // not checking if messageName isNull!
    val rDDHandler = new RDDHandler()
    val result = rDDHandler.HandleRDD(null)
    //    println(result)
    assert(result == null)

  }


  test("testCase1ContainsIgnoreCase") {
    val messageDefImpl = new MessageDefImpl()
    val result = messageDefImpl.containsIgnoreCase(List("one", "two", "three"), "OnE")
    assert(result == true)

  }

  test("testCase2ContainsIgnoreCase") {
    val messageDefImpl = new MessageDefImpl()
    val result = messageDefImpl.containsIgnoreCase(List("one", "two", "three"), "four")
    assert(result == false)

  }


  test("testProcessMsgDef") {
//    val messageDefImpl = new MessageDefImpl()
//    val json = "{\n  \"Message\" : {\n    \"NameSpace\" : \"com.ligadata.kamanja.samples.messages\",\n    \"Name\" : \"msg1\",\n    \"Version\" : \"00.01.00\",\n    \"Description\" : \"Hello World\",\n    \"Fixed\" : \"true\",\n    \"Elements\" : [\n      {\n        \"Field\" : {\n          \"Name\" : \"Id\",\n          \"Type\" : \"System.Int\"\n        }\n      },\n      {\n        \"Field\" : {\n          \"Name\" : \"Name\",\n          \"Type\" : \"System.String\"\n        }\n      },\n      {\n        \"Field\" : {\n          \"Name\" : \"Score\",\n          \"Type\" : \"System.Int\"\n        }\n      }\n    ]\n  }\n}"
//    var classstr_1: String = null
//    var javaFactoryClassstr_1: String = null
//    var nonVerJavaFactoryClassstr_1: String = null
//    var classname: String = null
//    var ver: String = null
//    var nonVerClassStrVal_1: String = null
//    var containerDef: ContainerDef = null
//    ((classstr_1, javaFactoryClassstr_1), containerDef, (nonVerClassStrVal_1, nonVerJavaFactoryClassstr_1)) = messageDefImpl.processMsgDef(json, "JSON", null, false)
//
//    assert(classstr_1 != null)

  }

  // fail
  // should be moved to MessageFldsExtractor
  test("testClassStr") {
//    val messageFldsExtractor = new MessageFldsExtractor()
//    var returnClassStr = new ArrayBuffer[String]
//    var list = List[(String, String)]()
//    var argsList = List[(String, String, String, String, Boolean, String)]()
//    var partitionPos = Array[Int]()
//    var count: Int = 0
//    var primaryPos = Array[Int]()
//    val message = new Message("message", "nameSpace", "name", "physicalname", "version", "description", "true", true, null, true, null, null, "pkg", null, "Ctype", "ccollection", null, null, null, 2, null)
//
//    (returnClassStr.toArray, count, list, argsList, partitionPos, primaryPos) = messageFldsExtractor.classStr(message: Message, new MdMgr(), true)
//
//    assert(returnClassStr.toArray != null && count != null && list != null && argsList != null && partitionPos != null && primaryPos != null)
  }


}
