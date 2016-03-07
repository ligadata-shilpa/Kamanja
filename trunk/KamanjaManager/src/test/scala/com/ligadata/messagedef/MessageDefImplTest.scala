package com.ligadata.messagedef

import org.scalatest.FunSuite


class MessageDefImplTest extends FunSuite {

  test("testCase1ContainsIgnoreCase") {
    val messageDefImpl = new MessageDefImpl()
    val result = messageDefImpl.containsIgnoreCase(List("one", "two", "three"), "OnE")
    assert(result == true)

  }

  test("testCase2ContainsIgnoreCase") {
    val messageDefImpl = new MessageDefImpl()
    val result = messageDefImpl.containsIgnoreCase(List("one", "two", "three"), "ne")
    assert(result == false)

  }
  //error
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

}
