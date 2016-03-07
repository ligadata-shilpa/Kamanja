package com.ligadata.messagedef

import com.ligadata.kamanja.metadata.MdMgr
import org.scalatest.FunSuite


class BaseTypesHandlerTest extends FunSuite {

  //error
  test("testSerializeMsgContainer") {
    val baseTypesHandler = new BaseTypesHandler()
    // TODO initialize typ
    val typ = MdMgr.GetMdMgr.Type("nameSpace1", "name1", -1, true)
    //    println(typ.get)
    val element = new Element("nameSpace", "var1", "type1", "colType1", "elemType", "fieldTypeVer", true, "NativeNAme")
    val result = baseTypesHandler.serializeMsgContainer(typ, "true", element, 123)
    println(result)
    assert(result != null)
  }

  //error
  test("testHandleBaseTypes") {
    //        val baseTypesHandler = new BaseTypesHandler()
    //        val res_1 = List[(String, String)]
    //        val res_2 = List[(String, String, String, String, Boolean, String)]
    //        val res_3 = Set[String]
    //        val res_4 = ArrayBuffer[String]
    //        val res_5 = ArrayBuffer[String]
    //        val res_6 = Array(String)
    //        val message = new Message("message", "nameSpace", "name", "physicalname", "version", "description", "true", true, null, true, null, null, "pkg", null, "Ctype", "ccollection", null, null, null, 2, null)
    //        (res_1, res_2, res_3, res_4, res_5, res_6) = baseTypesHandler.handleBaseTypes(Set("key1", "key2"), "true", null, null, "msgVersion1", Map(("child1", 1), ("child2", 2)), null, false, null, true, message)
    //        println(res_1)
    //        println(res_2)
    //        println(res_3)
    //        println(res_4)
    //        println(res_5)
    //        println(res_6)
    //
    //        assert(res_1 != null)

  }

  //error
  test("testDeSerializeMsgContainer") {
    //    val baseTypesHandler = new BaseTypesHandler()
    // TODO initialize typ
    //    val typ = MdMgr.GetMdMgr.Type("nameSpace1", "name1", -1, true)
    //    val element = new Element("nameSpace", "var1", "type1", "colType1", "elemType", "fieldTypeVer", true, "NativeNAme")
    //    val result = baseTypesHandler.deSerializeMsgContainer(typ, "true", element, 123)

  }


  //error
  test("testPrevObjDeserializeMsgContainer") {
    //    val baseTypesHandler = new BaseTypesHandler()
    // TODO initialize typ
    //    val typ = MdMgr.GetMdMgr.Type("nameSpace1", "name1", -1, true)
    //    val element = new Element("nameSpace", "var1", "type1", "colType1", "elemType", "fieldTypeVer", true, "NativeNAme")
    //    val result = baseTypesHandler.prevObjDeserializeMsgContainer(typ, "true", element, Map(("child1", 1), ("child2", 2)), 1, null)

  }


}