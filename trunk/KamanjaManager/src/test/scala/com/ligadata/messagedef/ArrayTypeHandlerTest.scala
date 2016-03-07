package com.ligadata.messagedef

import com.ligadata.kamanja.metadata.MdMgr
import org.scalatest.FunSuite

/**
  * Created by Haitham on 3/1/2016.
  */
class ArrayTypeHandlerTest extends FunSuite {

  test("testSerializeMsgContainer") {
    val arrayTypeHandler = new ArrayTypeHandler
    // TODO initialize typ
    val typ = MdMgr.GetMdMgr.Type("nameSpace1", "name1", -1, true)
    val temp = arrayTypeHandler.serializeMsgContainer(typ, "true", new Element("nameSpace", "var1", "type1", "colType1", "elemType", "fieldTypeVer", true, "NativeNAme"))
    println(temp)
    assert(temp != null)
  }

  test("testGetSerDeserPrimitives") {
    val arrayTypeHandler = new ArrayTypeHandler
    val temp = arrayTypeHandler.getSerDeserPrimitives("type1", "type.Full.Name1", "fieldName", "implName", Map(("child1", 1), ("child2", 2)), true, true, "true")
    println(temp)
    assert(temp != null)
  }

  //error
  test("testHandleArrayType") {
    val arrayTypeHandler = new ArrayTypeHandler
    val message = new Message("message", "nameSpace", "name", "physicalname", "version", "description", "true", true, null, true, null, null, "pkg", null, "Ctype", "ccollection", null, null, null, 2, null)
    // TODO initialize typ
    val typ = MdMgr.GetMdMgr.Type("nameSpace1", "name1", -1, true)
    println(typ)
    val (arr_1, arr_2, arr_3, arr_4) = arrayTypeHandler.handleArrayType(null, typ, null, message, null, null, true)
    //    println(arr_1)
    //    assert(null != arr_1 && null != arr_2 && null != arr_3 && null != arr_4)

    assert(arr_1 != null && arr_2 != null && arr_3 != null && arr_4 != null)
  }

  //error
  test("testDeSerializeMsgContainer") {
    val arrayTypeHandler = new ArrayTypeHandler
    // TODO initialize typ
    val typ = MdMgr.GetMdMgr.Type("nameSpace1", "name1", -1, true)
    val temp = arrayTypeHandler.deSerializeMsgContainer(typ, "true", new Element("nameSpace", "var1", "type1", "colType1", "elemType", "fieldTypeVer", true, "NativeNAme"), false)
    assert(temp != null)
  }

  //error
  test("testPrevObjDeserializeMsgContainer") {
    val arrayTypeHandler = new ArrayTypeHandler
    val message = new Message("message", "nameSpace", "name", "physicalname", "version", "description", "true", true, null, true, null, null, "pkg", null, "Ctype", "ccollection", null, null, null, 2, null)
    // TODO initialize typ
    val typ = MdMgr.GetMdMgr.Type("nameSpace1", "name1", -1, true)
    println(typ)
    val temp = arrayTypeHandler.prevObjDeserializeMsgContainer(typ, "true", new Element("nameSpace", "var1", "type1", "colType1", "elemType", "fieldTypeVer", true, "NativeNAme"), null, true)
    assert(temp != null)
  }


}
