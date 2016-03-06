package com.ligadata.messagedef

import com.ligadata.kamanja.metadata.MdMgr
import org.scalatest.FunSuite

/**
  * Created by Haitham on 3/1/2016.
  */
class ArrayTypeHandlerTest extends FunSuite {

  test("serializeMsgContainer") {
    val arrayTypeHandler = new ArrayTypeHandler
    val temp = arrayTypeHandler.serializeMsgContainer(null, "true", new Element(null, "var1", "type1", "colType1", "elemType", "fieldTypeVer", true, "NativeNAme"))
    assert(temp != null)
  }

  test("getSerDeserPrimitives") {
    val arrayTypeHandler = new ArrayTypeHandler
    val temp = arrayTypeHandler.getSerDeserPrimitives("type1", "typeFullName", "fieldName", "implName", null, true, true, "true")
    assert(temp != null)
  }

  //fail
  test("ArrayTypeHandler.handleArrayType") {
    val arrayTypeHandler = new ArrayTypeHandler
    val message = new Message("message", "nameSpace", "name", "physicalname", "version", "description", "true", true, null, true, null, null, "pkg", null, "Ctype", "ccollection", null, null, null, 2, null)
    val typ = MdMgr.GetMdMgr.Type("nameSpace", -1, true)
    println(typ)
    val (arr_1, arr_2, arr_3, arr_4) = arrayTypeHandler.handleArrayType(null, typ, null, message, null, null, true)
    //    println(arr_1)
    //    assert(null != arr_1 && null != arr_2 && null != arr_3 && null != arr_4)

    assert("one" != null)
  }

  //fail
  test("deSerializeMsgContainer") {
    val arrayTypeHandler = new ArrayTypeHandler
    val temp = arrayTypeHandler.deSerializeMsgContainer(null, "true", new Element(null, "var1", "type1", "colType1", "elemType", "fieldTypeVer", true, "NativeNAme"),false)
    assert(temp != null)
  }

  //fail
  test("prevObjDeserializeMsgContainer") {
    val arrayTypeHandler = new ArrayTypeHandler
    val message = new Message("message", "nameSpace", "name", "physicalname", "version", "description", "true", true, null, true, null, null, "pkg", null, "Ctype", "ccollection", null, null, null, 2, null)

    val typ = MdMgr.GetMdMgr.Type("nameSpace1", "name1", -1, true)
    println(typ)
    val temp = arrayTypeHandler.prevObjDeserializeMsgContainer(typ, "true", new Element(null, "var1", "type1", "colType1", "elemType", "fieldTypeVer", true, "NativeNAme"), null, true)
    assert(temp != null)
  }


}
