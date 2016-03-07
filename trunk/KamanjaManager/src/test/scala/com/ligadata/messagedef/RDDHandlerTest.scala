package com.ligadata.messagedef

import org.scalatest.FunSuite

/**
  * Created by Haitham on 3/7/2016.
  */
class RDDHandlerTest extends FunSuite {

  test("testJavaMessageFactory") {
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

  // not checking if messageName isNull!
  //test fail
  test("testHandleRDD_NullInput") {
    val rDDHandler = new RDDHandler()
    val result = rDDHandler.HandleRDD(null)
    //    println(result)
    assert(result == null)

  }

}
