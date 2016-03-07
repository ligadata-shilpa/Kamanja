package com.ligadata.messagedef

import com.ligadata.kamanja.metadata.MdMgr
import org.scalatest.FunSuite

class MessageFldsExtractorTest extends FunSuite {


  //fail
  test("testGetRecompiledMsgContainerVersion") {
    val messageFldsExtractor = new MessageFldsExtractor()
    val mdMgr = new MdMgr()
    // TODO initialize mdMgr
    val result = messageFldsExtractor.getRecompiledMsgContainerVersion("message", "nameSpace1", "name1", mdMgr)
    println(result)
    assert(result != null)
  }


  // fail
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
