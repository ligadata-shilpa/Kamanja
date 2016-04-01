package com.ligadata.MetadataAPI.utility
import java.io.{File, ByteArrayInputStream}
import org.scalatest._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.FunctionService
/**
 * Created by dhavalkolapkar on 3/21/16.
 */

import org.scalatest._
import Matchers._

class FunctionServiceTests extends FlatSpec with Matchers with MetadataBeforeAndAfterEach {

  "add function" should "produce error when invalid path to msg def is provided" in {
    val fnDef = getClass.getResource("/Metadata/function/SampleFunctions_2.10.json").getPath + " Invalid"
    val result = FunctionService.addFunction(fnDef)
    result should include regex ("File does not exist")
  }

  it should "add a valid function successfully" in {
    FunctionService.addFunction(getClass.getResource("/Metadata/function/SampleFunctions_2.10.json").getPath) should include regex ("Function Added Successfully")
  }

  it should "add the function selected by the user" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    FunctionService.addFunction("") should include regex ("Function Added Successfully")
  }

 "delete function" should " delete the function in the metadata" in {
    FunctionService.addFunction(getClass.getResource("/Metadata/function/SampleFunctions_2.10.json").getPath)
   FunctionService.removeFunction("pmml.notequal.000000000000000001") should include regex "Deleted Function Successfully"
  }

 it should " delete the function in the metadata based on the user input" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    FunctionService.addFunction(getClass.getResource("/Metadata/function/SampleFunctions_2.10.json").getPath)
    FunctionService.removeFunction("") should include regex "Deleted Function Successfully"
  }

  /*
  Github issue #1005 raised.


   it should "give alert if function to be deleted not present in the metadata" in {
     val result=FunctionService.removeFunction("pmml.max.invalid")
     result should include regex "Sorry, No functions available, in the Metadata, to delete!"
   }*/

 "get function" should " give error for invalid input" in {
   FunctionService.getFunction("pmml.max.123") should include regex "Failed to get function"
 }

  it should "get the function from the metadata as per the user input " in {

  }

 "load function from a file " should " process a valid function in a valid file" in {
   val fnDef = getClass.getResource("/Metadata/function/SampleFunctions_2.10.json").getPath
   val result=FunctionService.loadFunctionsFromAFile(fnDef)
   result should include regex ("Function Added Successfully")
 }

  it should " give error for an invalid file" in {
      FunctionService.loadFunctionsFromAFile(getClass.getResource("/Metadata/function/SampleFunctions_2.10.json").getPath + "invalid") should include regex ("does not exist")
  }

  "is valid dir" should " validate if the directory is present" in {
    val msgDef = getClass.getResource("/Metadata/message").getPath
    val result=FunctionService.IsValidDir(msgDef)
    assert(result==true)
  }

  it should " invalidate a wrong directory path" in {
    val msgDef = getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath+"Invalid"
    val result=FunctionService.IsValidDir(msgDef)
    assert(result==false)
  }

  "dumpAllFunctionsAsJson" should "display all the functions" in {
    FunctionService.dumpAllFunctionsAsJson should include regex "Successfully fetched"
  }

  "updateFunction" should "update a valid function successfully" in {
    FunctionService.updateFunction(getClass.getResource("/Metadata/function/SampleFunctions_2.10.json").getPath) should include regex "Function Updated Successfully"
  }
  it should "update the function selected by the user" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    FunctionService.updateFunction("") should include regex "Function Updated Successfully"
  }

  "user input " should "get the user input and perform requested operation on it" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    val messages: Array[File] = new java.io.File(getClass.getResource("/Metadata/inputMessage").getPath).listFiles.filter(_.getName.endsWith(".json"))
    FunctionService.getUserInputFromMainMenu(messages).size > 0
  }
}

