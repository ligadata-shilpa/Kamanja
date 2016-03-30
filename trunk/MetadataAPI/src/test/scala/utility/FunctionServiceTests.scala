package com.ligadata.MetadataAPI.utility.test
import org.scalatest._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.FunctionService

/**
 * Created by dhavalkolapkar on 3/21/16.
 */
class FunctionServiceTests extends FlatSpec with Matchers with MetadataBeforeAndAfterEach {

  "add function" should "produce error when invalid path to msg def is provided" in {
    val fnDef = getClass.getResource("/Metadata/function/SampleFunctions_2.10.json").getPath + " Invalid"
    val result = FunctionService.addFunction(fnDef)
    result should include regex ("File does not exist")
  }
  it should "add a valid function successfully" in {
    val fnDef = getClass.getResource("/Metadata/function/SampleFunctions_2.10.json").getPath
    val result = FunctionService.addFunction(fnDef)
    result should include regex ("Function Added Successfully")
  }

 "delete function" should " delete the function in the metadata" in {
    val fnDef = getClass.getResource("/Metadata/function/SampleFunctions_2.10.json").getPath
    FunctionService.addFunction(fnDef)
    val result=FunctionService.removeFunction("pmml.notequal.000000000000000001")
    result should include regex "Deleted Function Successfully"
  }
/*
Github issue #1005 raised.


 it should "give alert if function to be deleted not present in the metadata" in {
   val result=FunctionService.removeFunction("pmml.max.invalid")
   result should include regex "Sorry, No functions available, in the Metadata, to delete!"
 }*/

 "get function" should " give error for invalid input" in {
   val result=FunctionService.getFunction("pmml.max.123")
   result should include regex "Failed to get function"
 }

 "load function from a file " should " process a valid function in a valid file" in {
   val fnDef = getClass.getResource("/Metadata/function/SampleFunctions_2.10.json").getPath
   val result=FunctionService.loadFunctionsFromAFile(fnDef)
   result should include regex ("Function Added Successfully")
 }
}

