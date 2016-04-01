package com.ligadata.MetadataAPI.utility.test

import java.io.{File, ByteArrayInputStream}

import org.scalatest._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.ConceptService

/**
 * Created by dhavalkolapkar on 3/21/16.
 */
class ConceptServiceTests extends FlatSpec with Matchers with MetadataBeforeAndAfterEach {

  "addConcept" should "add the valid concept" in {
    ConceptService.addConcept(getClass.getResource("/Metadata/concept/SampleConcepts.json").getPath) should include regex ("Concept Added Successfully")
  }

  it should "add the user requested concept" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    ConceptService.addConcept("") should include regex ("Concept Added Successfully")
  }

  "removeConcept" should "remove the concept from the metadata" in {
    ConceptService.addConcept(getClass.getResource("/Metadata/concept/SampleConcepts.json").getPath)
    ConceptService.removeConcept("ligadata.providerid",Some("Metadataapi")) should include regex ("Deleted Concept Successfully")
  }

  it should "remove the user requested concept from the metadata" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    ConceptService.addConcept(getClass.getResource("/Metadata/concept/SampleConcepts.json").getPath)
    ConceptService.removeConcept("",Some("Metadataapi")) should include regex ("Deleted Concept Successfully")
  }
/* Invalid call from kamanja command line #1007
  "updateConcept" should "update the requested concept in the metadata" in {
    ConceptService.addContainer(getClass.getResource("/Metadata/container/EnvCodes.json").getPath)
    ConceptService.getContainer("system.context.000000000000000001") should include regex "Successfully fetched concept from Cache"
  }*/

  "loadConceptsFromAFile" should " load the the valid concept" in {
    ConceptService.loadConceptsFromAFile should include regex "NOT REQUIRED. Please use the ADD CONCEPT option."
  }

  "dumpAllConceptsAsJson" should "retrieve all the concepts in the metadata" in {
    ConceptService.addConcept(getClass.getResource("/Metadata/concept/SampleConcepts.json").getPath)
    ConceptService.dumpAllConceptsAsJson  should include regex "Successfully fetched all concepts"
  }

  "is valid dir" should " validate if the directory is present" in {
    assert((ConceptService.IsValidDir(getClass.getResource("/Metadata/message").getPath))==true)
  }

  it should " invalidate a wrong directory path" in {
    assert((ConceptService.IsValidDir(getClass.getResource("/Metadata/message/Message_Definition_HelloWorld.json").getPath+"Invalid"))==false)
  }

  "user input " should "get the user input and perform requested operation on it" in {
    Console.setIn(new ByteArrayInputStream("1".getBytes))
    val messages: Array[File] = new java.io.File(getClass.getResource("/Metadata/inputMessage").getPath).listFiles.filter(_.getName.endsWith(".json"))
    ConceptService.getUserInputFromMainMenu(messages).size > 0
  }
}

