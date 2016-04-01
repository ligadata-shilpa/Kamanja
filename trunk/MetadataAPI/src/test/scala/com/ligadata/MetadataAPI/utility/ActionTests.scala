package com.ligadata.MetadataAPI.utility
import org.scalatest._
import com.ligadata.kamanja.metadata._
import com.ligadata.MetadataAPI._
import com.ligadata.MetadataAPI.Utility.Action.Value
import com.ligadata.MetadataAPI.Utility.Action
/**
 * Created by dhavalkolapkar on 3/21/16.
 */

class ActionTests extends FlatSpec with Matchers {

  "ADDMESSAGE" should "return the corresponding value" in {
    Action.ADDMESSAGE.toString should include regex ("addmessage")
  }

  "UPDATEMESSAGE" should "return the corresponding value"  in {Action.UPDATEMESSAGE.toString should include regex ("updatemessage")}
  "GETALLMESSAGES" should "return the corresponding value"  in {Action.GETALLMESSAGES.toString should include regex ("getallmessages")}
  "REMOVEMESSAGE" should "return the corresponding value" in {Action.REMOVEMESSAGE.toString should include regex ("removemessage")}
  "GETMESSAGE" should "return the corresponding value"  in {Action.GETMESSAGE.toString should include regex ("getmessage")}
  //output message management
  "ADDOUTPUTMESSAGE" should "return the corresponding value"  in {Action.ADDOUTPUTMESSAGE.toString should include regex ("addoutputmessage")}
  "UPDATEOUTPUTMESSAGE" should "return the corresponding value"  in {Action.UPDATEOUTPUTMESSAGE.toString should include regex ("updateoutputmessage")}
  "REMOVEOUTPUTMESSAGE" should "return the corresponding value"  in {Action.REMOVEOUTPUTMESSAGE.toString should include regex ("removeoutputmessage")}
  "GETALLOUTPUTMESSAGES" should "return the corresponding value"  in {Action.GETALLOUTPUTMESSAGES.toString should include regex ("getalloutputmessages")}
  //model management
  "ADDMODELKPMML" should "return the corresponding value" in {Action.ADDMODELKPMML.toString should include regex ("addmodelkpmml")}
  "ADDMODELPMML" should "return the corresponding value"  in {Action.ADDMODELPMML.toString should include regex ("addmodelpmml")}
  "ADDMODELSCALA" should "return the corresponding value"  in {Action.ADDMODELSCALA.toString should include regex ("addmodelscala")}
  "ADDMODELJAVA" should "return the corresponding value"  in {Action.ADDMODELJAVA.toString should include regex ("addmodeljava")}
  "REMOVEMODEL" should "return the corresponding value"  in {Action.REMOVEMODEL.toString should include regex ("removemodel")}
  "ACTIVATEMODEL" should "return the corresponding value"  in {Action.ACTIVATEMODEL.toString should include regex ("activatemodel")}
  "DEACTIVATEMODEL" should "return the corresponding value"  in {Action.DEACTIVATEMODEL.toString should include regex ("deactivatemodel")}
  "UPDATEMODELKPMML" should "return the corresponding value"  in {Action.UPDATEMODELKPMML.toString should include regex ("updatemodelkpmml")}
  "UPDATEMODELPMML" should "return the corresponding value"  in {Action.UPDATEMODELPMML.toString should include regex ("updatemodelpmml")}
  "UPDATEMODELSCALA" should "return the corresponding value"  in {Action.UPDATEMODELSCALA.toString should include regex ("updatemodelscala")}
  "UPDATEMODELJAVA" should "return the corresponding value"  in {Action.UPDATEMODELJAVA.toString should include regex ("updatemodeljava")}
  "GETALLMODELS" should "return the corresponding value"  in {Action.GETALLMODELS.toString should include regex ("getallmodels")}
  "GETMODEL" should "return the corresponding value"  in {Action.GETMODEL.toString should include regex ("getmodel")}
  //container management
  "ADDCONTAINER" should "return the corresponding value"  in {Action.ADDCONTAINER.toString should include regex ("addcontainer")}
  "UPDATECONTAINER" should "return the corresponding value"  in {Action.UPDATECONTAINER.toString should include regex ("updatecontainer")}
  "GETCONTAINER" should "return the corresponding value"  in {Action.GETCONTAINER.toString should include regex ("getcontainer")}
  "GETALLCONTAINERS" should "return the corresponding value"  in {Action.GETALLCONTAINERS.toString should include regex ("getallcontainers")}
  "REMOVECONTAINER" should "return the corresponding value"  in {Action.REMOVECONTAINER.toString should include regex ("removecontainer")}
  //type management
  "ADDTYPE" should "return the corresponding value"  in {Action.ADDTYPE.toString should include regex ("addtype")}
  "GETTYPE" should "return the corresponding value"  in {Action.GETTYPE.toString should include regex ("gettype")}
  "GETALLTYPES" should "return the corresponding value"  in {Action.GETALLTYPES.toString should include regex ("getalltypes")}
  "REMOVETYPE" should "return the corresponding value"  in {Action.REMOVETYPE.toString should include regex ("removetype")}
  "LOADTYPESFROMAFILE" should "return the corresponding value"  in {Action.LOADTYPESFROMAFILE.toString should include regex ("loadtypesfromafile")}
  "DUMPALLTYPESBYOBJTYPEASJSON" should "return the corresponding value"  in {Action.DUMPALLTYPESBYOBJTYPEASJSON.toString should include regex ("dumpalltypes")}
  //function
  "ADDFUNCTION" should "return the corresponding value"  in {Action.ADDFUNCTION.toString should include regex ("addfunction")}
  "GETFUNCTION" should "return the corresponding value"  in {Action.GETFUNCTION.toString should include regex ("getfunction")}
  "REMOVEFUNCTION" should "return the corresponding value"  in {Action.REMOVEFUNCTION.toString should include regex ("removefunction")}
  "UPDATEFUNCTION" should "return the corresponding value"  in {Action.UPDATEFUNCTION.toString should include regex ("updatefunction")}
  "LOADFUNCTIONSFROMAFILE" should "return the corresponding value"  in {Action.LOADFUNCTIONSFROMAFILE.toString should include regex ("loadfunctionsfromafile")}
  "DUMPALLFUNCTIONSASJSON" should "return the corresponding value"  in {Action.DUMPALLFUNCTIONSASJSON.toString should include regex ("dumpallfunctions")}
  //config
  "UPLOADCLUSTERCONFIG" should "return the corresponding value"  in {Action.UPLOADCLUSTERCONFIG.toString should include regex ("uploadclusterconfig")}
  "UPLOADCOMPILECONFIG" should "return the corresponding value"  in {Action.UPLOADCOMPILECONFIG.toString should include regex ("uploadcompileconfig")}
  "DUMPALLCFGOBJECTS" should "return the corresponding value"  in {Action.DUMPALLCFGOBJECTS.toString should include regex ("dumpallcfgobjects")}
  "REMOVEENGINECONFIG" should "return the corresponding value"  in {Action.REMOVEENGINECONFIG.toString should include regex ("removeengineconfig")}
  //Concept
  "ADDCONCEPT" should "return the corresponding value"  in {Action.ADDCONCEPT.toString should include regex ("addconcept")}
  "REMOVECONCEPT" should "return the corresponding value"  in {Action.REMOVECONCEPT.toString should include regex ("removeconcept")}
  "UPDATECONCEPT" should "return the corresponding value"  in {Action.UPDATECONCEPT.toString should include regex ("updateconcept")}
  "LOADCONCEPTSFROMAFILE" should "return the corresponding value"  in {Action.LOADCONCEPTSFROMAFILE.toString should include regex ("loadconceptsfromafile ")}
  "DUMPALLCONCEPTSASJSON" should "return the corresponding value"  in {Action.DUMPALLCONCEPTSASJSON.toString should include regex ("dumpallconcepts")}
  "UPLOADJAR" should "return the corresponding value"  in {Action.UPLOADJAR.toString should include regex ("uploadjar")}
  //dump
  "DUMPMETADATA" should "return the corresponding value"  in {Action.DUMPMETADATA.toString should include regex ("dumpmetadata")}
  "DUMPALLNODES" should "return the corresponding value"  in {Action.DUMPALLNODES.toString should include regex ("dumpallnodes")}
  "DUMPALLCLUSTERS" should "return the corresponding value"  in {Action.DUMPALLCLUSTERS.toString should include regex ("dumpallclusters")}
  "DUMPALLCLUSTERCFGS" should "return the corresponding value"  in {Action.DUMPALLCLUSTERCFGS.toString should include regex ("dumpallclustercfgs")}
  "DUMPALLADAPTERS" should "return the corresponding value"  in {Action.DUMPALLADAPTERS.toString should include regex ("dumpalladapters")}
  "GETOUTPUTMESSAGE" should "return the corresponding value"  in {Action.GETOUTPUTMESSAGE.toString should include regex ("getoutputmessage")}
}

