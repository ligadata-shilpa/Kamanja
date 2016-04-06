package com.ligadata.MetadataAPI

import com.ligadata.Serialize.SerializerManager
import com.ligadata.kamanja.metadata.{AdapterMessageBinding, MdMgr}

import scala.collection.mutable.Map

object AdapterMessageBindingUtils {

    lazy val serializerType = "kryo"
    lazy val serializer = SerializerManager.GetSerializer(serializerType)


    val AdapterNameKey : String =  "AdapterName".toLowerCase
    val MessageNameKey : String =  "MessageName".toLowerCase
    val MessageNamesKey : String =  "MessageNames".toLowerCase
    val SerializerKey : String =  "Serializer".toLowerCase
    val OptionsKey : String =  "Options".toLowerCase

    val mdMgr : MdMgr = MdMgr.GetMdMgr

    /**
      * Add an AdapterMessageBinding to the metadata for the supplied values in the map.
      *
      * @param bindingMap binding values for adapter name, message name, serializer name, and optionally serializer options.
      * @param userId the user requesting this operation
      * @return json result string.
      */
    def AddAdapterMessageBinding(bindingMap : Map[String,Any], userId : Option[String]) : String = {

        val result : String = if (bindingMap != null) {
            val adapterName: String = bindingMap.getOrElse(AdapterNameKey, "**invalid adapter name**").asInstanceOf[String].trim
            val messageName: String = bindingMap.getOrElse(MessageNameKey, "**invalid message name**").asInstanceOf[String].trim
            val serializerName: String = bindingMap.getOrElse(SerializerKey, "**invalid serializer name**").asInstanceOf[String].trim
            val options: Map[String, String] = if (bindingMap.contains(OptionsKey)) {
                bindingMap(OptionsKey).asInstanceOf[Map[String, String]]
            } else {
                Map[String, String]()
            }

            val (acceptableBinding, errorMsgs) : (Boolean, String) = SemanticChecks(adapterName, messageName, serializerName, options)
            if (acceptableBinding) {

                /** Create an AdapterMessageBinding from the supplied map values and catalog it to mdmgr */
                val binding: AdapterMessageBinding = mdMgr.MakeAdapterMessageBinding(adapterName
                                                                                    , messageName
                                                                                    , serializerName
                                                                                    , options)
                mdMgr.AddAdapterMessageBinding(binding)

                /** persist the binding for next session */
                val key: String = s"$adapterName.$messageName.$serializerName"
                val value = serializer.SerializeObjectToByteArray(binding)
                MetadataAPIImpl.SaveObject(key.toLowerCase, value, "adapter_message_bindings", serializerType)

                /** format a json string for return result */
                val res: ApiResult = new ApiResult(0, "AddAdapterMessageBinding", "success!", "binding for $key was successfully added.\n")
                res.toString
            } else {
                val res: ApiResult = new ApiResult(-1, "AddAdapterMessageBinding", "failure!", s"$errorMsgs\n")
                res.toString
            }
        } else {
            new ApiResult(-1, "AddAdapterMessageBinding", "failed!", "json map was null.").toString
        }
        result
    }

    /**
      * Check out the AdapterMessageBinding values to be used are acceptable.  Checks are:
      *
      * 1) if the names start with "**" it means that the name was not supplied and this is itself an error message
      *    substitution value
      * 2) the message and serializerName names should be checked for namespace.name form
      * 3) the adapter name should exist in MdMgr's adapters map
      * 4) the message should exist in the MdMgr's message map
      * 5) the serializer should exist in the MdMgr's serializers map
      *
      * @param adapterName the name of the adapter
      * @param messageName the namespace.name of the message to be bound
      * @param serializerName the serializer that will be used to flatten/resurrect the message on binder's behalf
      * @param options the serializer options that may be used to configure the serializer
      * @return (boolean, string) If true returned, arguments are acceptable.  If false, the operation will be abandoned
      *         and the returned string contains the issues found.
      */
    private def SemanticChecks(adapterName: String, messageName: String, serializerName: String, options: Map[String,String]) : (Boolean, String) = {
        val buffer : StringBuilder = new StringBuilder

        /** 1) if the names start with "**" it means that the name was not supplied ... issue name error */
        val adapterStartsWithAstsk : Boolean = (adapterName != null && adapterName.startsWith("**"))
        val messageStartsWithAstsk : Boolean = (messageName != null && messageName.startsWith("**"))
        val serializerStartsWithAstsk : Boolean = (serializerName != null && serializerName.startsWith("**"))
        if (adapterStartsWithAstsk) {
            buffer.append(s"the adapter name was not supplied ... $adapterName...\n")
        }
        if (messageStartsWithAstsk) {
            buffer.append(s"the message name was not supplied ... $messageName...\n")
        }
        if (serializerStartsWithAstsk) {
            buffer.append(s"the serlializer name was not supplied ... $serializerName...\n")
        }

        /** 2) the message and serializerName are checked for namespace.name form */
        val messageNameHasDots : Boolean = (! messageStartsWithAstsk && messageName.size > 2 && messageName.contains('.'))
        val serializerNameHasDots : Boolean = (! serializerStartsWithAstsk && serializerName.size > 2 && serializerName.contains('.'))
        if (! (messageNameHasDots && serializerNameHasDots)) {
            buffer.append(s"$messageName and/or $serializerName do not conform to namespace.name form...\n")
        }
        /** 3) the adapter name should exist in MdMgr's adapters map */
        val adapterPresent : Boolean = mdMgr.GetAdapter(adapterName) != null
        if (! adapterPresent) {
            buffer.append(s"the adapter $adapterName cannot be found in the metadata...\n")
        }
        if (messageNameHasDots) {
            /** 4) the message should exist in the MdMgr's message map */
            val msgNamespace : String = messageName.split('.').dropRight(1).mkString(".")
            val msgName : String = messageName.split('.').last
            val adapterPresent: Boolean = mdMgr.ActiveMessage(msgNamespace, msgName) != null
            if (! adapterPresent) {
                buffer.append(s"the message $messageName cannot be found in the metadata...\n")
            }
        }
        if (serializerNameHasDots) {
            /** 5) he serializer should exist in the MdMgr's serializers map */
            val adapterPresent: Boolean = mdMgr.GetSerializer(messageName) != null
            if (! adapterPresent) {
                buffer.append(s"the message $messageName cannot be found in the metadata...\n")
            }
        }


        val ok = buffer.isEmpty
        val errorMsgs : String = buffer.toString
        (ok,errorMsgs)
    }

    /**
      * Add multiple AdapterMessageBindings in the supplied map.
      *
      * @param bindingMaps a list of AdapaterMessageBindings specs as a list of json maps.
      * @param userId the user requesting this operation
      * @return result string for all of the AdapterMessageBindings added.
      */
    def AddAdapterMessageBinding(bindingMaps : List[Map[String,Any]], userId : Option[String]) : String = {

        var resultBuffer : StringBuilder = new StringBuilder
        bindingMaps.foreach(bindingMap => {
            val result : String = if (bindingMap != null) {
                val messageNames: List[String] = bindingMap.getOrElse(MessageNamesKey, List[String]()).asInstanceOf[List[String]]
                val resultsStr : String = if (messageNames != null && messageNames.size > 0) {
                    /** handle multiple message name style specification... loop through them */
                    val multiBuffer : StringBuilder = new StringBuilder

                    messageNames.foreach(msgName => {
                        bindingMap.put(MessageNameKey, msgName) /** slam the key in place */
                        val res : String = AddAdapterMessageBinding(bindingMap, userId)
                        multiBuffer.append(s"$res\n")
                    })
                    multiBuffer.toString

                } else {
                    /** handle single name style specification */
                    val messageName: String = bindingMap.getOrElse(MessageNameKey, null).asInstanceOf[String]
                    val r : String = if (messageName != null) {
                        AddAdapterMessageBinding(bindingMap, userId)
                    } else {
                        /** simply put the bad name there that will be trapped by the single binding handler */
                        val msgName : String = "**invalid message name**"
                        bindingMap.put(MessageNameKey, msgName)
                        AddAdapterMessageBinding(bindingMap, userId)
                    }
                    s"$r\n"
                }
                resultsStr

            } else {
                new ApiResult(-1, "AddAdapterMessageBinding", "failed!", "json map was null.").toString
            }
            resultBuffer.append(result.toString)
        })
        val results : String = resultBuffer.toString
        results
    }

}