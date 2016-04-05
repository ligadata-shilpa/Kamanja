package com.ligadata.MetadataAPI

import com.ligadata.kamanja.metadata.MdMgr

import scala.collection.mutable.Map

object AdapterMessageBindingUtils {

    val AdapterNameKey : String =  "AdapterName".toLowerCase
    val MessageNameKey : String =  "MessageName".toLowerCase
    val MessageNamesKey : String =  "MessageNames".toLowerCase
    val SerializerKey : String =  "Serializer".toLowerCase
    val OptionsKey : String =  "Options".toLowerCase

    /**
      * Add an AdapterMessageBinding to the metadata for the supplied values in the map.
      * @param bindingMap binding values for adapter name, message name, serializer name, and optionally serializer options.
      * @return json result string.
      */
    def AddAdapterMessageBinding(bindingMap : Map[String,Any]) : String = {

        val result : String = if (bindingMap != null) {
            val adapterName: String = bindingMap.getOrElse(AdapterNameKey, "**invalid adapter name**").asInstanceOf[String]
            val messageName: String = bindingMap.getOrElse(MessageNameKey, "**invalid message name**").asInstanceOf[String]
            val serializerName: String = bindingMap.getOrElse(SerializerKey, "**invalid serializer name**").asInstanceOf[String]
            val options: Map[String, String] = if (bindingMap.contains(OptionsKey)) {
                bindingMap(OptionsKey).asInstanceOf[Map[String, String]]
            } else {
                Map[String, String]()
            }

            // Fixme : Add semantic checks
            // Fixme : Add semantic checks
            // Fixme : Add semantic checks
            // Fixme : Add semantic checks
            // Fixme : Add semantic checks
            /**
              * 1) the message and serializerName names should be checked for namespace.name form
              * 2) the adapter name should exist in MdMgr's adapters map
              * 3) the serializer should exist in the MdMgr's serializers map
              * 4) names starting with ** should be flagged as errors
              */
            MdMgr.GetMdMgr.AddAdapterMessageBinding(adapterName, messageName, serializerName, options)

            val key : String = s"$adapterName.$messageName.$serializerName"
            val res : ApiResult = new ApiResult(0, "AddAdapterMessageBinding", "success!", "binding for $key was successfully added.")
            res.toString
        } else {
            new ApiResult(0, "AddAdapterMessageBinding", "failed!", "json map was null.").toString
        }
        result
    }

    /**
      * Add multiple AdapterMessageBindings in the supplied map.
      * @param bindingMaps a list of AdapaterMessageBindings specs as a list of json maps.
      * @return result string for all of the AdapterMessageBindings added.
      */
    def AddAdapterMessageBinding(bindingMaps : List[Map[String,Any]]) : String = {

        var resultBuffer : StringBuilder = new StringBuilder
        bindingMaps.foreach(bindingMap => {
            val result : String = if (bindingMap != null) {

                val messageNames: List[String] = bindingMap.getOrElse(MessageNamesKey, List[String]()).asInstanceOf[List[String]]
                val resultsStr : String = if (messageNames != null && messageNames.size > 0) {
                    /** handle multiple message name style specification... loop through them */
                    val multiBuffer : StringBuilder = new StringBuilder

                    messageNames.foreach(msgName => {
                        bindingMap.put(MessageNameKey, msgName)
                        val res : String = AddAdapterMessageBinding(bindingMap)
                        multiBuffer.append(s"$res\n")
                    })
                    multiBuffer.toString

                } else {
                    /** handle single name style specification */
                    val messageName: String = bindingMap.getOrElse(MessageNameKey, null).asInstanceOf[String]
                    val r : String = if (messageName != null) {
                        AddAdapterMessageBinding(bindingMap)
                    } else {
                        /** simply put the bad name there that will be trapped by the single binding handler */
                        val msgName : String = "**invalid message name**"
                        bindingMap.put(MessageNameKey, msgName)
                        AddAdapterMessageBinding(bindingMap)
                    }
                    s"$r\n"
                }
                resultsStr

            } else {
                new ApiResult(0, "AddAdapterMessageBinding", "failed!", "json map was null.").toString
            }
            resultBuffer.append(result.toString)
        })
        val results : String = resultBuffer.toString
        results
    }

}