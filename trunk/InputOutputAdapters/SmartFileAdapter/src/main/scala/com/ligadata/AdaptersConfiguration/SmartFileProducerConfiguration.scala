/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.AdaptersConfiguration

import com.ligadata.InputOutputAdapterInfo.{ AdapterConfiguration, PartitionUniqueRecordKey, PartitionUniqueRecordValue }
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

class SmartFileProducerConfiguration extends AdapterConfiguration {
  var Location: String = _ // URI to the location where the files need to be created
  var CompressionString: String = _ // If it is null or empty we treat it as TEXT file
  var FileNamePrefix: String = _ // prefix for the file names
  
  var Authentication: String = _ // authentication method to be used. Kerberos for now
  var Principal: String = _ // kerberos authentication principal
  var Keytab: String = _  // keytab file for authentication principal
}

object SmartFileProducerConfiguration {
  def GetAdapterConfig(inputConfig: AdapterConfiguration): SmartFileProducerConfiguration = {
    if (inputConfig.adapterSpecificCfg == null || inputConfig.adapterSpecificCfg.size == 0) {
      val err = "Smart File Adapter specific configuration not found : " + inputConfig.Name
      throw new Exception(err)
    }

    val fc = new SmartFileProducerConfiguration
    fc.Name = inputConfig.Name
    fc.className = inputConfig.className
    fc.jarName = inputConfig.jarName
    fc.dependencyJars = inputConfig.dependencyJars

    val adapCfg = parse(inputConfig.adapterSpecificCfg)
    if (adapCfg == null || adapCfg.values == null) {
      val err = "Invalid Smart File Adapter specific configuration :" + inputConfig.Name
      throw new Exception(err)
    }
    val values = adapCfg.values.asInstanceOf[Map[String, String]]

    values.foreach(kv => {
      if (kv._1.compareToIgnoreCase("CompressionString") == 0) {
        fc.CompressionString = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("FileNamePrefix") == 0) {
        fc.FileNamePrefix = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("Location") == 0) {
        fc.Location = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("Authentication") == 0) {
        fc.Authentication = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("Principal") == 0) {
        fc.Principal = kv._2.trim
      } else if (kv._1.compareToIgnoreCase("Keytab") == 0) {
        fc.Keytab = kv._2.trim
      }
    })

    fc
  }
}
