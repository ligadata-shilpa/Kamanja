/*
 * Copyright 2016 ligaDATA
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

package com.ligadata.kamanja.metadataload

import com.ligadata.kamanja.metadata.MdMgr._
import com.ligadata.kamanja.metadata.ObjType._
import com.ligadata.kamanja.metadata._
import com.ligadata.KamanjaBase._
import com.ligadata.BaseTypes._

object ScalaVersionDependentInit {
	def initFactoryOfModelInstanceFactories(mgr : MdMgr): Unit = {
		mgr.AddFactoryOfModelInstanceFactory("com.ligadata.FactoryOfModelInstanceFactory", "JarFactoryOfModelInstanceFactory", ModelRepresentation.JAR, "com.ligadata.FactoryOfModelInstanceFactory.JarFactoryOfModelInstanceFactory$", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "jarfactoryofmodelinstancefactory_2.10-1.0.jar", Array("metadata_2.10-1.0.jar", "exceptions_2.10-1.0.jar", "kamanjabase_2.10-1.0.jar", "log4j-core-2.4.1.jar", "log4j-1.2-api-2.4.1.jar", "log4j-api-2.4.1.jar"))
		mgr.AddFactoryOfModelInstanceFactory("com.ligadata.jpmml", "JpmmlFactoryOfModelInstanceFactory", ModelRepresentation.PMML, "com.ligadata.jpmml.JpmmlFactoryOfModelInstanceFactory$", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "jpmmlfactoryofmodelinstancefactory_2.10-1.0.jar", Array("metadata_2.10-1.0.jar", "exceptions_2.10-1.0.jar", "kamanjabase_2.10-1.0.jar", "kamanjautils_2.10-1.0.jar", "kvbase_2.10-0.1.0.jar", "datadelimiters_2.10-1.0.jar", "log4j-api-2.4.1.jar", "log4j-core-2.4.1.jar", "log4j-1.2-api-2.4.1.jar", "joda-convert-1.6.jar", "joda-time-2.8.2.jar", "json4s-native_2.10-3.2.9.jar", "json4s-core_2.10-3.2.9.jar", "json4s-ast_2.10-3.2.9.jar", "paranamer-2.6.jar", "json4s-jackson_2.10-3.2.9.jar", "jackson-databind-2.3.1.jar", "jackson-annotations-2.3.0.jar", "jackson-core-2.3.1.jar", "jsr305-3.0.0.jar", "pmml-evaluator-1.2.9.jar", "pmml-model-1.2.9.jar", "pmml-agent-1.2.9.jar", "pmml-schema-1.2.9.jar", "guava-14.0.1.jar", "commons-math3-3.6.jar"))
	}

	def InitTypeDefs(mgr : MdMgr): Unit = {
		mgr.AddScalar(MdMgr.sysNS, "Any", tAny, "Any", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.AnyImpl")
		mgr.AddScalar(MdMgr.sysNS, "String", tString, "String", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.StringImpl")
		mgr.AddScalar(MdMgr.sysNS, "Int", tInt, "Int", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.IntImpl")
		mgr.AddScalar(MdMgr.sysNS, "Integer", tInt, "Int", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.IntImpl")
		mgr.AddScalar(MdMgr.sysNS, "Long", tLong, "Long", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.LongImpl")
		mgr.AddScalar(MdMgr.sysNS, "Boolean", tBoolean, "Boolean", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.BoolImpl")
		mgr.AddScalar(MdMgr.sysNS, "Bool", tBoolean, "Boolean", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.BoolImpl")
		mgr.AddScalar(MdMgr.sysNS, "Double", tDouble, "Double", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.DoubleImpl")
		mgr.AddScalar(MdMgr.sysNS, "Float", tFloat, "Float", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.FloatImpl")
		mgr.AddScalar(MdMgr.sysNS, "Char", tChar, "Char", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.CharImpl")

		mgr.AddScalar(MdMgr.sysNS, "date", tLong, "Long", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.LongImpl")
		mgr.AddScalar(MdMgr.sysNS, "dateTime", tLong, "Long", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.LongImpl")
		mgr.AddScalar(MdMgr.sysNS, "time", tLong, "Long", MetadataLoad.baseTypesTenant, MetadataLoad.baseTypesVer, "basetypes_2.10-0.1.0.jar", Array("metadata_2.10-1.0.jar"), "com.ligadata.BaseTypes.LongImpl")
	}
}

