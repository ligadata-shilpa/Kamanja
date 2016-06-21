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
package com.ligadata.jython.test

import java.io.File
import java.lang.reflect.Method

import com.ligadata.jython._
import org.apache.commons.io.FileUtils
import org.apache.logging.log4j.LogManager

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.python.core.Py
import org.python.core.PyObject
import org.python.core.PyString
import org.python.util.PythonInterpreter
import com.ligadata.kamanja.metadata.ModelDef
import com.ligadata.KamanjaBase._

//import com.ligadata.kamanja.samples.messages._
import java.util.Properties
import com.ligadata.runtime.Log
import java.net.{URL, URLClassLoader}
import org.python.core.PySystemState
import org.python.core.packagecache.PackageManager

import com.ligadata.KamanjaBase.ContainerOrConcept
/**
  *
  */
class TestJython extends FunSuite with BeforeAndAfter {

  val logger = new com.ligadata.runtime.Log(this.getClass.getName())

  // Simple jtm
  test("test1") {
    val code =
      """
        |#
        |# Copyright 2016 ligaDATA
        |#
        |# Licensed under the Apache License, Version 2.0 (the "License");
        |# you may not use this file except in compliance with the License.
        |# You may obtain a copy of the License at
        |#
        |#     http://www.apache.org/licenses/LICENSE-2.0
        |#
        |# Unless required by applicable law or agreed to in writing, software
        |# distributed under the License is distributed on an "AS IS" BASIS,
        |# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        |# See the License for the specific language governing permissions and
        |# limitations under the License.
        |#
        |#import sys
        |from com.ligadata.runtime import Log
        |#from com.ligadata.KamanjaBase import ContainerOrConcept
        |from com.ligadata.kamanja.samples.messages import msg1
        |from com.ligadata.kamanja.samples.messages import outmsg1
        |
        |class Model():
        |   def __init__(self):
        |       self.logger = Log("Model")
        |       self.logger.Info('Model.__init__')
        |
        |   def execute(self, txnCtxt, execMsgsSet, matchedInputSetIndex, outputDefault):
        |       self.logger.Info('Model.execute')
        |       inMsg = execMsgsSet[0]
        |       self.logger.Info('jython>>> 21')
        |       if inMsg.id()!=111:
        |           self.logger.Info('jython>>> 22')
        |           v = inMsg.in1()
        |           self.logger.Info('Early exit')
        |           return None
        |
        |       self.logger.Info('jython>>> 23')
        |       output = outmsg1.createInstance()
        |       self.logger.Info('jython>>> 24')
        |       output.set(0, inMsg.id())
        |       self.logger.Info('jython>>> 25')
        |       output.set(1, inMsg.name())
        |       self.logger.Info('jython>>> 26')
        |
        |       return output
        |return Model()
      """.stripMargin

    val cp="/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/kvinit_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/containersutility_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/metadataapi_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/kamanjamanager_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/simplekafkaproducer_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/extractdata_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/jdbcdatacollector_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/metadataapiservice_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/filedataconsumer_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/cleanutil_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/pmmltesttool-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/jsonchecker_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/nodeinfoextract_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/ExtDependencyLibs_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/ExtDependencyLibs2_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/KamanjaInternalDeps_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/jarfactoryofmodelinstancefactory_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/migratebase-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/migratefrom_v_1_1_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/migratefrom_v_1_2_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/migratefrom_v_1_3_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/migrateto_v_1_4_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/generateadapterbindings_2.10-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/system/migratefrom_v_1_3_2.11-1.4.1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.kamanja.samples.messages_msg1_1000000_1465412866388.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.kamanja.samples.messages_msg1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.kamanja.samples.messages_outmsg1_1000000_1465412910148.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.kamanja.samples.messages_outmsg1.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V1_HelloWorlJythondModel_0.0.1_1465414330598.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V1_HelloWorlJythondModel_0.0.1_1465418910326.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V1_HelloWorlJythondModel_0.0.1_1465419011856.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V2_HelloWorlJythondModel_0.0.2_1465419034006.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V2_HelloWorlJythondModel_0.0.2_1465419074737.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V2_HelloWorlJythondModel_0.0.2_1465419108181.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V2_HelloWorlJythondModel_0.0.2_1465419132294.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V1_HelloWorlJythondModel_0.0.1_1465419169540.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V2_HelloWorlJythondModel_0.0.2_1465419205063.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V2_HelloWorlJythondModel_0.0.2_1465419564109.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465420419220.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497260337.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497469163.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497494877.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497511191.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497535776.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497619523.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497692886.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V3_HelloWorlJythondModel_0.0.3_1465497779545.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V4_HelloWorlJythondModel_0.0.4_1465498567008.jar:/home/joerg/app2/Kamanja-1.4.1_2.10/lib/application/com.ligadata.samples.models.V5_HelloWorlJythondModel_0.0.5_1465498923845.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.logging.log4j/log4j-api/log4j-api-2.4.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.logging.log4j/log4j-core/log4j-core-2.4.1.jar".split(':')

  val cp1 = "/home/joerg/Kamanja/trunk/lib_managed/jars/com.yammer.metrics/metrics-core/metrics-core-2.2.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/commons-cli/commons-cli/commons-cli-1.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.101tec/zkclient/zkclient-0.6.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.commons/commons-vfs2/commons-vfs2-2.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.commons/commons-collections4/commons-collections4-4.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.commons/commons-pool2/commons-pool2-2.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.commons/commons-math3/commons-math3-3.6.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.commons/commons-dbcp2/commons-dbcp2-2.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.commons/commons-compress/commons-compress-1.5.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.commons/commons-math/commons-math-2.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.commons/commons-lang3/commons-lang3-3.4.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/tokens_2.11/tokens_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/taxonomic_2.11/taxonomic_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/dialects_2.11/dialects_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/tokenizers_2.11/tokenizers_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/tql_2.11/tql_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/foundation_2.11/foundation_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/quasiquotes_2.11/quasiquotes_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/trees_2.11/trees_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/parsers_2.11/parsers_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/exceptions_2.11/exceptions_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/prettyprinters_2.11/prettyprinters_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/scalameta_2.11/scalameta_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/semantic_2.11/semantic_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/tokenquasiquotes_2.11/tokenquasiquotes_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scalameta/interactive_2.11/interactive_2.11-0.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.rogach/scallop_2.11/scallop_2.11-0.9.5.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hbase/hbase-annotations/hbase-annotations-1.0.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hbase/hbase-common/hbase-common-1.0.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hbase/hbase-protocol/hbase-protocol-1.0.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hbase/hbase-client/hbase-client-1.0.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.joda/joda-convert/joda-convert-1.7.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.joda/joda-convert/joda-convert-1.6.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.hamcrest/hamcrest-core/hamcrest-core-1.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.github.tony19/named-regexp/named-regexp-0.2.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/antlr/antlr/antlr-2.7.7.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scala-lang/scalap/scalap-2.11.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scala-lang/scala-library/scala-library-2.11.7.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scala-lang/scala-compiler/scala-compiler-2.11.7.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scala-lang/scala-compiler/scala-compiler-2.11.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scala-lang/scala-reflect/scala-reflect-2.11.7.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scala-lang/scala-reflect/scala-reflect-2.11.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scala-lang/scala-actors/scala-actors-2.11.7.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.kafka/kafka-clients/kafka-clients-0.8.2.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.kafka/kafka_2.11/kafka_2.11-0.8.2.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.slf4j/slf4j-api/slf4j-api-1.6.4.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.slf4j/slf4j-api/slf4j-api-1.7.12.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.slf4j/slf4j-log4j12/slf4j-log4j12-1.7.12.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.slf4j/slf4j-log4j12/slf4j-log4j12-1.6.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.slf4j/slf4j-nop/slf4j-nop-1.7.12.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/javax.activation/activation/activation-1.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.json4s/json4s-ast_2.11/json4s-ast_2.11-3.2.9.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.json4s/json4s-native_2.11/json4s-native_2.11-3.2.9.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.json4s/json4s-jackson_2.11/json4s-jackson_2.11-3.2.9.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.json4s/json4s-core_2.11/json4s-core_2.11-3.2.9.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/commons-digester/commons-digester/commons-digester-1.8.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-mapreduce-client-jobclient/hadoop-mapreduce-client-jobclient-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-yarn-client/hadoop-yarn-client-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-mapreduce-client-core/hadoop-mapreduce-client-core-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-common/hadoop-common-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-hdfs/hadoop-hdfs-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-yarn-common/hadoop-yarn-common-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-yarn-server-nodemanager/hadoop-yarn-server-nodemanager-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-auth/hadoop-auth-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-client/hadoop-client-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-yarn-api/hadoop-yarn-api-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-mapreduce-client-app/hadoop-mapreduce-client-app-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-yarn-server-common/hadoop-yarn-server-common-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-mapreduce-client-shuffle/hadoop-mapreduce-client-shuffle-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-annotations/hadoop-annotations-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.hadoop/hadoop-mapreduce-client-common/hadoop-mapreduce-client-common-2.7.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.objenesis/objenesis/objenesis-1.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.jpmml/pmml-schema/pmml-schema-1.2.9.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.jpmml/pmml-evaluator/pmml-evaluator-1.2.9.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.jpmml/pmml-model/pmml-model-1.2.9.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.jpmml/pmml-agent/pmml-agent-1.2.9.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/javax.servlet/servlet-api/servlet-api-2.5.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/aopalliance/aopalliance/aopalliance-1.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/oro/oro/oro-2.0.8.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/xmlenc/xmlenc/xmlenc-0.52.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/regexp/regexp/regexp-1.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.avro/avro/avro-1.7.4.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.sonatype.sisu.inject/cglib/cglib-2.2.1-v20090111.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.logging.log4j/log4j-api/log4j-api-2.4.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.logging.log4j/log4j-core/log4j-core-2.4.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.ivy/ivy/ivy-2.4.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/joda-time/joda-time/joda-time-2.9.4.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/joda-time/joda-time/joda-time-2.9.4-javadoc.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/joda-time/joda-time/joda-time-2.8.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/commons-httpclient/commons-httpclient/commons-httpclient-3.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/net.sf.jopt-simple/jopt-simple/jopt-simple-3.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.mortbay.jetty/servlet-api/servlet-api-2.5.20110712.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.mortbay.jetty/servlet-api/servlet-api-2.5.20110712-sources.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.mortbay.jetty/jetty-sslengine/jetty-sslengine-6.1.26.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.mortbay.jetty/jetty-embedded/jetty-embedded-6.1.26-sources.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.mortbay.jetty/jetty-util/jetty-util-6.1.26.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.mortbay.jetty/jetty/jetty-6.1.26.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.thoughtworks.paranamer/paranamer/paranamer-2.6.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/commons-beanutils/commons-beanutils/commons-beanutils-1.8.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.jruby.jcodings/jcodings/jcodings-1.0.8.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.jruby.joni/joni/joni-2.1.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/junit/junit/junit-4.12.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/junit/junit/junit-3.8.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/net.sf.ehcache/ehcache-core/ehcache-core-2.6.5.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/net.sf.ehcache/ehcache-jgroupsreplication/ehcache-jgroupsreplication-1.7.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/commons-collections/commons-collections/commons-collections-3.2.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.ow2.asm/asm-tree/asm-tree-4.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.ow2.asm/asm/asm-4.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.ow2.asm/asm-commons/asm-commons-4.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.json/json/json-20090211.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/commons-daemon/commons-daemon/commons-daemon-1.0.13.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.twitter/chill-java/chill-java-0.5.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.twitter/chill_2.11/chill_2.11-0.5.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.sun.jersey.contribs/jersey-guice/jersey-guice-1.9.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scoverage/scalac-scoverage-plugin_2.11/scalac-scoverage-plugin_2.11-1.1.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.scoverage/scalac-scoverage-runtime_2.11/scalac-scoverage-runtime_2.11-1.1.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.typesafe.akka/akka-slf4j_2.11/akka-slf4j_2.11-2.3.9.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.typesafe.akka/akka-actor_2.11/akka-actor_2.11-2.3.9.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.typesafe.akka/akka-testkit_2.11/akka-testkit_2.11-2.3.9.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.skyscreamer/jsonassert/jsonassert-1.3.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/commons-logging/commons-logging/commons-logging-1.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.httpcomponents/httpcore/httpcore-4.2.4.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.httpcomponents/httpclient/httpclient-4.2.5.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/net.java.dev.jets3t/jets3t/jets3t-0.9.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.maven.scm/maven-scm-api/maven-scm-api-1.4.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.maven.scm/maven-scm-provider-svnexe/maven-scm-provider-svnexe-1.4.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.maven.scm/maven-scm-provider-svn-commons/maven-scm-provider-svn-commons-1.4.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/xml-apis/xmlParserAPIs/xmlParserAPIs-2.0.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/jmimemagic/jmimemagic/jmimemagic-0.1.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.github.stephenc.findbugs/findbugs-annotations/findbugs-annotations-1.3.9-1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.google.code.findbugs/jsr305/jsr305-3.0.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.googlecode.json-simple/json-simple/json-simple-1.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/commons-codec/commons-codec/commons-codec-1.10.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/commons-configuration/commons-configuration/commons-configuration-1.7.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/net.jpountz.lz4/lz4/lz4-1.2.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/jline/jline/jline-0.9.94.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/jline/jline/jline-2.12.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.curator/curator-test/curator-test-2.8.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.esotericsoftware.reflectasm/reflectasm/reflectasm-1.07-shaded.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/asm/asm/asm-3.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.pyruby/java-stub-server/java-stub-server-0.12-sources.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/ch.qos.logback/logback-core/logback-core-1.0.13.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/ch.qos.logback/logback-classic/logback-classic-1.0.13.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.thrift/libthrift/libthrift-0.9.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/javax.inject/javax.inject/javax.inject-1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.parboiled/parboiled-scala_2.11/parboiled-scala_2.11-1.1.7.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.parboiled/parboiled-core/parboiled-core-1.1.7.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.codehaus.jackson/jackson-jaxrs/jackson-jaxrs-1.9.13.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.codehaus.jackson/jackson-core-asl/jackson-core-asl-1.9.13.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.codehaus.jackson/jackson-mapper-asl/jackson-mapper-asl-1.9.13.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.codehaus.jackson/jackson-xc/jackson-xc-1.9.13.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.chuusai/shapeless_2.11/shapeless_2.11-1.2.4.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/commons-io/commons-io/commons-io-2.4.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/javax.servlet.jsp/jsp-api/jsp-api-2.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/commons-lang/commons-lang/commons-lang-2.6.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/io.netty/netty-all/netty-all-4.0.23.Final.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.sun.xml.bind/jaxb-impl/jaxb-impl-2.2.3-1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.tukaani/xz/xz-1.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.htrace/htrace-core/htrace-core-3.1.0-incubating.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.zookeeper/zookeeper/zookeeper-3.4.6.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/javax.xml.stream/stax-api/stax-api-1.0-2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.esotericsoftware.minlog/minlog/minlog-1.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.codehaus.plexus/plexus-utils/plexus-utils-1.5.6.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/uk.co.bigbeeconsultants/bee-client_2.11/bee-client_2.11-0.28.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.jcraft/jsch/jsch-0.1.53.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/javax.xml.bind/jaxb-api/jaxb-api-2.2.2.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/xerces/xercesImpl/xercesImpl-2.9.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.anarres.lzo/lzo-core/lzo-core-1.0.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.jvnet.mimepull/mimepull/mimepull-1.9.5.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/commons-net/commons-net/commons-net-3.1.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.apache.cassandra/cassandra-thrift/cassandra-thrift-2.0.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.python/jython/jython-2.7.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.google.code.gson/gson/gson-2.5.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.beust/jcommander/jcommander-1.48.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.google.inject/guice/guice-3.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/io.spray/spray-testkit_2.11/spray-testkit_2.11-1.3.3.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/org.aicer.grok/grok/grok-0.9.0.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.jamesmurty.utils/java-xmlbuilder/java-xmlbuilder-0.4.jar:/home/joerg/Kamanja/trunk/lib_managed/jars/com.sdicons.jsontools/jsontools-core/jsontools-core-1.7-sources.jar".split(':')

  def urlses(cl: ClassLoader): Array[java.net.URL] = cl match {
    case null => Array()
    case u: java.net.URLClassLoader => u.getURLs() ++ urlses(cl.getParent)
    case _ => urlses(cl.getParent)
  }

  class PythonInterpreterClassLoader(url: Array[URL], parent : ClassLoader) extends URLClassLoader(url, parent) {
  }

  try {
  // Create class loader
  var cl1 = new PythonInterpreterClassLoader(cp1.map(c => new File(c).toURI().toURL), java.lang.ClassLoader.getSystemClassLoader())
  //var method: Method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
  //method.setAccessible(true)


  val urls2 = urlses(cl1)
  logger.Info("CLASSPATH-JYTHON-1:=" + urls2.mkString(":"))

  var props: Properties = new Properties();
  props.put("python.home", "/home/joerg/bin/jython/Lib")
  props.put("python.console.encoding", "UTF-8")
  props.put("python.security.respectJavaAccessibility", "false")
  props.put("python.import.site", "false")
  //props.put("classpath", cp)

  var preprops: Properties = System.getProperties()

  //var interpreterModule = cl1.loadClass("org.python.util.PythonInterpreter$")
  //var method: Method = interpreterModule.getDeclaredMethod("initialize", interpreterModule.getClass)
  //method.invoke(interpreterModule, preprops, props, Array.empty[String])
  PythonInterpreter.initialize(preprops, props, Array.empty[String])

  val interpreter1 = cl1.loadClass("org.python.util.PythonInterpreter").newInstance()

  val interpreter = interpreter1.asInstanceOf[org.python.util.PythonInterpreter]

    {
      val cl2 = interpreter.getSystemState().getClassLoader()
      val urls2 = urlses(cl2)
      logger.Info("CLASSPATH-JYTHON-2:=" + urls2.mkString(":"))
    }

    //val sysstate: PySystemState = new PySystemState()

//    cp.foreach(c => PySystemState.packageManager.addJar(c, true))

//    sysstate.setClassLoader(java.lang.ClassLoader.getSystemClassLoader())
//
//    // Create the jython object
//    logger.Info("<<< 10")
    //val interpreter = new PythonInterpreter(null, sysstate)
//    val interpreter = new PythonInterpreter()
//    logger.Info("<<< 11")
//
//    def urlses(cl: ClassLoader): Array[java.net.URL] = cl match {
//      case null => Array()
//      case u: java.net.URLClassLoader => u.getURLs() ++ urlses(cl.getParent)
//      case _ => urlses(cl.getParent)
//    }
//
//    logger.Info("<<< 11-0")
////
////    val cl1 = interpreter.getSystemState().getClassLoader()
////    val urls1 = urlses(cl1)
////    logger.Info("CLASSPATH-JYTHON-1:=" + urls1.mkString(":"))
////
////    var method: Method = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
////    method.setAccessible(true)
////    cp.foreach(c =>method.invoke(cl1, new File(c).toURI().toURL))
////
////    //cp.foreach(c => cl1.addURL(new File(c).toURI().toURL))
//
//    logger.Info("<<< 11-1")
//
//    {
//      val cl2 = interpreter.getSystemState().getClassLoader()
//      val urls2 = urlses(cl2)
//      logger.Info("CLASSPATH-JYTHON-2:=" + urls2.mkString(":"))
//    }
//
//    logger.Info("<<< 11-2")


      val modelObject: PyObject = interpreter.eval(code)
    } catch {
      case e: Exception =>  println(e.toString)
        throw e
    }
    logger.Info("<<< 12")
    //interpreter.exec("import Model")
    logger.Info("<<< 13")
    //val modelClass = interpreter.get("Model")
    logger.Info("<<< 14")
    //val modelObject: PyObject = modelClass.__call__()
    logger.Info("<<< 15")
  }
}
