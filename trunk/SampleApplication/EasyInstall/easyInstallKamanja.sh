#!/bin/bash

set -e

installPath=$1
srcPath=$2
ivyPath=$3
KafkaRootDir=$4
ver210=1.4.0_2.10
ver211=1.4.0_2.11

if [ ! -d "$installPath" ]; then
        echo "Not valid install path supplied.  It should be a directory that can be written to and whose current content is of no value (will be overwritten) "
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

if [ ! -d "$srcPath" ]; then
        echo "Not valid src path supplied.  It should be the trunk directory containing the jars, files, what not that need to be supplied."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

if [ ! -d "$ivyPath" ]; then
        echo "Not valid ivy path supplied.  It should be the ivy path for dependency the jars."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

if [ ! -d "$KafkaRootDir" ]; then
        echo "Not valid Kafka path supplied."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

migration2_10libsCopiesFor2_11="false"

installPath=$(echo $installPath | sed 's/[\/]*$//')
srcPath=$(echo $srcPath | sed 's/[\/]*$//')
ivyPath=$(echo $ivyPath | sed 's/[\/]*$//')

# *******************************
# Clean out prior installation
# *******************************
rm -Rf $installPath

# *******************************
# Make the directories as needed for version-2.10
# *******************************
mkdir -p $installPath/Kamanja-$ver210/bin
#mkdir -p $installPath/Kamanja-$ver210/lib
mkdir -p $installPath/Kamanja-$ver210/lib/system
mkdir -p $installPath/Kamanja-$ver210/lib/application
mkdir -p $installPath/Kamanja-$ver210/storage
mkdir -p $installPath/Kamanja-$ver210/logs
mkdir -p $installPath/Kamanja-$ver210/config
mkdir -p $installPath/Kamanja-$ver210/documentation
mkdir -p $installPath/Kamanja-$ver210/output
mkdir -p $installPath/Kamanja-$ver210/workingdir
#mkdir -p $installPath/Kamanja-$ver210/template
mkdir -p $installPath/Kamanja-$ver210/template/config
mkdir -p $installPath/Kamanja-$ver210/template/script
#mkdir -p $installPath/Kamanja-$ver210/input/
#new one
#mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/bin
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/data
#mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/config
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/container
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/function
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/message
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/model
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/script
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/metadata/type
mkdir -p $installPath/Kamanja-$ver210/input/SampleApplications/template
#new one



# *******************************
# Make the directories as needed for version-2.11
# *******************************
mkdir -p $installPath/Kamanja-$ver211/bin
#mkdir -p $installPath/Kamanja-$ver211/lib
mkdir -p $installPath/Kamanja-$ver211/lib/system
mkdir -p $installPath/Kamanja-$ver211/lib/application
mkdir -p $installPath/Kamanja-$ver211/storage
mkdir -p $installPath/Kamanja-$ver211/logs
mkdir -p $installPath/Kamanja-$ver211/config
mkdir -p $installPath/Kamanja-$ver211/documentation
mkdir -p $installPath/Kamanja-$ver211/output
mkdir -p $installPath/Kamanja-$ver211/workingdir
#mkdir -p $installPath/Kamanja-$ver211/template
mkdir -p $installPath/Kamanja-$ver211/template/config
mkdir -p $installPath/Kamanja-$ver211/template/script
#mkdir -p $installPath/Kamanja-$ver211/input
#new one
#mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/bin
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/data
#mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/config
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/container
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/function
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/message
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/model
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/script
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/metadata/type
mkdir -p $installPath/Kamanja-$ver211/input/SampleApplications/template
#new one


#************************************************************
# Making directories for Kamanja InstallMigrationAndCluster
#************************************************************
# *******************************
# Make the directories as needed for version-2.10
# *******************************
mkdir -p $installPath/KamanjaInstall-$ver210/bin
mkdir -p $installPath/KamanjaInstall-$ver210/lib/system
mkdir -p $installPath/KamanjaInstall-$ver210/lib/application
mkdir -p $installPath/KamanjaInstall-$ver210/logs
mkdir -p $installPath/KamanjaInstall-$ver210/config
mkdir -p $installPath/KamanjaInstall-$ver210/template/config
mkdir -p $installPath/KamanjaInstall-$ver210/template/script

# *******************************
# Make the directories as needed for version-2.11
# *******************************
mkdir -p $installPath/KamanjaInstall-$ver211/bin
mkdir -p $installPath/KamanjaInstall-$ver211/lib/system
mkdir -p $installPath/KamanjaInstall-$ver211/lib/application
mkdir -p $installPath/KamanjaInstall-$ver211/logs
mkdir -p $installPath/KamanjaInstall-$ver211/config
mkdir -p $installPath/KamanjaInstall-$ver211/template/config
mkdir -p $installPath/KamanjaInstall-$ver211/template/script

kamanjainstallbin=$installPath/KamanjaInstall-$ver210/bin
kamanjainstallsystemlib=$installPath/KamanjaInstall-$ver210/lib/system
kamanjainstallapplib=$installPath/KamanjaInstall-$ver210/lib/application
kamanjainstallconfig=$installPath/KamanjaInstall-$ver210/config


# *******************************
# Build fat-jars for version-2.10
# *******************************

bin=$installPath/Kamanja-$ver210/bin
systemlib=$installPath/Kamanja-$ver210/lib/system
applib=$installPath/Kamanja-$ver210/lib/application

echo $installPath
echo $srcPath
echo $bin

echo "clean, package and assemble $srcPath ..."

cd $srcPath/

sbt clean '++ 2.10.4 package' '++ 2.10.4 ExtDependencyLibs/assembly' '++ 2.10.4 ExtDependencyLibs2/assembly' '++ 2.10.4 KamanjaInternalDeps/assembly'
sbt '++ 2.10.4 MigrateManager/assembly' '++ 2.10.4 ClusterInstallerDriver/assembly' '++ 2.10.4 InstallDriver/assembly' '++ 2.10.4 GetComponent/assembly' '++ 2.10.4 NodeInfoExtract/assembly'

#sbt clean '++ 2.10.4 package' '++ 2.10.4 KamanjaManager/assembly' '++ 2.10.4 MetadataAPI/assembly' '++ 2.10.4 KVInit/assembly' '++ 2.10.4 SimpleKafkaProducer/assembly'
#sbt '++ 2.10.4 NodeInfoExtract/assembly' '++ 2.10.4 MetadataAPIService/assembly' '++ 2.10.4 JdbcDataCollector/assembly'
#sbt '++ 2.10.4 FileDataConsumer/assembly' '++ 2.10.4 CleanUtil/assembly' '++ 2.10.4 MigrateManager/assembly' '++ 2.10.4 ClusterInstallerDriver/assembly' '++ 2.10.4 InstallDriver/assembly' '++ 2.10.4 GetComponent/assembly' '++ 2.10.4 PmmlTestTool/assembly' '++ 2.10.4 ExtDependencyLibs/assembly' '++ 2.10.4 ExtDependencyLibs2/assembly' '++ 2.10.4 KamanjaInternalDeps/assembly'
# sbt '++ 2.10.4 MethodExtractor/assembly' '++ 2.10.4 SaveContainerDataComponent/assembly' '++ 2.10.4 ExtractData/assembly'

# recreate eclipse projects
#echo "refresh the eclipse projects ..."
#cd $srcPath
#sbt eclipse

# Move them into place
echo "copy the fat jars to $installPath ..."

cd $srcPath
cp Utils/KVInit/target/scala-2.10/kvinit* $systemlib
cp MetadataAPI/target/scala-2.10/metadataapi* $systemlib
cp KamanjaManager/target/scala-2.10/kamanjamanager* $systemlib
# cp Pmml/MethodExtractor/target/scala-2.10/methodextractor* $bin
cp Utils/SimpleKafkaProducer/target/scala-2.10/simplekafkaproducer* $systemlib
# cp Utils/ExtractData/target/scala-2.10/extractdata* $bin
cp Utils/JdbcDataCollector/target/scala-2.10/jdbcdatacollector* $systemlib
cp MetadataAPIService/target/scala-2.10/metadataapiservice* $systemlib
cp FileDataConsumer/target/scala-2.10/filedataconsumer* $systemlib
cp Utils/CleanUtil/target/scala-2.10/cleanutil* $systemlib
cp Utils/ClusterInstaller/ClusterInstallerDriver/target/ClusterInstallerDriver* $kamanjainstallbin
cp Utils/ClusterInstaller/InstallDriver/target/scala-2.10/InstallDriver* $kamanjainstallbin
cp Utils/ClusterInstaller/GetComponent/target/scala-2.10/GetComponent* $kamanjainstallbin
cp Utils/ClusterInstaller/InstallDriver/src/main/resources/GetComponentsVersions.sh $kamanjainstallbin
cp Utils/PmmlTestTool/target/pmmltesttool* $systemlib
#cp Utils/KVInit/target/scala-2.10/KVInit* $systemlib
#cp MetadataAPI/target/scala-2.10/MetadataAPI* $systemlib
#cp KamanjaManager/target/scala-2.10/KamanjaManager* $systemlib
## cp Pmml/MethodExtractor/target/scala-2.10/MethodExtractor* $bin
#cp Utils/SimpleKafkaProducer/target/scala-2.10/SimpleKafkaProducer* $systemlib
## cp Utils/ExtractData/target/scala-2.10/ExtractData* $bin
#cp Utils/JdbcDataCollector/target/scala-2.10/JdbcDataCollector* $systemlib
#cp MetadataAPIService/target/scala-2.10/MetadataAPIService* $systemlib
#cp FileDataConsumer/target/scala-2.10/FileDataConsumer* $systemlib
#cp Utils/CleanUtil/target/scala-2.10/CleanUtil* $systemlib
#cp Utils/ClusterInstaller/ClusterInstallerDriver/target/ClusterInstallerDriver* $kamanjainstallbin
#cp Utils/ClusterInstaller/InstallDriver/target/scala-2.10/InstallDriver* $kamanjainstallbin
#cp Utils/ClusterInstaller/GetComponent/target/scala-2.10/GetComponent* $kamanjainstallbin
#cp Utils/ClusterInstaller/InstallDriver/src/main/resources/GetComponentsVersions.sh $kamanjainstallbin
#cp Utils/PmmlTestTool/target/PmmlTestTool* $systemlib
# only for 2.11 ?
cp Utils/Migrate/MigrateManager/target/MigrateManager* $bin

# copy fat jars to KamanjaInstall
cp Utils/Migrate/MigrateManager/target/MigrateManager* $kamanjainstallbin
cp $srcPath/Utils/NodeInfoExtract/target/scala-2.10/NodeInfoExtract* $kamanjainstallbin

# copy jars used to reduce package size
cp ExtDependencyLibs/target/scala-2.10/ExtDependencyLibs_2.10-1.4.0.jar $systemlib
cp ExtDependencyLibs2/target/scala-2.10/ExtDependencyLibs2_2.10-1.4.0.jar $systemlib
cp KamanjaInternalDeps/target/scala-2.10/KamanjaInternalDeps_2.10-1.4.0.jar $systemlib

# *******************************
# Copy jars required for version-2.10 (more than required if the fat jars are used)
# *******************************

# Base Types and Functions, InputOutput adapters, and original versions of things
#echo "copy all Kamanja jars and the jars upon which they depend to the $systemlib"
#
## -------------------- generated cp commands --------------------
#
#cp $srcPath/lib_managed/bundles/org.apache.directory.api/api-util/api-util-1.0.0-M20.jar $systemlib
#cp $ivyPath/cache/org.codehaus.jackson/jackson-xc/jars/jackson-xc-1.8.3.jar $systemlib
#cp $ivyPath/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-0.8.2.2.jar $systemlib
#cp $ivyPath/cache/org.xerial.snappy/snappy-java/bundles/snappy-java-1.0.4.1.jar $systemlib
#cp $ivyPath/cache/javax.xml.bind/jaxb-api/jars/jaxb-api-2.2.2.jar $systemlib
#cp $ivyPath/cache/log4j/log4j/bundles/log4j-1.2.16.jar $systemlib
#cp $ivyPath/cache/com.sun.jersey/jersey-core/bundles/jersey-core-1.9.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.codehaus.jettison/jettison/jettison-1.1.jar $systemlib
#cp $ivyPath/cache/org.jruby.jcodings/jcodings/jars/jcodings-1.0.8.jar $systemlib
#cp $ivyPath/cache/org.scalatest/scalatest_2.10/bundles/scalatest*.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-client/bundles/curator-client-2.7.1.jar $systemlib
#cp $ivyPath/cache/commons-digester/commons-digester/jars/commons-digester-1.8.jar $systemlib
#cp $ivyPath/cache/org.jruby.joni/joni/jars/joni-2.1.2.jar $systemlib
#cp $ivyPath/cache/org.apache.directory.api/api-util/bundles/api-util-1.0.0-M20.jar $systemlib
#cp $ivyPath/cache/org.mortbay.jetty/jetty-util/jars/jetty-util-6.1.26.jar $systemlib
#cp $srcPath/InputOutputAdapters/KafkaSimpleInputOutputAdapters/target/scala-2.10/kafkasimpleinputoutputadapters*.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.commons/commons-lang3/commons-lang3-3.1.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-collections4/jars/commons-collections4-4.0.jar $systemlib
#cp $srcPath/Storage/SqlServer/target/scala-2.10/sqlserver*.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.commons/commons-compress/commons-compress-1.4.1.jar $systemlib
#cp $ivyPath/cache/org.apache.logging.log4j/log4j-api/jars/log4j-api-2.4.1.jar $systemlib
#cp $ivyPath/cache/org.apache.logging.log4j/log4j-1.2-api/bundles/log4j-1.2-api-2.4.1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hadoop/hadoop-auth/hadoop-auth-2.7.1.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-lang/commons-lang/commons-lang-2.6.jar $systemlib
#cp $srcPath/lib_managed/jars/com.google.code.gson/gson/gson-2.2.4.jar $systemlib
#cp $ivyPath/cache/org.jpmml/pmml-schema/jars/pmml-schema-1.2.9.jar $systemlib
#cp $ivyPath/cache/org.javassist/javassist/bundles/javassist-3.18.1-GA.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.logging.log4j/log4j-api/log4j-api-2.4.1.jar $systemlib
#cp $ivyPath/cache/com.typesafe/config/bundles/config-1.2.1.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.curator/curator-framework/curator-framework-2.7.1.jar $systemlib
#cp $ivyPath/cache/org.json4s/json4s-jackson_2.10/jars/json4s-jackson_2.10-3.2.9.jar $systemlib
#cp $ivyPath/cache/commons-net/commons-net/jars/commons-net-3.1.jar $systemlib
#cp $ivyPath/cache/org.apache.hadoop/hadoop-annotations/jars/hadoop-annotations-2.7.1.jar $systemlib
#cp $ivyPath/cache/com.101tec/zkclient/jars/zkclient-0.3.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-math3/jars/commons-math3-3.6.jar $systemlib
#cp $ivyPath/cache/org.apache.camel/camel-core/bundles/camel-core-2.9.2.jar $systemlib
#cp $ivyPath/cache/com.google.code.findbugs/jsr305/jars/jsr305-3.0.0.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-cli/commons-cli/commons-cli-1.2.jar $systemlib
#cp $srcPath/lib_managed/jars/com.jamesmurty.utils/java-xmlbuilder/java-xmlbuilder-0.4.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.fasterxml.jackson.core/jackson-annotations/jackson-annotations-2.3.0.jar $systemlib
#cp $ivyPath/cache/com.pyruby/java-stub-server/jars/java-stub-server-0.12-sources.jar $systemlib
#cp $ivyPath/cache/com.esotericsoftware.reflectasm/reflectasm/jars/reflectasm-1.07-shaded.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-client_2.10/bundles/spray-client_2.10-1.3.3.jar $systemlib
#cp $srcPath/lib_managed/jars/javax.servlet/servlet-api/servlet-api-2.5.jar $systemlib
#cp $srcPath/Utils/Audit/target/scala-2.10/auditadapters*.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.sun.jersey/jersey-server/jersey-server-1.9.jar $systemlib
#cp $srcPath/lib_managed/jars/org.json4s/json4s-native_2.10/json4s-native_2.10-3.2.9.jar $systemlib
#cp $ivyPath/cache/org.apache.directory.server/apacheds-i18n/bundles/apacheds-i18n-2.0.0-M15.jar $systemlib
#cp $srcPath/FactoriesOfModelInstanceFactory/JarFactoryOfModelInstanceFactory/target/scala-2.10/jarfactoryofmodelinstancefactory*.jar $systemlib
#cp $ivyPath/cache/org.jpmml/pmml-evaluator/jars/pmml-evaluator-1.2.9.jar $systemlib
#cp $ivyPath/cache/commons-httpclient/commons-httpclient/jars/commons-httpclient-3.1.jar $systemlib
#cp $srcPath/TransactionService/target/scala-2.10/transactionservice*.jar $systemlib
#cp $ivyPath/cache/io.netty/netty-all/jars/netty-all-4.0.23.Final.jar $systemlib
#cp $ivyPath/cache/com.esotericsoftware.kryo/kryo/bundles/kryo-2.21.jar $systemlib
## might be only for 2.11
##cp $srcPath/lib_managed/jars/org.scalameta/tokens_2.11/tokens_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/com.ning/compress-lzf/bundles/compress-lzf-0.9.1.jar $systemlib
#cp $ivyPath/cache/org.scala-lang/scala-actors/jars/scala-actors-2.10.4.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.directory.server/apacheds-i18n/apacheds-i18n-2.0.0-M15.jar $systemlib
#cp $srcPath/Utils/Controller/target/scala-2.10/controller*.jar $systemlib
#cp $ivyPath/cache/org.slf4j/slf4j-log4j12/jars/slf4j-log4j12-1.6.1.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.fasterxml.jackson.core/jackson-databind/jackson-databind-2.3.1.jar $systemlib
#cp $srcPath/lib_managed/jars/javax.activation/activation/activation-1.1.jar $systemlib
## cp $srcPath/Utils/ExtractData/target/scala-2.10/extractdata*.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.xerial.snappy/snappy-java/snappy-java-1.0.4.1.jar $systemlib
#cp $srcPath/lib_managed/jars/joda-time/joda-time/joda-time-2.9.1.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-io_2.10/bundles/spray-io_2.10-1.3.3.jar $systemlib
#cp $ivyPath/cache/com.typesafe.akka/akka-actor_2.10/jars/akka-actor_2.10-2.3.2.jar $systemlib
#cp $ivyPath/cache/com.typesafe.akka/akka-actor_2.10/jars/akka-actor_2.10-2.3.9.jar $systemlib
#cp $ivyPath/cache/uk.co.bigbeeconsultants/bee-client_2.10/jars/bee-client_2.10-0.28.0.jar $systemlib
## might be only for 2.11
##cp $srcPath/lib_managed/jars/org.scalameta/foundation_2.11/foundation_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/org.apache.hbase/hbase-protocol/jars/hbase-protocol-1.0.2.jar $systemlib
## might be only for 2.11
##cp $ivyPath/cache/org.scala-lang.modules/scala-parser-combinators_2.11/bundles/scala-parser-combinators_2.11-1.0.2.jar $systemlib
#cp $srcPath/lib_managed/jars/javax.xml.bind/jaxb-api/jaxb-api-2.2.2.jar $systemlib
#cp $ivyPath/cache/org.jdom/jdom/jars/jdom-1.1.jar $systemlib
#cp $srcPath/KvBase/target/scala-2.10/kvbase*.jar $systemlib
#cp $ivyPath/cache/com.googlecode.json-simple/json-simple/jars/json-simple-1.1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hbase/hbase-client/hbase-client-1.0.2.jar $systemlib
#cp $ivyPath/cache/org.apache.hbase/hbase-client/jars/hbase-client-1.0.2.jar $systemlib
#cp $ivyPath/cache/ch.qos.logback/logback-classic/jars/logback-classic-1.0.13.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hadoop/hadoop-annotations/hadoop-annotations-2.7.1.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-util_2.10/bundles/spray-util_2.10-1.3.3.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.commons/commons-collections4/commons-collections4-4.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.zookeeper/zookeeper/zookeeper-3.4.6.jar $systemlib
#cp $ivyPath/cache/com.twitter/chill-java/jars/chill-java-0.5.0.jar $systemlib
#cp $ivyPath/cache/asm/asm/jars/asm-3.1.jar $systemlib
## might be only for 2.11
##cp $ivyPath/cache/org.scalameta/quasiquotes_2.11/jars/quasiquotes_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-recipes/bundles/curator-recipes-2.7.1.jar $systemlib
#cp $srcPath/MetadataAPI/target/scala-2.10/metadataapi*.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-math/jars/commons-math-2.2.jar $systemlib
#cp $ivyPath/cache/com.sun.xml.bind/jaxb-impl/jars/jaxb-impl-2.2.3-1.jar $systemlib
## might be missing in version 2.10
##cp $srcPath/lib_managed/jars/org.scalameta/dialects_2.11/dialects_2.11-0.0.3.jar $systemlib
## might be missing in version 2.10
##cp $srcPath/lib_managed/jars/org.scalameta/parsers_2.11/parsers_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/bundles/io.netty/netty/netty-3.9.0.Final.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.commons/commons-math3/commons-math3-3.6.jar $systemlib
#cp $srcPath/lib_managed/jars/com.github.stephenc.findbugs/findbugs-annotations/findbugs-annotations-1.3.9-1.jar $systemlib
#cp $ivyPath/cache/com.fasterxml.jackson.core/jackson-databind/bundles/jackson-databind-2.3.1.jar $systemlib
#cp $ivyPath/cache/com.fasterxml.jackson.core/jackson-annotations/bundles/jackson-annotations-2.3.0.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-dbcp2/jars/commons-dbcp2-2.1.jar $systemlib
#cp $srcPath/lib_managed/jars/com.twitter/chill_2.10/chill_2.10-0.5.0.jar $systemlib
#cp $ivyPath/cache/org.json4s/json4s-native_2.10/jars/json4s-native_2.10-3.2.9.jar $systemlib
#cp $ivyPath/cache/junit/junit/jars/junit-3.8.1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.mortbay.jetty/jetty-util/jetty-util-6.1.26.jar $systemlib
#cp $ivyPath/cache/com.typesafe/config/bundles/config-1.2.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hadoop/hadoop-common/hadoop-common-2.7.1.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-digester/commons-digester/commons-digester-1.8.1.jar $systemlib
#cp $ivyPath/cache/commons-dbcp/commons-dbcp/jars/commons-dbcp-1.4.jar $systemlib
#cp $srcPath/lib_managed/jars/org.scala-lang/scala-actors/scala-actors-2.10.4.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.logging.log4j/log4j-core/log4j-core-2.4.1.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-framework/bundles/curator-framework-2.7.1.jar $systemlib
#cp $srcPath/lib_managed/bundles/log4j/log4j/log4j-1.2.17.jar $systemlib
#cp $srcPath/lib_managed/jars/xmlenc/xmlenc/xmlenc-0.52.jar $systemlib
#cp $ivyPath/cache/org.mortbay.jetty/jetty-embedded/jars/jetty-embedded-6.1.26-sources.jar $systemlib
#cp $ivyPath/cache/org.apache.httpcomponents/httpclient/jars/httpclient-4.2.5.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.directory.api/api-asn1-api/api-asn1-api-1.0.0-M20.jar $systemlib
#cp $ivyPath/cache/net.jpountz.lz4/lz4/jars/lz4-1.2.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.scala-lang/scala-reflect/scala-reflect-2.10.4.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.commons/commons-dbcp2/commons-dbcp2-2.1.jar $systemlib
## might be only for 2.11
##cp $ivyPath/cache/org.scala-lang.modules/scala-parser-combinators_2.11/bundles/scala-parser-combinators_2.11-1.0.4.jar $systemlib
#cp $srcPath/lib_managed/jars/org.jpmml/pmml-agent/pmml-agent-1.2.9.jar $systemlib
#cp $ivyPath/cache/commons-logging/commons-logging/jars/commons-logging-1.1.1.jar $systemlib
#cp $ivyPath/cache/net.java.dev.jets3t/jets3t/jars/jets3t-0.9.0.jar $systemlib
#cp $ivyPath/cache/org.scalatest/scalatest_2.10/bundles/scalatest*.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.sun.jersey/jersey-json/jersey-json-1.9.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-can_2.10/bundles/spray-can_2.10-1.3.3.jar $systemlib
#cp $ivyPath/cache/commons-logging/commons-logging/jars/commons-logging-1.2.jar $systemlib
#cp $srcPath/Storage/TreeMap/target/scala-2.10/treemap*.jar $systemlib
#cp $srcPath/Utils/KVInit/target/scala-2.10/kvinit*.jar $systemlib
#cp $srcPath/Utils/Serialize/target/scala-2.10/serialize*.jar $systemlib
#cp $ivyPath/cache/org.apache.htrace/htrace-core/jars/htrace-core-3.1.0-incubating.jar $systemlib
#cp $srcPath/Storage/StorageBase/target/scala-2.10/storagebase*.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-json_2.10/bundles/spray-json_2.10-1.3.2.jar $systemlib
#cp $ivyPath/cache/commons-codec/commons-codec/jars/commons-codec-1.9.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.avro/avro/avro-1.7.4.jar $systemlib
#cp $ivyPath/cache/org.joda/joda-convert/jars/joda-convert-1.7.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.codahale.metrics/metrics-core/metrics-core-3.0.2.jar $systemlib
#cp $ivyPath/cache/com.esotericsoftware.minlog/minlog/jars/minlog-1.2.jar $systemlib
#cp $ivyPath/cache/org.mortbay.jetty/servlet-api/jars/servlet-api-2.5.20110712-sources.jar $systemlib
#cp $ivyPath/cache/com.google.collections/google-collections/jars/google-collections-1.0.jar $systemlib
#cp $ivyPath/cache/ch.qos.logback/logback-core/jars/logback-core-1.0.12.jar $systemlib
## Might be only for 2.11
##cp $ivyPath/cache/org.scala-lang.modules/scala-parser-combinators_2.11/bundles/scala-parser-combinators_2.11-1.0.1.jar $systemlib
#cp $ivyPath/cache/io.netty/netty/bundles/netty-3.7.0.Final.jar $systemlib
## Might be only for 2.11
##cp $ivyPath/cache/org.scalameta/parsers_2.11/jars/parsers_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/com.jamesmurty.utils/java-xmlbuilder/jars/java-xmlbuilder-0.4.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.fasterxml.jackson.core/jackson-core/jackson-core-2.3.1.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.datastax.cassandra/cassandra-driver-core/cassandra-driver-core-2.1.2.jar $systemlib
#cp $srcPath/MetadataAPIService/target/scala-2.10/metadataapiservice*.jar $systemlib
## Might be only for 2.11
##cp $srcPath/lib_managed/jars/org.scalameta/exceptions_2.11/exceptions_2.11-0.0.3.jar $systemlib
#cp $srcPath/Utils/ZooKeeper/CuratorClient/target/scala-2.10/zookeeperclient*.jar $systemlib
#cp $srcPath/Metadata/target/scala-2.10/metadata*.jar $systemlib
## Might be only for 2.11
##cp $ivyPath/cache/org.scalameta/exceptions_2.11/jars/exceptions_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/commons-pool/commons-pool/jars/commons-pool-1.5.4.jar $systemlib
#cp $ivyPath/cache/org.parboiled/parboiled-scala_2.10/jars/parboiled-scala_2.10-1.1.7.jar $systemlib
#cp $ivyPath/cache/org.parboiled/parboiled-core/jars/parboiled-core-1.1.7.jar $systemlib
#cp $srcPath/lib_managed/jars/io.netty/netty-all/netty-all-4.0.23.Final.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.htrace/htrace-core/htrace-core-3.1.0-incubating.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.curator/curator-client/curator-client-2.7.1.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-compress/jars/commons-compress-1.4.1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.codehaus.jackson/jackson-xc/jackson-xc-1.8.3.jar $systemlib
#cp $srcPath/lib_managed/jars/jline/jline/jline-0.9.94.jar $systemlib
#cp $ivyPath/cache/commons-beanutils/commons-beanutils/jars/commons-beanutils-1.7.0.jar $systemlib
#cp $ivyPath/cache/org.apache.avro/avro/jars/avro-1.7.4.jar $systemlib
#cp $ivyPath/cache/ch.qos.logback/logback-core/jars/logback-core-1.0.13.jar $systemlib
#cp $srcPath/KamanjaBase/target/scala-2.10/kamanjabase*.jar $systemlib
#cp $ivyPath/cache/com.typesafe.akka/akka-testkit_2.10/jars/akka-testkit_2.10-2.3.9.jar $systemlib
#cp $ivyPath/cache/commons-configuration/commons-configuration/jars/commons-configuration-1.7.jar $systemlib
#cp $ivyPath/cache/commons-beanutils/commons-beanutils-core/jars/commons-beanutils-core-1.8.0.jar $systemlib
#cp $srcPath/Utils/CleanUtil/target/scala-2.10/cleanutil*.jar $systemlib
#cp $ivyPath/cache/com.jcraft/jsch/jars/jsch-0.1.42.jar $systemlib
#cp $srcPath/lib_managed/jars/org.codehaus.jackson/jackson-core-asl/jackson-core-asl-1.9.13.jar $systemlib
#cp $ivyPath/cache/com.google.code.gson/gson/jars/gson-2.3.1.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-routing_2.10/bundles/spray-routing_2.10-1.3.3.jar $systemlib
#cp $ivyPath/cache/org.apache.kafka/kafka_2.10/jars/kafka_2.10-0.8.2.2.jar $systemlib
#cp $ivyPath/cache/com.chuusai/shapeless_2.10/jars/shapeless_2.10-1.2.4.jar $systemlib
#cp $ivyPath/cache/org.apache.directory.api/api-asn1-api/bundles/api-asn1-api-1.0.0-M20.jar $systemlib
#cp $ivyPath/cache/com.google.code.findbugs/jsr305/jars/jsr305-1.3.9.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-logging/commons-logging/commons-logging-1.2.jar $systemlib
#cp $srcPath/lib_managed/jars/org.ow2.asm/asm/asm-4.0.jar $systemlib
#cp $ivyPath/cache/org.apache.thrift/libthrift/jars/libthrift-0.9.2.jar $systemlib
#cp $srcPath/KamanjaUtils/target/scala-2.10/kamanjautils*.jar $systemlib
#cp $srcPath/lib_managed/jars/com.esotericsoftware.reflectasm/reflectasm/reflectasm-1.07-shaded.jar $systemlib
#cp $srcPath/Utils/ZooKeeper/CuratorListener/target/scala-2.10/zookeeperlistener*.jar $systemlib
#cp $srcPath/InputOutputAdapters/InputOutputAdapterBase/target/scala-2.10/inputoutputadapterbase*.jar $systemlib
#cp $ivyPath/cache/org.scala-lang/scala-reflect/jars/scala-reflect-2.10.4.jar $systemlib
#cp $ivyPath/cache/org.jpmml/pmml-agent/jars/pmml-agent-1.2.9.jar $systemlib
#cp $ivyPath/cache/org.json4s/json4s-core_2.10/jars/json4s-core_2.10-3.2.9.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-configuration/commons-configuration/commons-configuration-1.7.jar $systemlib
#cp $ivyPath/cache/org.codehaus.jackson/jackson-mapper-asl/jars/jackson-mapper-asl-1.9.13.jar $systemlib
#cp $ivyPath/cache/org.hamcrest/hamcrest-core/jars/hamcrest-core-1.3.jar $systemlib
#cp $ivyPath/cache/org.joda/joda-convert/jars/joda-convert-1.6.jar $systemlib
#cp $ivyPath/cache/org.scala-lang/scala-library/jars/scala-library-2.10.4.jar $systemlib
#cp $ivyPath/cache/commons-collections/commons-collections/jars/commons-collections-3.2.1.jar $systemlib
## Might be only for 2.11
##cp $ivyPath/cache/org.scalameta/tokenizers_2.11/jars/tokenizers_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/jars/org.slf4j/slf4j-api/slf4j-api-1.7.10.jar $systemlib
#cp $ivyPath/cache/org.apache.zookeeper/zookeeper/jars/zookeeper-3.4.6.jar $systemlib
## Might be only for 2.11
##cp $ivyPath/cache/org.scalameta/foundation_2.11/jars/foundation_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.curator/curator-recipes/curator-recipes-2.7.1.jar $systemlib
#cp $ivyPath/cache/org.objenesis/objenesis/jars/objenesis-1.2.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.scalatest/scalatest_2.10/scalatest*.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-net/commons-net/commons-net-3.1.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-httpclient/commons-httpclient/commons-httpclient-3.1.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.directory.server/apacheds-kerberos-codec/apacheds-kerberos-codec-2.0.0-M15.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.esotericsoftware.kryo/kryo/kryo-2.21.jar $systemlib
#cp $srcPath/lib_managed/jars/org.scala-lang/scala-compiler/scala-compiler-2.10.4.jar $systemlib
## Might be only for 2.11
##cp $ivyPath/cache/org.scala-lang.modules/scala-xml_2.11/bundles/scala-xml_2.11-1.0.4.jar $systemlib
#cp $srcPath/Utils/JsonDataGen/target/scala-2.10/jsondatagen*.jar $systemlib
#cp $ivyPath/cache/org.apache.httpcomponents/httpclient/jars/httpclient-4.1.2.jar $systemlib
#cp $srcPath/lib_managed/jars/org.jruby.jcodings/jcodings/jcodings-1.0.8.jar $systemlib
#cp $ivyPath/cache/ch.qos.logback/logback-classic/jars/logback-classic-1.0.12.jar $systemlib
#cp $srcPath/FactoriesOfModelInstanceFactory/JpmmlFactoryOfModelInstanceFactory/target/scala-2.10/jpmmlfactoryofmodelinstancefactory*.jar $systemlib
#cp $ivyPath/cache/org.apache.directory.server/apacheds-kerberos-codec/bundles/apacheds-kerberos-codec-2.0.0-M15.jar $systemlib
#cp $ivyPath/cache/io.netty/netty/bundles/netty-3.9.0.Final.jar $systemlib
## Might be only for 2.11
##cp $ivyPath/cache/org.scalameta/tokens_2.11/jars/tokens_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/org.slf4j/slf4j-log4j12/jars/slf4j-log4j12-1.7.10.jar $systemlib
#cp $srcPath/OutputMsgDef/target/scala-2.10/outputmsgdef*.jar $systemlib
#cp $ivyPath/cache/org.jpmml/pmml-model/jars/pmml-model-1.2.9.jar $systemlib
#cp $ivyPath/cache/org.apache.httpcomponents/httpcore/jars/httpcore-4.2.4.jar $systemlib
#cp $ivyPath/cache/commons-configuration/commons-configuration/jars/commons-configuration-1.6.jar $systemlib
#cp $ivyPath/cache/log4j/log4j/bundles/log4j-1.2.17.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-test/jars/curator-test-2.8.0.jar $systemlib
#cp $srcPath/Storage/StorageManager/target/scala-2.10/storagemanager*.jar $systemlib
#cp $srcPath/lib_managed/jars/org.jpmml/pmml-evaluator/pmml-evaluator-1.2.9.jar $systemlib
#cp $srcPath/lib_managed/jars/com.jcraft/jsch/jsch-0.1.42.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-recipes/bundles/curator-recipes-2.6.0.jar $systemlib
## Might be only for 2.11
##cp $srcPath/lib_managed/bundles/org.scala-lang.modules/scala-parser-combinators_2.11/scala-parser-combinators_2.11-1.0.4.jar $systemlib
#cp $ivyPath/cache/org.ow2.asm/asm/jars/asm-4.0.jar $systemlib
#cp $ivyPath/cache/org.apache.hbase/hbase-annotations/jars/hbase-annotations-1.0.2.jar $systemlib
#cp $ivyPath/cache/com.google.protobuf/protobuf-java/bundles/protobuf-java-2.6.0.jar $systemlib
#cp $ivyPath/cache/org.tukaani/xz/jars/xz-1.0.jar $systemlib
#cp $ivyPath/cache/org.codehaus.jackson/jackson-jaxrs/jars/jackson-jaxrs-1.8.3.jar $systemlib
#cp $srcPath/SampleApplication/InterfacesSamples/target/scala-2.10/interfacessamples*.jar $systemlib
#cp $srcPath/lib_managed/jars/junit/junit/junit-4.12.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-http_2.10/bundles/spray-http_2.10-1.3.3.jar $systemlib
#cp $ivyPath/cache/org.xerial.snappy/snappy-java/bundles/snappy-java-1.1.1.7.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.httpcomponents/httpcore/httpcore-4.2.4.jar $systemlib
#cp $ivyPath/cache/org.jvnet.mimepull/mimepull/jars/mimepull-1.9.5.jar $systemlib
#cp $ivyPath/cache/org.mortbay.jetty/servlet-api/jars/servlet-api-2.5.20110712.jar $systemlib
#cp $ivyPath/cache/org.apache.logging.log4j/log4j-core/jars/log4j-core-2.4.1.jar $systemlib
#cp $ivyPath/cache/org.mortbay.jetty/jetty/jars/jetty-6.1.26.jar $systemlib
#cp $ivyPath/cache/javax.activation/activation/jars/activation-1.1.jar $systemlib
#cp $ivyPath/cache/com.sdicons.jsontools/jsontools-core/jars/jsontools-core-1.7-sources.jar $systemlib
#cp $srcPath/HeartBeat/target/scala-2.10/heartbeat*.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hbase/hbase-protocol/hbase-protocol-1.0.2.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-beanutils/commons-beanutils/commons-beanutils-1.8.3.jar $systemlib
## Might be only for 2.11
##cp $ivyPath/cache/org.scalameta/dialects_2.11/jars/dialects_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/jars/com.esotericsoftware.minlog/minlog/minlog-1.2.jar $systemlib
#cp $ivyPath/cache/antlr/antlr/jars/antlr-2.7.7.jar $systemlib
#cp $srcPath/MetadataAPIServiceClient/target/scala-2.10/metadataapiserviceclient*.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-framework/bundles/curator-framework-2.6.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.scala-lang/scala-library/scala-library-2.10.4.jar $systemlib
## cp $srcPath/Utils/SaveContainerDataComponent/target/scala-2.10/savecontainerdatacomponent*.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-client/bundles/curator-client-2.6.0.jar $systemlib
#cp $srcPath/Storage/HashMap/target/scala-2.10/hashmap*.jar $systemlib
#cp $srcPath/InputOutputAdapters/FileSimpleInputOutputAdapters/target/scala-2.10/filesimpleinputoutputadapters*.jar $systemlib
#cp $ivyPath/cache/javax.servlet.jsp/jsp-api/jars/jsp-api-2.1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.cassandra/cassandra-thrift/cassandra-thrift-2.0.3.jar $systemlib
#cp $ivyPath/cache/com.sun.jersey/jersey-json/bundles/jersey-json-1.9.jar $systemlib
#cp $srcPath/KamanjaManager/target/scala-2.10/kamanjamanager*.jar $systemlib
#cp $ivyPath/cache/org.ow2.asm/asm-tree/jars/asm-tree-4.0.jar $systemlib
#cp $ivyPath/cache/commons-io/commons-io/jars/commons-io-2.4.jar $systemlib
#cp $srcPath/lib_managed/jars/org.joda/joda-convert/joda-convert-1.6.jar $systemlib
#cp $ivyPath/cache/net.sf.jopt-simple/jopt-simple/jars/jopt-simple-3.2.jar $systemlib
#cp $ivyPath/cache/com.github.stephenc.findbugs/findbugs-annotations/jars/findbugs-annotations-1.3.9-1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.json4s/json4s-ast_2.10/json4s-ast_2.10-3.2.9.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.thrift/libthrift/libthrift-0.9.2.jar $systemlib
#cp $srcPath/Exceptions/target/scala-2.10/exceptions*.jar $systemlib
#cp $srcPath/EnvContexts/SimpleEnvContextImpl/target/scala-2.10/simpleenvcontextimpl*.jar $systemlib
#cp $ivyPath/cache/commons-cli/commons-cli/jars/commons-cli-1.2.jar $systemlib
#cp $srcPath/lib_managed/jars/javax.servlet.jsp/jsp-api/jsp-api-2.1.jar $systemlib
#cp $ivyPath/cache/com.yammer.metrics/metrics-core/jars/metrics-core-2.2.0.jar $systemlib
#cp $srcPath/FileDataConsumer/target/scala-2.10/filedataconsumer*.jar $systemlib
#cp $ivyPath/cache/org.mapdb/mapdb/bundles/mapdb-1.0.6.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.sun.jersey/jersey-core/jersey-core-1.9.jar $systemlib
## cp $srcPath/Pmml/MethodExtractor/target/scala-2.10/methodextractor*.jar $systemlib
#cp $srcPath/lib_managed/jars/net.java.dev.jets3t/jets3t/jets3t-0.9.0.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.google.protobuf/protobuf-java/protobuf-java-2.6.0.jar $systemlib
#cp $ivyPath/cache/net.java.dev.jna/jna/jars/jna-3.2.7.jar $systemlib
## Might be only for 2.11
##cp $ivyPath/cache/org.scalameta/prettyprinters_2.11/jars/prettyprinters_2.11-0.0.3.jar $systemlib
#cp $srcPath/Utils/JdbcDataCollector/target/scala-2.10/jdbcdatacollector*.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.shiro/shiro-core/shiro-core-1.2.3.jar $systemlib
#cp $ivyPath/cache/joda-time/joda-time/jars/joda-time-2.8.2.jar $systemlib
#cp $srcPath/Utils/SimpleKafkaProducer/target/scala-2.10/simplekafkaproducer*.jar $systemlib
#cp $ivyPath/cache/voldemort/voldemort/jars/voldemort-0.96.jar $systemlib
#cp $srcPath/lib_managed/jars/asm/asm/asm-3.1.jar $systemlib
#cp $srcPath/Utils/UtilsForModels/target/scala-2.10/utilsformodels*.jar $systemlib
#cp $ivyPath/cache/com.fasterxml.jackson.core/jackson-core/bundles/jackson-core-2.3.1.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-pool/commons-pool/commons-pool-1.5.4.jar $systemlib
#cp $ivyPath/cache/org.json4s/json4s-ast_2.10/jars/json4s-ast_2.10-3.2.9.jar $systemlib
## Might be only for 2.11
##cp $srcPath/lib_managed/jars/org.scalameta/prettyprinters_2.11/prettyprinters_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/commons-lang/commons-lang/jars/commons-lang-2.6.jar $systemlib
#cp $ivyPath/cache/commons-digester/commons-digester/jars/commons-digester-1.8.1.jar $systemlib
#cp $ivyPath/cache/org.slf4j/slf4j-api/jars/slf4j-api-1.7.10.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hbase/hbase-annotations/hbase-annotations-1.0.2.jar $systemlib
#cp $ivyPath/cache/org.apache.hbase/hbase-common/jars/hbase-common-1.0.2.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-testkit_2.10/jars/spray-testkit_2.10-1.3.3.jar $systemlib
#cp $ivyPath/cache/net.debasishg/redisclient_2.10/jars/redisclient_2.10-2.13.jar $systemlib
## Might be only for 2.11
##cp $ivyPath/cache/org.scalameta/tokenquasiquotes_2.11/jars/tokenquasiquotes_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/jars/com.thoughtworks.paranamer/paranamer/paranamer-2.6.jar $systemlib
## one line added
#cp $ivyPath/cache/org.scala-lang/scala-compiler/jars/scala-compiler-2.10.0.jar $systemlib
#cp $ivyPath/cache/org.scala-lang/scala-compiler/jars/scala-compiler-2.10.4.jar $systemlib
#cp $srcPath/Utils/ZooKeeper/CuratorLeaderLatch/target/scala-2.10/zookeeperleaderlatch*.jar $systemlib
## Might be only for 2.11
##cp $srcPath/lib_managed/jars/org.scalameta/tokenquasiquotes_2.11/tokenquasiquotes*.jar $systemlib
#cp $ivyPath/cache/com.google.guava/guava/bundles/guava-14.0.1.jar $systemlib
#cp $ivyPath/cache/com.sun.jersey/jersey-server/bundles/jersey-server-1.9.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.httpcomponents/httpclient/httpclient-4.2.5.jar $systemlib
#cp $ivyPath/cache/com.twitter/chill_2.10/jars/chill_2.10-0.5.0.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.mapdb/mapdb/mapdb-1.0.6.jar $systemlib
#cp $srcPath/Pmml/PmmlRuntime/target/scala-2.10/pmmlruntime*.jar $systemlib
#cp $srcPath/lib_managed/jars/org.mortbay.jetty/jetty/jetty-6.1.26.jar $systemlib
#cp $srcPath/lib_managed/jars/com.sun.xml.bind/jaxb-impl/jaxb-impl-2.2.3-1.jar $systemlib
#
#cp $srcPath/Utils/ClusterInstaller/ClusterInstallerDriver/target/*.jar $systemlib
#cp $srcPath/Utils/ClusterInstaller/InstallDriver/target/scala-2.10/*.jar $systemlib
#cp $srcPath/Utils/ClusterInstaller/InstallDriverBase/target/*.jar $systemlib
#cp $srcPath/Utils/ClusterInstaller/GetComponent/target/scala-2.10/*.jar $systemlib
#
#cp $srcPath/lib_managed/jars/com.googlecode.json-simple/json-simple/json-simple-1.1.jar $systemlib
#cp $srcPath/AuditAdapters/AuditAdapterBase/target/scala-2.10/auditadapterbase*.jar $systemlib
#cp $ivyPath/cache/javax.xml.stream/stax-api/jars/stax-api-1.0-2.jar $systemlib
#cp $ivyPath/cache/commons-pool/commons-pool/jars/commons-pool-1.6.jar $systemlib
#cp $ivyPath/cache/org.apache.hadoop/hadoop-auth/jars/hadoop-auth-2.7.1.jar $systemlib
#cp $srcPath/Pmml/PmmlUdfs/target/scala-2.10/pmmludfs*.jar $systemlib
#cp $srcPath/lib_managed/jars/org.scala-lang/scalap/scalap-2.10.0.jar $systemlib
#cp $ivyPath/cache/org.codehaus.jettison/jettison/bundles/jettison-1.1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.codehaus.jackson/jackson-mapper-asl/jackson-mapper-asl-1.9.13.jar $systemlib
#cp $srcPath/Utils/NodeInfoExtract/target/scala-2.10/nodeinfoextract*.jar $systemlib
#cp $srcPath/Utils/UtilitySerivce/target/scala-2.10/utilityservice*.jar $systemlib
#cp $srcPath/lib_managed/jars/org.json4s/json4s-jackson_2.10/json4s-jackson_2.10-3.2.9.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-lang3/jars/commons-lang3-3.1.jar $systemlib
#cp $ivyPath/cache/com.datastax.cassandra/cassandra-driver-core/bundles/cassandra-driver-core-2.1.2.jar $systemlib
#cp $ivyPath/cache/org.apache.shiro/shiro-core/bundles/shiro-core-1.2.3.jar $systemlib
#cp $ivyPath/cache/commons-beanutils/commons-beanutils/jars/commons-beanutils-1.8.3.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-collections/commons-collections/commons-collections-3.2.1.jar $systemlib
## Might be only for 2.11
##cp $srcPath/lib_managed/bundles/org.scala-lang.modules/scala-xml_2.11/scala-xml_2.11-1.0.4.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.commons/commons-pool2/commons-pool2-2.3.jar $systemlib
#cp $srcPath/lib_managed/jars/org.tukaani/xz/xz-1.0.jar $systemlib
#cp $ivyPath/cache/org.mortbay.jetty/jetty-sslengine/jars/jetty-sslengine-6.1.26.jar $systemlib
#cp $srcPath/MessageDef/target/scala-2.10/messagedef*.jar $systemlib
#cp $srcPath/lib_managed/jars/org.codehaus.jackson/jackson-jaxrs/jackson-jaxrs-1.8.3.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-pool2/jars/commons-pool2-2.3.jar $systemlib
#cp $ivyPath/cache/com.sleepycat/je/jars/je-4.0.92.jar $systemlib
#cp $srcPath/lib_managed/jars/org.slf4j/slf4j-log4j12/slf4j-log4j12-1.7.10.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-io/commons-io/commons-io-2.4.jar $systemlib
#cp $srcPath/SampleApplication/CustomUdfLib/target/scala-2.10/customudflib*.jar $systemlib
## Might be only for 2.11
##cp $srcPath/lib_managed/jars/org.scalameta/quasiquotes_2.11/quasiquotes*.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-codec/commons-codec/commons-codec-1.10.jar $systemlib
#cp $ivyPath/cache/com.thoughtworks.paranamer/paranamer/jars/paranamer-2.6.jar $systemlib
#cp $ivyPath/cache/javax.servlet/servlet-api/jars/servlet-api-2.5.jar $systemlib
#cp $srcPath/lib_managed/jars/org.json4s/json4s-core_2.10/json4s-core_2.10-3.2.9.jar $systemlib
#cp $ivyPath/cache/com.google.code.gson/gson/jars/gson-2.2.4.jar $systemlib
#cp $srcPath/SecurityAdapters/SecurityAdapterBase/target/scala-2.10/securityadapterbase*.jar $systemlib
#cp $ivyPath/cache/org.ow2.asm/asm-commons/jars/asm-commons-4.0.jar $systemlib
#cp $srcPath/lib_managed/jars/com.twitter/chill-java/chill-java-0.5.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.hamcrest/hamcrest-core/hamcrest-core-1.3.jar $systemlib
#cp $srcPath/lib_managed/jars/org.jpmml/pmml-schema/pmml-schema-1.2.9.jar $systemlib
## Might be only for 2.11
##cp $ivyPath/cache/org.scalameta/trees_2.11/jars/trees_2.11-0.0.3.jar $systemlib
#cp $srcPath/Storage/HBase/target/scala-2.10/hbase*.jar $systemlib
#cp $ivyPath/cache/joda-time/joda-time/jars/joda-time-2.9.1.jar $systemlib
#cp $ivyPath/cache/org.apache.cassandra/cassandra-thrift/jars/cassandra-thrift-2.0.3.jar $systemlib
#cp $srcPath/BaseTypes/target/scala-2.10/basetypes*.jar $systemlib
#cp $srcPath/lib_managed/jars/com.google.code.findbugs/jsr305/jsr305-3.0.0.jar $systemlib
#cp $ivyPath/cache/org.apache.hadoop/hadoop-common/jars/hadoop-common-2.7.1.jar $systemlib
#cp $srcPath/BaseFunctions/target/scala-2.10/basefunctions*.jar $systemlib
#cp $ivyPath/cache/commons-codec/commons-codec/jars/commons-codec-1.10.jar $systemlib
#cp $ivyPath/cache/junit/junit/jars/junit-4.12.jar $systemlib
#cp $srcPath/Pmml/PmmlCompiler/target/scala-2.10/pmmlcompiler*.jar $systemlib
#cp $srcPath/lib_managed/jars/org.objenesis/objenesis/objenesis-1.2.jar $systemlib
#cp $ivyPath/cache/org.codehaus.jackson/jackson-core-asl/jars/jackson-core-asl-1.9.13.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hbase/hbase-common/hbase-common-1.0.2.jar $systemlib
#cp $srcPath/DataDelimiters/target/scala-2.10/datadelimiters*.jar $systemlib
#cp $ivyPath/cache/jline/jline/jars/jline-0.9.94.jar $systemlib
#cp $ivyPath/cache/org.scala-lang/scalap/jars/scalap-2.10.0.jar $systemlib
#cp $srcPath/MetadataBootstrap/Bootstrap/target/scala-2.10/bootstrap*.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-math3/jars/commons-math3-3.1.1.jar $systemlib
#cp $srcPath/Storage/Cassandra/target/scala-2.10/cassandra*.jar $systemlib
#cp $srcPath/Utils/Security/SimpleApacheShiroAdapter/target/scala-2.10/simpleapacheshiroadapter*.jar $systemlib
## Might be only for 2.11
##cp $srcPath/lib_managed/jars/org.scalameta/tokenizers_2.11/tokenizers_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/xmlenc/xmlenc/jars/xmlenc-0.52.jar $systemlib
#cp $srcPath/lib_managed/jars/org.jpmml/pmml-model/pmml-model-1.2.9.jar $systemlib
#cp $ivyPath/cache/org.apache.httpcomponents/httpcore/jars/httpcore-4.1.2.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-dbcp/commons-dbcp/commons-dbcp-1.4.jar $systemlib
#cp $srcPath/lib_managed/jars/javax.xml.stream/stax-api/stax-api-1.0-2.jar $systemlib
## Might be only for 2.11
##cp $srcPath/lib_managed/jars/org.scalameta/trees_2.11/trees_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/jars/org.jruby.joni/joni/joni-2.1.2.jar $systemlib
#cp $ivyPath/cache/com.codahale.metrics/metrics-core/bundles/metrics-core-3.0.2.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-httpx_2.10/bundles/spray-httpx_2.10-1.3.3.jar $systemlib
#cp $ivyPath/cache/commons-codec/commons-codec/jars/commons-codec-1.4.jar $systemlib
#cp $ivyPath/cache/com.101tec/zkclient/jars/zkclient-0.6.jar $systemlib

# -------------------- end of generated cp commands --------------------


###### For Version-2.10 ######
#
#cp $ivyPath/cache/io.spray/spray-json_2.10/bundles/spray-json_2.10-1.3.2.jar $systemlib
#cp $ivyPath/cache/com.codahale.metrics/metrics-core/bundles/metrics-core-3.0.2.jar $systemlib
#cp $ivyPath/cache/org.json4s/json4s-ast_2.10/jars/json4s-ast_2.10-3.2.9.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-testkit_2.10/jars/spray-testkit_2.10-1.3.3.jar $systemlib

cp $srcPath/Utils/Migrate/MigrateBase/target/migratebase-1.0.jar $systemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_1/target/scala-2.10/migratefrom_v_1_1_2.10-1.0.jar $systemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_2/target/scala-2.10/migratefrom_v_1_2_2.10-1.0.jar $systemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_3/target/scala-2.10/migratefrom_v_1_3_2.10-1.0.jar $systemlib
cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_3/target/scala-2.10/migrateto_v_1_3_2.10-1.0.jar $systemlib
cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_4/target/scala-2.10/migrateto_v_1_4_2.10-1.0.jar $systemlib

#cp $srcPath/Storage/Cassandra/target/scala-2.10/*.jar $systemlib
#cp $srcPath/Storage/HashMap/target/scala-2.10/*.jar $systemlib
#cp $srcPath/Storage/HBase/target/scala-2.10/*.jar $systemlib
##cp $srcPath/Storage/Redis/target/scala-2.11/*.jar $systemlib
#cp $srcPath/Storage/StorageBase/target/scala-2.10/storagebase_2.10-1.0.jar $systemlib
#cp $srcPath/Storage/StorageManager/target/scala-2.10/*.jar $systemlib
#cp $srcPath/Storage/TreeMap/target/scala-2.10/*.jar $systemlib
##cp $srcPath/Storage/Voldemort/target/scala-2.10/*.jar $systemlib
#cp $srcPath/InputOutputAdapters/InputOutputAdapterBase/target/scala-2.10/*.jar $systemlib
#cp $srcPath/KamanjaUtils/target/scala-2.10/kamanjautils_2.10-1.0.jar $systemlib
#cp $srcPath/SecurityAdapters/SecurityAdapterBase/target/scala-2.10/*.jar $systemlib

# cp $srcPath/Utils/SaveContainerDataComponent/target/scala-2.10/SaveContainerDataComponent* $systemlib
#cp $srcPath/Utils/UtilsForModels/target/scala-2.10/utilsformodels*.jar $systemlib

#copy jars for kamanjainstallapplib
cp $srcPath/Utils/Migrate/MigrateBase/target/migratebase-1.0.jar $kamanjainstallsystemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_1/target/scala-2.10/migratefrom_v_1_1_2.10-1.0.jar $kamanjainstallsystemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_2/target/scala-2.10/migratefrom_v_1_2_2.10-1.0.jar $kamanjainstallsystemlib
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_3/target/scala-2.10/migratefrom_v_1_3_2.10-1.0.jar $kamanjainstallsystemlib

cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_1/target/scala-2.10/migratefrom_v_1_1_2.10-1.0.jar $installPath/Kamanja-$ver211/lib/system/
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_2/target/scala-2.10/migratefrom_v_1_2_2.10-1.0.jar $installPath/Kamanja-$ver211/lib/system/
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_3/target/scala-2.10/migratefrom_v_1_3_2.10-1.0.jar $installPath/Kamanja-$ver211/lib/system/
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_1/target/scala-2.10/migratefrom_v_1_1_2.10-1.0.jar $installPath/KamanjaInstall-$ver211/lib/system/
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_2/target/scala-2.10/migratefrom_v_1_2_2.10-1.0.jar $installPath/KamanjaInstall-$ver211/lib/system/
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_3/target/scala-2.10/migratefrom_v_1_3_2.10-1.0.jar $installPath/KamanjaInstall-$ver211/lib/system/
migration2_10libsCopiesFor2_11="true"


# this should be changed?
cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_3/target/scala-2.10/migrateto_v_1_3_2.10-1.0.jar $kamanjainstallsystemlib
cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_4/target/scala-2.10/migrateto_v_1_4_2.10-1.0.jar $kamanjainstallsystemlib



# sample configs
#echo "copy sample configs..."
cp $srcPath/Utils/KVInit/src/main/resources/*cfg $systemlib

# Generate keystore file
#echo "generating keystore..."
#keytool -genkey -keyalg RSA -alias selfsigned -keystore $installPath/config/keystore.jks -storepass password -validity 360 -keysize 2048

#copy kamanja to bin directory
cp $srcPath/Utils/Script/scala-2.10/kamanja $bin
#cp $srcPath/Utils/Script/MedicalApp.sh $bin
cp $srcPath/MetadataAPI/target/scala-2.10/classes/HelpMenu.txt $installPath/Kamanja-$ver210/input
# *******************************
# COPD messages data prep
# *******************************

# Prepare test messages and copy them into place

echo "Prepare test messages and copy them into place..."
# *******************************
# Copy documentation files
# *******************************
cd $srcPath/Documentation
cp -rf * $installPath/Kamanja-$ver210/documentation

# *******************************
# Copy ClusterInstall
# *******************************
# mkdir -p $installPath/Kamanja-$ver210/ClusterInstall
# cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.10/* $installPath/Kamanja-$ver210/ClusterInstall/
# cp $srcPath/Utils/NodeInfoExtract/target/scala-2.10/NodeInfoExtract* $installPath/Kamanja-$ver210/ClusterInstall/
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.10/*.sh $kamanjainstallbin
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.10/*log4*.xml $kamanjainstallconfig
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.10/*.json $kamanjainstallconfig
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.10/*.properties $kamanjainstallconfig
cp $srcPath/Utils/ClusterInstaller/ClusterInstallerDriver/src/main/resources/log4j2.xml $kamanjainstallconfig

# *******************************
# copy models, messages, containers, config, scripts, types  messages data prep
# *******************************

#HelloWorld
cd $srcPath/SampleApplication/HelloWorld/data
cp * $installPath/Kamanja-$ver210/input/SampleApplications/data

cd $srcPath/SampleApplication/HelloWorld/message
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/HelloWorld/model
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/HelloWorld/template
cp -rf * $installPath/Kamanja-$ver210/input/SampleApplications/template


cd $srcPath/SampleApplication/HelloWorld/config
cp -rf * $installPath/Kamanja-$ver210/config
#HelloWorld

#Medical
cd $srcPath/SampleApplication/Medical/SampleData
cp *.csv $installPath/Kamanja-$ver210/input/SampleApplications/data
cp *.csv.gz $installPath/Kamanja-$ver210/input/SampleApplications/data

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Containers
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/Medical/Functions
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/function

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Messages
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/Medical/Models
cp *.* $installPath/Kamanja-$ver210/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/Medical/Types
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/type

cd $srcPath/SampleApplication/Medical/template
cp -rf * $installPath/Kamanja-$ver210/input/SampleApplications/template

cd $srcPath/SampleApplication/Medical/Configs
cp -rf * $installPath/Kamanja-$ver210/config
#Medical

#Telecom
cd $srcPath/SampleApplication/Telecom/data
cp * $installPath/Kamanja-$ver210/input/SampleApplications/data

cd $srcPath/SampleApplication/Telecom/metadata/container
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/Telecom/metadata/message
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/Telecom/metadata/model
cp *.* $installPath/Kamanja-$ver210/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/Telecom/metadata/template
cp -rf * $installPath/Kamanja-$ver210/input/SampleApplications/template

cd $srcPath/SampleApplication/Telecom/metadata/config
cp -rf * $installPath/Kamanja-$ver210/config
#Telecom

#Finance
cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/data
cp * $installPath/Kamanja-$ver210/input/SampleApplications/data

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/container
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/message
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/model
cp *.* $installPath/Kamanja-$ver210/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/type
cp * $installPath/Kamanja-$ver210/input/SampleApplications/metadata/type

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/template
cp -rf * $installPath/Kamanja-$ver210/input/SampleApplications/template

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/config
cp -rf * $installPath/Kamanja-$ver210/config
#Finance

cd $srcPath/SampleApplication/EasyInstall/template/scala-2.10
cp -rf * $installPath/Kamanja-$ver210/template

cd $srcPath/SampleApplication/EasyInstall
cp SetPaths.sh $installPath/Kamanja-$ver210/bin/

bash $installPath/Kamanja-$ver210/bin/SetPaths.sh $KafkaRootDir

chmod 0700 $installPath/Kamanja-$ver210/input/SampleApplications/bin/*sh

################################ Version-2.10 Finished ################################



# *******************************
# Build fat-jars for version-2.11
# *******************************

echo "clean, package and assemble $srcPath ..."

bin=$installPath/Kamanja-$ver211/bin
systemlib=$installPath/Kamanja-$ver211/lib/system
applib=$installPath/Kamanja-$ver211/lib/application


kamanjainstallbin=$installPath/KamanjaInstall-$ver211/bin
kamanjainstallsystemlib=$installPath/KamanjaInstall-$ver211/lib/system
kamanjainstallapplib=$installPath/KamanjaInstall-$ver211/lib/application
kamanjainstallconfig=$installPath/KamanjaInstall-$ver211/config


echo $installPath
echo $srcPath
echo $bin

# Once we get all 2.10 libraries and copy them to corresponding directories, we can run 2.11 again and copy them to corresponding directories
# sbt clean '++ 2.11.7 package' '++ 2.11.7 KamanjaManager/assembly' '++ 2.11.7 MetadataAPI/assembly' '++ 2.11.7 KVInit/assembly' '++ 2.11.7 MethodExtractor/assembly' '++ 2.11.7 SimpleKafkaProducer/assembly' '++ 2.11.7 NodeInfoExtract/assembly' '++ 2.11.7 ExtractData/assembly' '++ 2.11.7 MetadataAPIService/assembly' '++ 2.11.7 JdbcDataCollector/assembly' '++ 2.11.7 FileDataConsumer/assembly' '++ 2.11.7 SaveContainerDataComponent/assembly' '++ 2.11.7 CleanUtil/assembly' '++ 2.11.7 MigrateManager/assembly'

cd $srcPath
#Build and copy 2.10 for both MigrateFrom_V_1_1 & MigrateFrom_V_1_2, if they are not copied from 2.10.4 build
if [ "$migration2_10libsCopiesFor2_11" == "false" ]; then
	sbt clean '++ 2.10.4 MigrateFrom_V_1_1/package' '++ 2.10.4 MigrateFrom_V_1_2/package' '++ 2.10.4 MigrateFrom_V_1_3/package' '++ 2.10.4 MigrateTo_V_1_4/package'
	cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_1/target/scala-2.10/migratefrom_v_1_1_2.10-1.0.jar $systemlib
	cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_2/target/scala-2.10/migratefrom_v_1_2_2.10-1.0.jar $systemlib
	cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_3/target/scala-2.10/migratefrom_v_1_3_2.10-1.0.jar $systemlib
	cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_4/target/scala-2.10/migrateto_v_1_4_2.10-1.0.jar $systemlib
	cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_1/target/scala-2.10/migratefrom_v_1_1_2.10-1.0.jar $kamanjainstallsystemlib
	cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_2/target/scala-2.10/migratefrom_v_1_2_2.10-1.0.jar $kamanjainstallsystemlib
	cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_3/target/scala-2.10/migratefrom_v_1_3_2.10-1.0.jar $kamanjainstallsystemlib
	cp $srcPath/Utils/Migrate/DestnationVersion/MigrateTo_V_1_4/target/scala-2.10/migrateto_v_1_4_2.10-1.0.jar $kamanjainstallsystemlib
fi

#Now do full build of 2.11
sbt clean '++ 2.11.7 package' '++ 2.11.7 ExtDependencyLibs/assembly' '++ 2.11.7 ExtDependencyLibs2/assembly' '++ 2.11.7 KamanjaInternalDeps/assembly'
sbt '++ 2.11.7 MigrateManager/assembly' '++ 2.11.7 ClusterInstallerDriver/assembly' '++ 2.11.7 InstallDriver/assembly' '++ 2.11.7 GetComponent/assembly' '++ 2.11.7 NodeInfoExtract/assembly'
#sbt clean '++ 2.11.7 package' '++ 2.11.7 KamanjaManager/assembly' '++ 2.11.7 MetadataAPI/assembly' '++ 2.11.7 KVInit/assembly' '++ 2.11.7 SimpleKafkaProducer/assembly'
#sbt '++ 2.11.7 NodeInfoExtract/assembly' '++ 2.11.7 MetadataAPIService/assembly' '++ 2.11.7 JdbcDataCollector/assembly'
#sbt '++ 2.11.7 FileDataConsumer/assembly' '++ 2.11.7 CleanUtil/assembly' '++ 2.11.7 MigrateManager/assembly' '++ 2.11.7 ClusterInstallerDriver/assembly' '++ 2.11.7 InstallDriver/assembly' '++ 2.11.7 GetComponent/assembly' '++ 2.11.7 PmmlTestTool/assembly' '++ 2.11.7 ExtDependencyLibs/assembly' '++ 2.11.7 ExtDependencyLibs2/assembly' '++ 2.11.7 KamanjaInternalDeps/assembly'
# sbt '++ 2.11.7 MethodExtractor/assembly' '++ 2.11.7 SaveContainerDataComponent/assembly' '++ 2.11.7 ExtractData/assembly' 

# recreate eclipse projects
#echo "refresh the eclipse projects ..."
#cd $srcPath
#sbt eclipse

# Move them into place
echo "copy the fat jars to $installPath ..."

cd $srcPath
cp Utils/KVInit/target/scala-2.11/kvinit* $systemlib
cp MetadataAPI/target/scala-2.11/metadataapi* $systemlib
cp KamanjaManager/target/scala-2.11/kamanjamanager* $systemlib
# cp Pmml/MethodExtractor/target/scala-2.11/methodextractor* $bin
cp Utils/SimpleKafkaProducer/target/scala-2.11/simplekafkaproducer* $systemlib
# cp Utils/ExtractData/target/scala-2.11/extractdata* $bin
cp Utils/JdbcDataCollector/target/scala-2.11/jdbcdatacollector* $systemlib
cp MetadataAPIService/target/scala-2.11/metadataapiservice* $systemlib
cp FileDataConsumer/target/scala-2.11/filedataconsumer* $systemlib
cp Utils/CleanUtil/target/scala-2.11/cleanutil* $systemlib
cp Utils/Migrate/MigrateManager/target/MigrateManager* $bin
cp Utils/ClusterInstaller/ClusterInstallerDriver/target/ClusterInstallerDriver* $kamanjainstallbin
cp Utils/ClusterInstaller/InstallDriver/target/scala-2.11/InstallDriver* $kamanjainstallbin
cp Utils/ClusterInstaller/GetComponent/target/scala-2.11/GetComponent* $kamanjainstallbin
cp Utils/ClusterInstaller/InstallDriver/src/main/resources/GetComponentsVersions.sh $kamanjainstallbin
cp Utils/Migrate/MigrateManager/target/MigrateManager* $kamanjainstallbin
cp $srcPath/Utils/NodeInfoExtract/target/scala-2.11/NodeInfoExtract* $kamanjainstallbin
cp Utils/PmmlTestTool/target/pmmltesttool* $systemlib
#cp Utils/KVInit/target/scala-2.11/KVInit* $systemlib
#cp MetadataAPI/target/scala-2.11/MetadataAPI* $systemlib
#cp KamanjaManager/target/scala-2.11/KamanjaManager* $systemlib
## cp Pmml/MethodExtractor/target/scala-2.11/MethodExtractor* $bin
#cp Utils/SimpleKafkaProducer/target/scala-2.11/SimpleKafkaProducer* $systemlib
## cp Utils/ExtractData/target/scala-2.11/ExtractData* $bin
#cp Utils/JdbcDataCollector/target/scala-2.11/JdbcDataCollector* $systemlib
#cp MetadataAPIService/target/scala-2.11/MetadataAPIService* $systemlib
#cp FileDataConsumer/target/scala-2.11/FileDataConsumer* $systemlib
#cp Utils/CleanUtil/target/scala-2.11/CleanUtil* $systemlib
#cp Utils/Migrate/MigrateManager/target/MigrateManager* $bin
#cp Utils/ClusterInstaller/ClusterInstallerDriver/target/ClusterInstallerDriver* $kamanjainstallbin
#cp Utils/ClusterInstaller/InstallDriver/target/scala-2.11/InstallDriver* $kamanjainstallbin
#cp Utils/ClusterInstaller/GetComponent/target/scala-2.11/GetComponent* $kamanjainstallbin
#cp Utils/ClusterInstaller/InstallDriver/src/main/resources/GetComponentsVersions.sh $kamanjainstallbin
#cp Utils/Migrate/MigrateManager/target/MigrateManager* $kamanjainstallbin
#cp $srcPath/Utils/NodeInfoExtract/target/scala-2.11/NodeInfoExtract* $kamanjainstallbin
#cp Utils/PmmlTestTool/target/PmmlTestTool* $systemlib

# copy jars used to reduce package size
cp ExtDependencyLibs/target/scala-2.11/ExtDependencyLibs_2.11-1.4.0.jar $systemlib
cp ExtDependencyLibs2/target/scala-2.11/ExtDependencyLibs2_2.11-1.4.0.jar $systemlib
cp KamanjaInternalDeps/target/scala-2.11/KamanjaInternalDeps_2.11-1.4.0.jar $systemlib

# *******************************
# Copy jars required version-2.11 (more than required if the fat jars are used)
# *******************************

# Base Types and Functions, InputOutput adapters, and original versions of things
echo "copy all Kamanja jars and the jars upon which they depend to the $systemlib"

# -------------------- generated cp commands --------------------

#cp $srcPath/lib_managed/bundles/org.apache.directory.api/api-util/api-util-1.0.0-M20.jar $systemlib
#cp $ivyPath/cache/org.codehaus.jackson/jackson-xc/jars/jackson-xc-1.8.3.jar $systemlib
#cp $ivyPath/cache/org.apache.kafka/kafka-clients/jars/kafka-clients-0.8.2.2.jar $systemlib
#cp $ivyPath/cache/org.xerial.snappy/snappy-java/bundles/snappy-java-1.0.4.1.jar $systemlib
#cp $ivyPath/cache/javax.xml.bind/jaxb-api/jars/jaxb-api-2.2.2.jar $systemlib
#cp $ivyPath/cache/log4j/log4j/bundles/log4j-1.2.16.jar $systemlib
#cp $ivyPath/cache/com.sun.jersey/jersey-core/bundles/jersey-core-1.9.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.codehaus.jettison/jettison/jettison-1.1.jar $systemlib
#cp $ivyPath/cache/org.jruby.jcodings/jcodings/jars/jcodings-1.0.8.jar $systemlib
#cp $ivyPath/cache/org.scalatest/scalatest_2.11/bundles/scalatest_2.11-2.2.0.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-client/bundles/curator-client-2.7.1.jar $systemlib
#cp $ivyPath/cache/commons-digester/commons-digester/jars/commons-digester-1.8.jar $systemlib
#cp $ivyPath/cache/org.jruby.joni/joni/jars/joni-2.1.2.jar $systemlib
#cp $ivyPath/cache/org.apache.directory.api/api-util/bundles/api-util-1.0.0-M20.jar $systemlib
#cp $ivyPath/cache/org.mortbay.jetty/jetty-util/jars/jetty-util-6.1.26.jar $systemlib
#cp $srcPath/InputOutputAdapters/KafkaSimpleInputOutputAdapters/target/scala-2.11/kafkasimpleinputoutputadapters_2.11-1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.commons/commons-lang3/commons-lang3-3.1.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-collections4/jars/commons-collections4-4.0.jar $systemlib
#cp $srcPath/Storage/SqlServer/target/scala-2.11/sqlserver_2.11-0.1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.commons/commons-compress/commons-compress-1.4.1.jar $systemlib
#cp $ivyPath/cache/org.apache.logging.log4j/log4j-api/jars/log4j-api-2.4.1.jar $systemlib
#cp $ivyPath/cache/org.apache.logging.log4j/log4j-1.2-api/bundles/log4j-1.2-api-2.4.1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hadoop/hadoop-auth/hadoop-auth-2.7.1.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-lang/commons-lang/commons-lang-2.6.jar $systemlib
#cp $srcPath/lib_managed/jars/com.google.code.gson/gson/gson-2.2.4.jar $systemlib
#cp $ivyPath/cache/org.jpmml/pmml-schema/jars/pmml-schema-1.2.9.jar $systemlib
#cp $ivyPath/cache/org.javassist/javassist/bundles/javassist-3.18.1-GA.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.logging.log4j/log4j-api/log4j-api-2.4.1.jar $systemlib
#cp $ivyPath/cache/com.typesafe/config/bundles/config-1.2.1.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.curator/curator-framework/curator-framework-2.7.1.jar $systemlib
#cp $ivyPath/cache/org.json4s/json4s-jackson_2.11/jars/json4s-jackson_2.11-3.2.9.jar $systemlib
#cp $ivyPath/cache/commons-net/commons-net/jars/commons-net-3.1.jar $systemlib
#cp $ivyPath/cache/org.apache.hadoop/hadoop-annotations/jars/hadoop-annotations-2.7.1.jar $systemlib
#cp $ivyPath/cache/com.101tec/zkclient/jars/zkclient-0.3.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-math3/jars/commons-math3-3.6.jar $systemlib
#cp $ivyPath/cache/org.apache.camel/camel-core/bundles/camel-core-2.9.2.jar $systemlib
#cp $ivyPath/cache/com.google.code.findbugs/jsr305/jars/jsr305-3.0.0.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-cli/commons-cli/commons-cli-1.2.jar $systemlib
#cp $srcPath/lib_managed/jars/com.jamesmurty.utils/java-xmlbuilder/java-xmlbuilder-0.4.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.fasterxml.jackson.core/jackson-annotations/jackson-annotations-2.3.0.jar $systemlib
#cp $ivyPath/cache/com.pyruby/java-stub-server/jars/java-stub-server-0.12-sources.jar $systemlib
#cp $ivyPath/cache/com.esotericsoftware.reflectasm/reflectasm/jars/reflectasm-1.07-shaded.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-client_2.11/bundles/spray-client_2.11-1.3.3.jar $systemlib
#cp $srcPath/lib_managed/jars/javax.servlet/servlet-api/servlet-api-2.5.jar $systemlib
#cp $srcPath/Utils/Audit/target/scala-2.11/auditadapters_2.11-1.0.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.sun.jersey/jersey-server/jersey-server-1.9.jar $systemlib
#cp $srcPath/lib_managed/jars/org.json4s/json4s-native_2.11/json4s-native_2.11-3.2.9.jar $systemlib
#cp $ivyPath/cache/org.apache.directory.server/apacheds-i18n/bundles/apacheds-i18n-2.0.0-M15.jar $systemlib
#cp $srcPath/FactoriesOfModelInstanceFactory/JarFactoryOfModelInstanceFactory/target/scala-2.11/jarfactoryofmodelinstancefactory_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/org.jpmml/pmml-evaluator/jars/pmml-evaluator-1.2.9.jar $systemlib
#cp $ivyPath/cache/commons-httpclient/commons-httpclient/jars/commons-httpclient-3.1.jar $systemlib
#cp $srcPath/TransactionService/target/scala-2.11/transactionservice_2.11-0.1.0.jar $systemlib
#cp $ivyPath/cache/io.netty/netty-all/jars/netty-all-4.0.23.Final.jar $systemlib
#cp $ivyPath/cache/com.esotericsoftware.kryo/kryo/bundles/kryo-2.21.jar $systemlib
##cp $srcPath/lib_managed/jars/org.scalameta/tokens_2.11/tokens_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/com.ning/compress-lzf/bundles/compress-lzf-0.9.1.jar $systemlib
#cp $ivyPath/cache/org.scala-lang/scala-actors/jars/scala-actors-2.11.7.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.directory.server/apacheds-i18n/apacheds-i18n-2.0.0-M15.jar $systemlib
#cp $srcPath/Utils/Controller/target/scala-2.11/controller_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/org.slf4j/slf4j-log4j12/jars/slf4j-log4j12-1.6.1.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.fasterxml.jackson.core/jackson-databind/jackson-databind-2.3.1.jar $systemlib
#cp $srcPath/lib_managed/jars/javax.activation/activation/activation-1.1.jar $systemlib
## cp $srcPath/Utils/ExtractData/target/scala-2.11/extractdata_2.11-1.0.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.xerial.snappy/snappy-java/snappy-java-1.0.4.1.jar $systemlib
#cp $srcPath/lib_managed/jars/joda-time/joda-time/joda-time-2.9.1.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-io_2.11/bundles/spray-io_2.11-1.3.3.jar $systemlib
#cp $ivyPath/cache/com.typesafe.akka/akka-actor_2.11/jars/akka-actor_2.11-2.3.2.jar $systemlib
#cp $ivyPath/cache/com.typesafe.akka/akka-actor_2.11/jars/akka-actor_2.11-2.3.9.jar $systemlib
#cp $ivyPath/cache/uk.co.bigbeeconsultants/bee-client_2.11/jars/bee-client_2.11-0.28.0.jar $systemlib
##cp $srcPath/lib_managed/jars/org.scalameta/foundation_2.11/foundation_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/org.apache.hbase/hbase-protocol/jars/hbase-protocol-1.0.2.jar $systemlib
#cp $ivyPath/cache/org.scala-lang.modules/scala-parser-combinators_2.11/bundles/scala-parser-combinators_2.11-1.0.2.jar $systemlib
#cp $srcPath/lib_managed/jars/javax.xml.bind/jaxb-api/jaxb-api-2.2.2.jar $systemlib
#cp $ivyPath/cache/org.jdom/jdom/jars/jdom-1.1.jar $systemlib
#cp $srcPath/KvBase/target/scala-2.11/kvbase_2.11-0.1.0.jar $systemlib
#cp $ivyPath/cache/com.googlecode.json-simple/json-simple/jars/json-simple-1.1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hbase/hbase-client/hbase-client-1.0.2.jar $systemlib
#cp $ivyPath/cache/org.apache.hbase/hbase-client/jars/hbase-client-1.0.2.jar $systemlib
#cp $ivyPath/cache/ch.qos.logback/logback-classic/jars/logback-classic-1.0.13.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hadoop/hadoop-annotations/hadoop-annotations-2.7.1.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-util_2.11/bundles/spray-util_2.11-1.3.3.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.commons/commons-collections4/commons-collections4-4.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.zookeeper/zookeeper/zookeeper-3.4.6.jar $systemlib
#cp $ivyPath/cache/com.twitter/chill-java/jars/chill-java-0.5.0.jar $systemlib
#cp $ivyPath/cache/asm/asm/jars/asm-3.1.jar $systemlib
#cp $ivyPath/cache/org.scalameta/quasiquotes_2.11/jars/quasiquotes_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-recipes/bundles/curator-recipes-2.7.1.jar $systemlib
#cp $srcPath/MetadataAPI/target/scala-2.11/metadataapi_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-math/jars/commons-math-2.2.jar $systemlib
#cp $ivyPath/cache/com.sun.xml.bind/jaxb-impl/jars/jaxb-impl-2.2.3-1.jar $systemlib
##cp $srcPath/lib_managed/jars/org.scalameta/dialects_2.11/dialects_2.11-0.0.3.jar $systemlib
##cp $srcPath/lib_managed/jars/org.scalameta/parsers_2.11/parsers_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/bundles/io.netty/netty/netty-3.9.0.Final.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.commons/commons-math3/commons-math3-3.6.jar $systemlib
#cp $srcPath/lib_managed/jars/com.github.stephenc.findbugs/findbugs-annotations/findbugs-annotations-1.3.9-1.jar $systemlib
#cp $ivyPath/cache/com.fasterxml.jackson.core/jackson-databind/bundles/jackson-databind-2.3.1.jar $systemlib
#cp $ivyPath/cache/com.fasterxml.jackson.core/jackson-annotations/bundles/jackson-annotations-2.3.0.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-dbcp2/jars/commons-dbcp2-2.1.jar $systemlib
#cp $srcPath/lib_managed/jars/com.twitter/chill_2.11/chill_2.11-0.5.0.jar $systemlib
#cp $ivyPath/cache/org.json4s/json4s-native_2.11/jars/json4s-native_2.11-3.2.9.jar $systemlib
#cp $ivyPath/cache/junit/junit/jars/junit-3.8.1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.mortbay.jetty/jetty-util/jetty-util-6.1.26.jar $systemlib
#cp $ivyPath/cache/com.typesafe/config/bundles/config-1.2.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hadoop/hadoop-common/hadoop-common-2.7.1.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-digester/commons-digester/commons-digester-1.8.1.jar $systemlib
#cp $ivyPath/cache/commons-dbcp/commons-dbcp/jars/commons-dbcp-1.4.jar $systemlib
#cp $srcPath/lib_managed/jars/org.scala-lang/scala-actors/scala-actors-2.11.7.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.logging.log4j/log4j-core/log4j-core-2.4.1.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-framework/bundles/curator-framework-2.7.1.jar $systemlib
#cp $srcPath/lib_managed/bundles/log4j/log4j/log4j-1.2.17.jar $systemlib
#cp $srcPath/lib_managed/jars/xmlenc/xmlenc/xmlenc-0.52.jar $systemlib
#cp $ivyPath/cache/org.mortbay.jetty/jetty-embedded/jars/jetty-embedded-6.1.26-sources.jar $systemlib
#cp $ivyPath/cache/org.apache.httpcomponents/httpclient/jars/httpclient-4.2.5.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.directory.api/api-asn1-api/api-asn1-api-1.0.0-M20.jar $systemlib
#cp $ivyPath/cache/net.jpountz.lz4/lz4/jars/lz4-1.2.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.scala-lang/scala-reflect/scala-reflect-2.11.7.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.commons/commons-dbcp2/commons-dbcp2-2.1.jar $systemlib
#cp $ivyPath/cache/org.scala-lang.modules/scala-parser-combinators_2.11/bundles/scala-parser-combinators_2.11-1.0.4.jar $systemlib
#cp $srcPath/lib_managed/jars/org.jpmml/pmml-agent/pmml-agent-1.2.9.jar $systemlib
#cp $ivyPath/cache/commons-logging/commons-logging/jars/commons-logging-1.1.1.jar $systemlib
#cp $ivyPath/cache/net.java.dev.jets3t/jets3t/jars/jets3t-0.9.0.jar $systemlib
#cp $ivyPath/cache/org.scalatest/scalatest_2.11/bundles/scalatest_2.11-2.2.4.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.sun.jersey/jersey-json/jersey-json-1.9.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-can_2.11/bundles/spray-can_2.11-1.3.3.jar $systemlib
#cp $ivyPath/cache/commons-logging/commons-logging/jars/commons-logging-1.2.jar $systemlib
#cp $srcPath/Storage/TreeMap/target/scala-2.11/treemap_2.11-0.1.0.jar $systemlib
#cp $srcPath/Utils/KVInit/target/scala-2.11/kvinit_2.11-1.0.jar $systemlib
#cp $srcPath/Utils/Serialize/target/scala-2.11/serialize_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/org.apache.htrace/htrace-core/jars/htrace-core-3.1.0-incubating.jar $systemlib
#cp $srcPath/Storage/StorageBase/target/scala-2.11/storagebase_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-json_2.11/bundles/spray-json_2.11-1.3.2.jar $systemlib
#cp $ivyPath/cache/commons-codec/commons-codec/jars/commons-codec-1.9.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.avro/avro/avro-1.7.4.jar $systemlib
#cp $ivyPath/cache/org.joda/joda-convert/jars/joda-convert-1.7.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.codahale.metrics/metrics-core/metrics-core-3.0.2.jar $systemlib
#cp $ivyPath/cache/com.esotericsoftware.minlog/minlog/jars/minlog-1.2.jar $systemlib
#cp $ivyPath/cache/org.mortbay.jetty/servlet-api/jars/servlet-api-2.5.20110712-sources.jar $systemlib
#cp $ivyPath/cache/com.google.collections/google-collections/jars/google-collections-1.0.jar $systemlib
#cp $ivyPath/cache/ch.qos.logback/logback-core/jars/logback-core-1.0.12.jar $systemlib
#cp $ivyPath/cache/org.scala-lang.modules/scala-parser-combinators_2.11/bundles/scala-parser-combinators_2.11-1.0.1.jar $systemlib
#cp $ivyPath/cache/io.netty/netty/bundles/netty-3.7.0.Final.jar $systemlib
#cp $ivyPath/cache/org.scalameta/parsers_2.11/jars/parsers_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/com.jamesmurty.utils/java-xmlbuilder/jars/java-xmlbuilder-0.4.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.fasterxml.jackson.core/jackson-core/jackson-core-2.3.1.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.datastax.cassandra/cassandra-driver-core/cassandra-driver-core-2.1.2.jar $systemlib
#cp $srcPath/MetadataAPIService/target/scala-2.11/metadataapiservice_2.11-1.0.jar $systemlib
##cp $srcPath/lib_managed/jars/org.scalameta/exceptions_2.11/exceptions_2.11-0.0.3.jar $systemlib
#cp $srcPath/Utils/ZooKeeper/CuratorClient/target/scala-2.11/zookeeperclient_2.11-1.0.jar $systemlib
#cp $srcPath/Metadata/target/scala-2.11/metadata_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/org.scalameta/exceptions_2.11/jars/exceptions_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/commons-pool/commons-pool/jars/commons-pool-1.5.4.jar $systemlib
#cp $ivyPath/cache/org.parboiled/parboiled-scala_2.11/jars/parboiled-scala_2.11-1.1.7.jar $systemlib
#cp $ivyPath/cache/org.parboiled/parboiled-core/jars/parboiled-core-1.1.7.jar $systemlib
#cp $srcPath/lib_managed/jars/io.netty/netty-all/netty-all-4.0.23.Final.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.htrace/htrace-core/htrace-core-3.1.0-incubating.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.curator/curator-client/curator-client-2.7.1.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-compress/jars/commons-compress-1.4.1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.codehaus.jackson/jackson-xc/jackson-xc-1.8.3.jar $systemlib
#cp $srcPath/lib_managed/jars/jline/jline/jline-0.9.94.jar $systemlib
#cp $ivyPath/cache/commons-beanutils/commons-beanutils/jars/commons-beanutils-1.7.0.jar $systemlib
#cp $ivyPath/cache/org.apache.avro/avro/jars/avro-1.7.4.jar $systemlib
#cp $ivyPath/cache/ch.qos.logback/logback-core/jars/logback-core-1.0.13.jar $systemlib
#cp $srcPath/KamanjaBase/target/scala-2.11/kamanjabase_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/com.typesafe.akka/akka-testkit_2.11/jars/akka-testkit_2.11-2.3.9.jar $systemlib
#cp $ivyPath/cache/commons-configuration/commons-configuration/jars/commons-configuration-1.7.jar $systemlib
#cp $ivyPath/cache/commons-beanutils/commons-beanutils-core/jars/commons-beanutils-core-1.8.0.jar $systemlib
#cp $srcPath/Utils/CleanUtil/target/scala-2.11/cleanutil_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/com.jcraft/jsch/jars/jsch-0.1.42.jar $systemlib
#cp $srcPath/lib_managed/jars/org.codehaus.jackson/jackson-core-asl/jackson-core-asl-1.9.13.jar $systemlib
#cp $ivyPath/cache/com.google.code.gson/gson/jars/gson-2.3.1.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-routing_2.11/bundles/spray-routing_2.11-1.3.3.jar $systemlib
#cp $ivyPath/cache/org.apache.kafka/kafka_2.11/jars/kafka_2.11-0.8.2.2.jar $systemlib
#cp $ivyPath/cache/com.chuusai/shapeless_2.11/jars/shapeless_2.11-1.2.4.jar $systemlib
#cp $ivyPath/cache/org.apache.directory.api/api-asn1-api/bundles/api-asn1-api-1.0.0-M20.jar $systemlib
#cp $ivyPath/cache/com.google.code.findbugs/jsr305/jars/jsr305-1.3.9.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-logging/commons-logging/commons-logging-1.2.jar $systemlib
#cp $srcPath/lib_managed/jars/org.ow2.asm/asm/asm-4.0.jar $systemlib
#cp $ivyPath/cache/org.apache.thrift/libthrift/jars/libthrift-0.9.2.jar $systemlib
#cp $srcPath/KamanjaUtils/target/scala-2.11/kamanjautils_2.11-1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/com.esotericsoftware.reflectasm/reflectasm/reflectasm-1.07-shaded.jar $systemlib
#cp $srcPath/Utils/ZooKeeper/CuratorListener/target/scala-2.11/zookeeperlistener_2.11-1.0.jar $systemlib
#cp $srcPath/InputOutputAdapters/InputOutputAdapterBase/target/scala-2.11/inputoutputadapterbase_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/org.scala-lang/scala-reflect/jars/scala-reflect-2.11.7.jar $systemlib
#cp $ivyPath/cache/org.jpmml/pmml-agent/jars/pmml-agent-1.2.9.jar $systemlib
#cp $ivyPath/cache/org.json4s/json4s-core_2.11/jars/json4s-core_2.11-3.2.9.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-configuration/commons-configuration/commons-configuration-1.7.jar $systemlib
#cp $ivyPath/cache/org.codehaus.jackson/jackson-mapper-asl/jars/jackson-mapper-asl-1.9.13.jar $systemlib
#cp $ivyPath/cache/org.hamcrest/hamcrest-core/jars/hamcrest-core-1.3.jar $systemlib
#cp $ivyPath/cache/org.joda/joda-convert/jars/joda-convert-1.6.jar $systemlib
#cp $ivyPath/cache/org.scala-lang/scala-library/jars/scala-library-2.11.7.jar $systemlib
#cp $ivyPath/cache/commons-collections/commons-collections/jars/commons-collections-3.2.1.jar $systemlib
#cp $ivyPath/cache/org.scalameta/tokenizers_2.11/jars/tokenizers_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/jars/org.slf4j/slf4j-api/slf4j-api-1.7.10.jar $systemlib
#cp $ivyPath/cache/org.apache.zookeeper/zookeeper/jars/zookeeper-3.4.6.jar $systemlib
#cp $ivyPath/cache/org.scalameta/foundation_2.11/jars/foundation_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.curator/curator-recipes/curator-recipes-2.7.1.jar $systemlib
#cp $ivyPath/cache/org.objenesis/objenesis/jars/objenesis-1.2.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.scalatest/scalatest_2.11/scalatest_2.11-2.2.4.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-net/commons-net/commons-net-3.1.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-httpclient/commons-httpclient/commons-httpclient-3.1.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.directory.server/apacheds-kerberos-codec/apacheds-kerberos-codec-2.0.0-M15.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.esotericsoftware.kryo/kryo/kryo-2.21.jar $systemlib
#cp $srcPath/lib_managed/jars/org.scala-lang/scala-compiler/scala-compiler-2.11.7.jar $systemlib
#cp $ivyPath/cache/org.scala-lang.modules/scala-xml_2.11/bundles/scala-xml_2.11-1.0.4.jar $systemlib
#cp $srcPath/Utils/JsonDataGen/target/scala-2.11/jsondatagen_2.11-0.1.0.jar $systemlib
#cp $ivyPath/cache/org.apache.httpcomponents/httpclient/jars/httpclient-4.1.2.jar $systemlib
#cp $srcPath/lib_managed/jars/org.jruby.jcodings/jcodings/jcodings-1.0.8.jar $systemlib
#cp $ivyPath/cache/ch.qos.logback/logback-classic/jars/logback-classic-1.0.12.jar $systemlib
#cp $srcPath/FactoriesOfModelInstanceFactory/JpmmlFactoryOfModelInstanceFactory/target/scala-2.11/jpmmlfactoryofmodelinstancefactory_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/org.apache.directory.server/apacheds-kerberos-codec/bundles/apacheds-kerberos-codec-2.0.0-M15.jar $systemlib
#cp $ivyPath/cache/io.netty/netty/bundles/netty-3.9.0.Final.jar $systemlib
#cp $ivyPath/cache/org.scalameta/tokens_2.11/jars/tokens_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/org.slf4j/slf4j-log4j12/jars/slf4j-log4j12-1.7.10.jar $systemlib
#cp $srcPath/OutputMsgDef/target/scala-2.11/outputmsgdef_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/org.jpmml/pmml-model/jars/pmml-model-1.2.9.jar $systemlib
#cp $ivyPath/cache/org.apache.httpcomponents/httpcore/jars/httpcore-4.2.4.jar $systemlib
#cp $ivyPath/cache/commons-configuration/commons-configuration/jars/commons-configuration-1.6.jar $systemlib
#cp $ivyPath/cache/log4j/log4j/bundles/log4j-1.2.17.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-test/jars/curator-test-2.8.0.jar $systemlib
#cp $srcPath/Storage/StorageManager/target/scala-2.11/storagemanager_2.11-0.1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.jpmml/pmml-evaluator/pmml-evaluator-1.2.9.jar $systemlib
#cp $srcPath/lib_managed/jars/com.jcraft/jsch/jsch-0.1.42.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-recipes/bundles/curator-recipes-2.6.0.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.scala-lang.modules/scala-parser-combinators_2.11/scala-parser-combinators_2.11-1.0.4.jar $systemlib
#cp $ivyPath/cache/org.ow2.asm/asm/jars/asm-4.0.jar $systemlib
#cp $ivyPath/cache/org.apache.hbase/hbase-annotations/jars/hbase-annotations-1.0.2.jar $systemlib
#cp $ivyPath/cache/com.google.protobuf/protobuf-java/bundles/protobuf-java-2.6.0.jar $systemlib
#cp $ivyPath/cache/org.tukaani/xz/jars/xz-1.0.jar $systemlib
#cp $ivyPath/cache/org.codehaus.jackson/jackson-jaxrs/jars/jackson-jaxrs-1.8.3.jar $systemlib
#cp $srcPath/SampleApplication/InterfacesSamples/target/scala-2.11/interfacessamples_2.11-1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/junit/junit/junit-4.12.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-http_2.11/bundles/spray-http_2.11-1.3.3.jar $systemlib
#cp $ivyPath/cache/org.xerial.snappy/snappy-java/bundles/snappy-java-1.1.1.7.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.httpcomponents/httpcore/httpcore-4.2.4.jar $systemlib
#cp $ivyPath/cache/org.jvnet.mimepull/mimepull/jars/mimepull-1.9.5.jar $systemlib
#cp $ivyPath/cache/org.mortbay.jetty/servlet-api/jars/servlet-api-2.5.20110712.jar $systemlib
#cp $ivyPath/cache/org.apache.logging.log4j/log4j-core/jars/log4j-core-2.4.1.jar $systemlib
#cp $ivyPath/cache/org.mortbay.jetty/jetty/jars/jetty-6.1.26.jar $systemlib
#cp $ivyPath/cache/javax.activation/activation/jars/activation-1.1.jar $systemlib
#cp $ivyPath/cache/com.sdicons.jsontools/jsontools-core/jars/jsontools-core-1.7-sources.jar $systemlib
#cp $srcPath/HeartBeat/target/scala-2.11/heartbeat_2.11-0.1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hbase/hbase-protocol/hbase-protocol-1.0.2.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-beanutils/commons-beanutils/commons-beanutils-1.8.3.jar $systemlib
#cp $ivyPath/cache/org.scalameta/dialects_2.11/jars/dialects_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/jars/com.esotericsoftware.minlog/minlog/minlog-1.2.jar $systemlib
#cp $ivyPath/cache/antlr/antlr/jars/antlr-2.7.7.jar $systemlib
#cp $srcPath/MetadataAPIServiceClient/target/scala-2.11/metadataapiserviceclient_2.11-0.1.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-framework/bundles/curator-framework-2.6.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.scala-lang/scala-library/scala-library-2.11.7.jar $systemlib
## cp $srcPath/Utils/SaveContainerDataComponent/target/scala-2.11/savecontainerdatacomponent_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/org.apache.curator/curator-client/bundles/curator-client-2.6.0.jar $systemlib
#cp $srcPath/Storage/HashMap/target/scala-2.11/hashmap_2.11-0.1.0.jar $systemlib
#cp $srcPath/InputOutputAdapters/FileSimpleInputOutputAdapters/target/scala-2.11/filesimpleinputoutputadapters_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/javax.servlet.jsp/jsp-api/jars/jsp-api-2.1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.cassandra/cassandra-thrift/cassandra-thrift-2.0.3.jar $systemlib
#cp $ivyPath/cache/com.sun.jersey/jersey-json/bundles/jersey-json-1.9.jar $systemlib
#cp $srcPath/KamanjaManager/target/scala-2.11/kamanjamanager_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/org.ow2.asm/asm-tree/jars/asm-tree-4.0.jar $systemlib
#cp $ivyPath/cache/commons-io/commons-io/jars/commons-io-2.4.jar $systemlib
#cp $srcPath/lib_managed/jars/org.joda/joda-convert/joda-convert-1.6.jar $systemlib
#cp $ivyPath/cache/net.sf.jopt-simple/jopt-simple/jars/jopt-simple-3.2.jar $systemlib
#cp $ivyPath/cache/com.github.stephenc.findbugs/findbugs-annotations/jars/findbugs-annotations-1.3.9-1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.json4s/json4s-ast_2.11/json4s-ast_2.11-3.2.9.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.thrift/libthrift/libthrift-0.9.2.jar $systemlib
#cp $srcPath/Exceptions/target/scala-2.11/exceptions_2.11-1.0.jar $systemlib
#cp $srcPath/EnvContexts/SimpleEnvContextImpl/target/scala-2.11/simpleenvcontextimpl_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/commons-cli/commons-cli/jars/commons-cli-1.2.jar $systemlib
#cp $srcPath/lib_managed/jars/javax.servlet.jsp/jsp-api/jsp-api-2.1.jar $systemlib
#cp $ivyPath/cache/com.yammer.metrics/metrics-core/jars/metrics-core-2.2.0.jar $systemlib
#cp $srcPath/FileDataConsumer/target/scala-2.11/filedataconsumer_2.11-0.1.0.jar $systemlib
#cp $ivyPath/cache/org.mapdb/mapdb/bundles/mapdb-1.0.6.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.sun.jersey/jersey-core/jersey-core-1.9.jar $systemlib
## cp $srcPath/Pmml/MethodExtractor/target/scala-2.11/methodextractor_2.11-1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/net.java.dev.jets3t/jets3t/jets3t-0.9.0.jar $systemlib
#cp $srcPath/lib_managed/bundles/com.google.protobuf/protobuf-java/protobuf-java-2.6.0.jar $systemlib
#cp $ivyPath/cache/net.java.dev.jna/jna/jars/jna-3.2.7.jar $systemlib
#cp $ivyPath/cache/org.scalameta/prettyprinters_2.11/jars/prettyprinters_2.11-0.0.3.jar $systemlib
#cp $srcPath/Utils/JdbcDataCollector/target/scala-2.11/jdbcdatacollector_2.11-1.0.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.apache.shiro/shiro-core/shiro-core-1.2.3.jar $systemlib
#cp $ivyPath/cache/joda-time/joda-time/jars/joda-time-2.8.2.jar $systemlib
#cp $srcPath/Utils/SimpleKafkaProducer/target/scala-2.11/simplekafkaproducer_2.11-0.1.0.jar $systemlib
#cp $ivyPath/cache/voldemort/voldemort/jars/voldemort-0.96.jar $systemlib
#cp $srcPath/lib_managed/jars/asm/asm/asm-3.1.jar $systemlib
#cp $srcPath/Utils/UtilsForModels/target/scala-2.11/utilsformodels_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/com.fasterxml.jackson.core/jackson-core/bundles/jackson-core-2.3.1.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-pool/commons-pool/commons-pool-1.5.4.jar $systemlib
#cp $ivyPath/cache/org.json4s/json4s-ast_2.11/jars/json4s-ast_2.11-3.2.9.jar $systemlib
##cp $srcPath/lib_managed/jars/org.scalameta/prettyprinters_2.11/prettyprinters_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/commons-lang/commons-lang/jars/commons-lang-2.6.jar $systemlib
#cp $ivyPath/cache/commons-digester/commons-digester/jars/commons-digester-1.8.1.jar $systemlib
#cp $ivyPath/cache/org.slf4j/slf4j-api/jars/slf4j-api-1.7.10.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hbase/hbase-annotations/hbase-annotations-1.0.2.jar $systemlib
#cp $ivyPath/cache/org.apache.hbase/hbase-common/jars/hbase-common-1.0.2.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-testkit_2.11/jars/spray-testkit_2.11-1.3.3.jar $systemlib
#cp $ivyPath/cache/net.debasishg/redisclient_2.11/jars/redisclient_2.11-2.13.jar $systemlib
#cp $ivyPath/cache/org.scalameta/tokenquasiquotes_2.11/jars/tokenquasiquotes_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/jars/com.thoughtworks.paranamer/paranamer/paranamer-2.6.jar $systemlib
#cp $ivyPath/cache/org.scala-lang/scala-compiler/jars/scala-compiler-2.11.0.jar $systemlib
#cp $ivyPath/cache/org.scala-lang/scala-compiler/jars/scala-compiler-2.11.7.jar $systemlib
#cp $srcPath/Utils/ZooKeeper/CuratorLeaderLatch/target/scala-2.11/zookeeperleaderlatch_2.11-1.0.jar $systemlib
##cp $srcPath/lib_managed/jars/org.scalameta/tokenquasiquotes_2.11/tokenquasiquotes_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/com.google.guava/guava/bundles/guava-14.0.1.jar $systemlib
#cp $ivyPath/cache/com.sun.jersey/jersey-server/bundles/jersey-server-1.9.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.httpcomponents/httpclient/httpclient-4.2.5.jar $systemlib
#cp $ivyPath/cache/com.twitter/chill_2.11/jars/chill_2.11-0.5.0.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.mapdb/mapdb/mapdb-1.0.6.jar $systemlib
#cp $srcPath/Pmml/PmmlRuntime/target/scala-2.11/pmmlruntime_2.11-1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.mortbay.jetty/jetty/jetty-6.1.26.jar $systemlib
#cp $srcPath/lib_managed/jars/com.sun.xml.bind/jaxb-impl/jaxb-impl-2.2.3-1.jar $systemlib
#cp $srcPath/lib_managed/jars/com.googlecode.json-simple/json-simple/json-simple-1.1.jar $systemlib
#cp $srcPath/AuditAdapters/AuditAdapterBase/target/scala-2.11/auditadapterbase_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/javax.xml.stream/stax-api/jars/stax-api-1.0-2.jar $systemlib
#cp $ivyPath/cache/commons-pool/commons-pool/jars/commons-pool-1.6.jar $systemlib
#cp $ivyPath/cache/org.apache.hadoop/hadoop-auth/jars/hadoop-auth-2.7.1.jar $systemlib
#cp $srcPath/Pmml/PmmlUdfs/target/scala-2.11/pmmludfs_2.11-1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.scala-lang/scalap/scalap-2.11.0.jar $systemlib
#cp $ivyPath/cache/org.codehaus.jettison/jettison/bundles/jettison-1.1.jar $systemlib
#cp $srcPath/lib_managed/jars/org.codehaus.jackson/jackson-mapper-asl/jackson-mapper-asl-1.9.13.jar $systemlib
#cp $srcPath/Utils/NodeInfoExtract/target/scala-2.11/nodeinfoextract_2.11-1.0.jar $systemlib
#cp $srcPath/Utils/UtilitySerivce/target/scala-2.11/utilityservice_2.11-1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.json4s/json4s-jackson_2.11/json4s-jackson_2.11-3.2.9.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-lang3/jars/commons-lang3-3.1.jar $systemlib
#cp $ivyPath/cache/com.datastax.cassandra/cassandra-driver-core/bundles/cassandra-driver-core-2.1.2.jar $systemlib
#cp $ivyPath/cache/org.apache.shiro/shiro-core/bundles/shiro-core-1.2.3.jar $systemlib
#cp $ivyPath/cache/commons-beanutils/commons-beanutils/jars/commons-beanutils-1.8.3.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-collections/commons-collections/commons-collections-3.2.1.jar $systemlib
#cp $srcPath/lib_managed/bundles/org.scala-lang.modules/scala-xml_2.11/scala-xml_2.11-1.0.4.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.commons/commons-pool2/commons-pool2-2.3.jar $systemlib
#cp $srcPath/lib_managed/jars/org.tukaani/xz/xz-1.0.jar $systemlib
#cp $ivyPath/cache/org.mortbay.jetty/jetty-sslengine/jars/jetty-sslengine-6.1.26.jar $systemlib
#cp $srcPath/MessageDef/target/scala-2.11/messagedef_2.11-1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.codehaus.jackson/jackson-jaxrs/jackson-jaxrs-1.8.3.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-pool2/jars/commons-pool2-2.3.jar $systemlib
#cp $ivyPath/cache/com.sleepycat/je/jars/je-4.0.92.jar $systemlib
#cp $srcPath/lib_managed/jars/org.slf4j/slf4j-log4j12/slf4j-log4j12-1.7.10.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-io/commons-io/commons-io-2.4.jar $systemlib
#cp $srcPath/SampleApplication/CustomUdfLib/target/scala-2.11/customudflib_2.11-1.0.jar $systemlib
##cp $srcPath/lib_managed/jars/org.scalameta/quasiquotes_2.11/quasiquotes_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-codec/commons-codec/commons-codec-1.10.jar $systemlib
#cp $ivyPath/cache/com.thoughtworks.paranamer/paranamer/jars/paranamer-2.6.jar $systemlib
#cp $ivyPath/cache/javax.servlet/servlet-api/jars/servlet-api-2.5.jar $systemlib
#cp $srcPath/lib_managed/jars/org.json4s/json4s-core_2.11/json4s-core_2.11-3.2.9.jar $systemlib
#cp $ivyPath/cache/com.google.code.gson/gson/jars/gson-2.2.4.jar $systemlib
#cp $srcPath/SecurityAdapters/SecurityAdapterBase/target/scala-2.11/securityadapterbase_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/org.ow2.asm/asm-commons/jars/asm-commons-4.0.jar $systemlib
#cp $srcPath/lib_managed/jars/com.twitter/chill-java/chill-java-0.5.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.hamcrest/hamcrest-core/hamcrest-core-1.3.jar $systemlib
#cp $srcPath/lib_managed/jars/org.jpmml/pmml-schema/pmml-schema-1.2.9.jar $systemlib
#cp $ivyPath/cache/org.scalameta/trees_2.11/jars/trees_2.11-0.0.3.jar $systemlib
#cp $srcPath/Storage/HBase/target/scala-2.11/hbase_2.11-0.1.0.jar $systemlib
#cp $ivyPath/cache/joda-time/joda-time/jars/joda-time-2.9.1.jar $systemlib
#cp $ivyPath/cache/org.apache.cassandra/cassandra-thrift/jars/cassandra-thrift-2.0.3.jar $systemlib
#cp $srcPath/BaseTypes/target/scala-2.11/basetypes_2.11-0.1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/com.google.code.findbugs/jsr305/jsr305-3.0.0.jar $systemlib
#cp $ivyPath/cache/org.apache.hadoop/hadoop-common/jars/hadoop-common-2.7.1.jar $systemlib
#cp $srcPath/BaseFunctions/target/scala-2.11/basefunctions_2.11-0.1.0.jar $systemlib
#cp $ivyPath/cache/commons-codec/commons-codec/jars/commons-codec-1.10.jar $systemlib
#cp $ivyPath/cache/junit/junit/jars/junit-4.12.jar $systemlib
#cp $srcPath/Pmml/PmmlCompiler/target/scala-2.11/pmmlcompiler_2.11-1.0.jar $systemlib
#cp $srcPath/lib_managed/jars/org.objenesis/objenesis/objenesis-1.2.jar $systemlib
#cp $ivyPath/cache/org.codehaus.jackson/jackson-core-asl/jars/jackson-core-asl-1.9.13.jar $systemlib
#cp $srcPath/lib_managed/jars/org.apache.hbase/hbase-common/hbase-common-1.0.2.jar $systemlib
#cp $srcPath/DataDelimiters/target/scala-2.11/datadelimiters_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/jline/jline/jars/jline-0.9.94.jar $systemlib
#cp $ivyPath/cache/org.scala-lang/scalap/jars/scalap-2.11.0.jar $systemlib
#cp $srcPath/MetadataBootstrap/Bootstrap/target/scala-2.11/bootstrap_2.11-1.0.jar $systemlib
#cp $ivyPath/cache/org.apache.commons/commons-math3/jars/commons-math3-3.1.1.jar $systemlib
#cp $srcPath/Storage/Cassandra/target/scala-2.11/cassandra_2.11-0.1.0.jar $systemlib
#cp $srcPath/Utils/Security/SimpleApacheShiroAdapter/target/scala-2.11/simpleapacheshiroadapter_2.11-1.0.jar $systemlib
##cp $srcPath/lib_managed/jars/org.scalameta/tokenizers_2.11/tokenizers_2.11-0.0.3.jar $systemlib
#cp $ivyPath/cache/xmlenc/xmlenc/jars/xmlenc-0.52.jar $systemlib
#cp $srcPath/lib_managed/jars/org.jpmml/pmml-model/pmml-model-1.2.9.jar $systemlib
#cp $ivyPath/cache/org.apache.httpcomponents/httpcore/jars/httpcore-4.1.2.jar $systemlib
#cp $srcPath/lib_managed/jars/commons-dbcp/commons-dbcp/commons-dbcp-1.4.jar $systemlib
#cp $srcPath/lib_managed/jars/javax.xml.stream/stax-api/stax-api-1.0-2.jar $systemlib
##cp $srcPath/lib_managed/jars/org.scalameta/trees_2.11/trees_2.11-0.0.3.jar $systemlib
#cp $srcPath/lib_managed/jars/org.jruby.joni/joni/joni-2.1.2.jar $systemlib
#cp $ivyPath/cache/com.codahale.metrics/metrics-core/bundles/metrics-core-3.0.2.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-httpx_2.11/bundles/spray-httpx_2.11-1.3.3.jar $systemlib
#cp $ivyPath/cache/commons-codec/commons-codec/jars/commons-codec-1.4.jar $systemlib
#cp $ivyPath/cache/com.101tec/zkclient/jars/zkclient-0.6.jar $systemlib

# -------------------- end of generated cp commands --------------------


###### For Version-2.11 ######
#
#cp $ivyPath/cache/io.spray/spray-json_2.11/bundles/spray-json_2.11-1.3.2.jar $systemlib
#cp $ivyPath/cache/com.codahale.metrics/metrics-core/bundles/metrics-core-3.0.2.jar $systemlib
#cp $ivyPath/cache/org.json4s/json4s-ast_2.11/jars/json4s-ast_2.11-3.2.9.jar $systemlib
#cp $ivyPath/cache/io.spray/spray-testkit_2.11/jars/spray-testkit_2.11-1.3.3.jar $systemlib

cp $srcPath/Utils/Migrate/MigrateBase/target/migratebase-1.0.jar $systemlib
cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_3/target/scala-2.11/migrateto_v_1_3_2.11-1.0.jar $systemlib
cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_4/target/scala-2.11/migrateto_v_1_4_2.11-1.0.jar $systemlib


#copy jars for kamanjainstallapplib

cp $srcPath/Utils/Migrate/MigrateBase/target/migratebase-1.0.jar $kamanjainstallsystemlib
# not found
#cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_1/target/scala-2.11/migratefrom_v_1_1_2.10-1.0.jar $kamanjainstallsystemlib
#cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_2/target/scala-2.11/migratefrom_v_1_2_2.10-1.0.jar $kamanjainstallsystemlib

cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_3/target/scala-2.11/migratefrom_v_1_3_2.11-1.0.jar $kamanjainstallsystemlib
cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_3/target/scala-2.11/migrateto_v_1_3_2.11-1.0.jar $kamanjainstallsystemlib
cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_4/target/scala-2.11/migrateto_v_1_4_2.11-1.0.jar $kamanjainstallsystemlib


#cp $srcPath/Storage/Cassandra/target/scala-2.11/*.jar $systemlib
#cp $srcPath/Storage/HashMap/target/scala-2.11/*.jar $systemlib
#cp $srcPath/Storage/HBase/target/scala-2.11/*.jar $systemlib
##cp $srcPath/Storage/Redis/target/scala-2.11/*.jar $systemlib
#cp $srcPath/Storage/StorageBase/target/scala-2.11/storagebase_2.11-1.0.jar $systemlib
#cp $srcPath/Storage/StorageManager/target/scala-2.11/*.jar $systemlib
#cp $srcPath/Storage/TreeMap/target/scala-2.11/*.jar $systemlib
##cp $srcPath/Storage/Voldemort/target/scala-2.11/*.jar $systemlib
#cp $srcPath/InputOutputAdapters/InputOutputAdapterBase/target/scala-2.11/*.jar $systemlib
#cp $srcPath/KamanjaUtils/target/scala-2.11/kamanjautils_2.11-1.0.jar $systemlib
#cp $srcPath/SecurityAdapters/SecurityAdapterBase/target/scala-2.11/*.jar $systemlib
cp $srcPath/Utils/ClusterInstaller/ClusterInstallerDriver/target/*.jar $systemlib
cp $srcPath/Utils/ClusterInstaller/InstallDriver/target/scala-2.11/*.jar $systemlib
cp $srcPath/Utils/ClusterInstaller/InstallDriverBase/target/*.jar $systemlib
cp $srcPath/Utils/ClusterInstaller/GetComponent/target/scala-2.11/*.jar $systemlib

# cp $srcPath/Utils/SaveContainerDataComponent/target/scala-2.11/SaveContainerDataComponent* $systemlib
#cp $srcPath/Utils/UtilsForModels/target/scala-2.11/utilsformodels*.jar $systemlib

# sample configs
#echo "copy sample configs..."
cp $srcPath/Utils/KVInit/src/main/resources/*cfg $systemlib

# Generate keystore file
#echo "generating keystore..."
#keytool -genkey -keyalg RSA -alias selfsigned -keystore $installPath/config/keystore.jks -storepass password -validity 360 -keysize 2048

#copy kamanja to bin directory
cp $srcPath/Utils/Script/scala-2.11/kamanja $bin
#cp $srcPath/Utils/Script/MedicalApp.sh $bin
cp $srcPath/MetadataAPI/target/scala-2.11/classes/HelpMenu.txt $installPath/Kamanja-$ver211/input
# *******************************
# COPD messages data prep
# *******************************

# Prepare test messages and copy them into place

echo "Prepare test messages and copy them into place..."
# *******************************
# Copy documentation files
# *******************************
cd $srcPath/Documentation
cp -rf * $installPath/Kamanja-$ver211/documentation

# *******************************
# Copy ClusterInstall
# *******************************
# mkdir -p $installPath/Kamanja-$ver211/ClusterInstall
# cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.11/* $installPath/Kamanja-$ver211/ClusterInstall/
# cp $srcPath/Utils/NodeInfoExtract/target/scala-2.11/NodeInfoExtract* $installPath/Kamanja-$ver211/ClusterInstall/
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.11/*.sh $kamanjainstallbin
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.11/*log4*.xml $kamanjainstallconfig
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.11/*.json $kamanjainstallconfig
cp -rf $srcPath/SampleApplication/ClusterInstall/scala-2.11/*.properties $kamanjainstallconfig
cp $srcPath/Utils/ClusterInstaller/ClusterInstallerDriver/src/main/resources/log4j2.xml $kamanjainstallconfig

# *******************************
# copy models, messages, containers, config, scripts, types  messages data prep
# *******************************

#HelloWorld
cd $srcPath/SampleApplication/HelloWorld/data
cp * $installPath/Kamanja-$ver211/input/SampleApplications/data

cd $srcPath/SampleApplication/HelloWorld/message
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/HelloWorld/model
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/HelloWorld/template
cp -rf * $installPath/Kamanja-$ver211/input/SampleApplications/template


cd $srcPath/SampleApplication/HelloWorld/config
cp -rf * $installPath/Kamanja-$ver211/config
#HelloWorld

#Medical
cd $srcPath/SampleApplication/Medical/SampleData
cp *.csv $installPath/Kamanja-$ver211/input/SampleApplications/data
cp *.csv.gz $installPath/Kamanja-$ver211/input/SampleApplications/data

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Containers
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/Medical/Functions
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/function

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Messages
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/Medical/Models
cp *.* $installPath/Kamanja-$ver211/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/Medical/Types
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/type

cd $srcPath/SampleApplication/Medical/template
cp -rf * $installPath/Kamanja-$ver211/input/SampleApplications/template

cd $srcPath/SampleApplication/Medical/Configs
cp -rf * $installPath/Kamanja-$ver211/config
#Medical

#Telecom
cd $srcPath/SampleApplication/Telecom/data
cp * $installPath/Kamanja-$ver211/input/SampleApplications/data

cd $srcPath/SampleApplication/Telecom/metadata/container
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/Telecom/metadata/message
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/Telecom/metadata/model
cp *.* $installPath/Kamanja-$ver211/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/Telecom/metadata/template
cp -rf * $installPath/Kamanja-$ver211/input/SampleApplications/template

cd $srcPath/SampleApplication/Telecom/metadata/config
cp -rf * $installPath/Kamanja-$ver211/config
#Telecom

#Finance
cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/data
cp * $installPath/Kamanja-$ver211/input/SampleApplications/data

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/container
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/message
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/model
cp *.* $installPath/Kamanja-$ver211/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/type
cp * $installPath/Kamanja-$ver211/input/SampleApplications/metadata/type

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/template
cp -rf * $installPath/Kamanja-$ver211/input/SampleApplications/template

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/config
cp -rf * $installPath/Kamanja-$ver211/config
#Finance

cd $srcPath/SampleApplication/EasyInstall/template/scala-2.11
cp -rf * $installPath/Kamanja-$ver211/template

cd $srcPath/SampleApplication/EasyInstall
cp SetPaths.sh $installPath/Kamanja-$ver211/bin/

bash $installPath/Kamanja-$ver211/bin/SetPaths.sh $KafkaRootDir

chmod 0700 $installPath/Kamanja-$ver211/input/SampleApplications/bin/*sh

#Migration and cluster Install*****************

chmod 0700 $installPath/KamanjaInstall-$ver210/bin/*.sh
chmod 0700 $installPath/KamanjaInstall-$ver211/bin/*.sh

#Migration and Cluster Install*****************

cd $installPath
tar -cvzf Kamanja-$ver210.tar.gz Kamanja-$ver210
tar -cvzf Kamanja-$ver211.tar.gz Kamanja-$ver211
tar -cvzf KamanjaInstall-$ver210.tar.gz KamanjaInstall-$ver210
tar -cvzf KamanjaInstall-$ver211.tar.gz KamanjaInstall-$ver211

echo "Kamanja install complete..."





