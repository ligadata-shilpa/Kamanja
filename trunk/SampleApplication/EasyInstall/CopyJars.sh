#!/bin/bash

set -e

srcPath=$1
ivyPath=$2
systemlib210=$3
systemlib211=$4

if [ ! -d "$srcPath" ]; then
        echo "Not valid install path supplied.  It should be a directory that can be written to and whose current content is of no value (will be overwritten) "
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi
if [ ! -d "$ivyPath" ]; then
        echo "Not valid ivy path supplied.  It should be the ivy path for dependency the jars."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi
if [ ! -d "$systemlib210" ]; then
        echo "Not valid src path supplied.  It should be the trunk directory containing the jars, files, what not that need to be supplied."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi
if [ ! -d "$systemlib211" ]; then
        echo "Not valid src path supplied.  It should be the trunk directory containing the jars, files, what not that need to be supplied."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

srcPath=$(echo $srcPath | sed 's/[\/]*$//')
ivyPath=$(echo $ivyPath | sed 's/[\/]*$//')
systemlib210=$(echo $systemlib210 | sed 's/[\/]*$//')
systemlib211=$(echo $systemlib211 | sed 's/[\/]*$//')

# scala version 210 jars
#cp $srcPath/Storage/SqlServer/target/scala-2.10/sqlserver*.jar $systemlib210
#cp $srcPath/Utils/Audit/target/scala-2.10/auditadapters*.jar $systemlib210
#cp $srcPath/FactoriesOfModelInstanceFactory/JarFactoryOfModelInstanceFactory/target/scala-2.10/jarfactoryofmodelinstancefactory*.jar $systemlib210
#cp $srcPath/TransactionService/target/scala-2.10/transactionservice*.jar $systemlib210
#cp $srcPath/Utils/Controller/target/scala-2.10/controller*.jar $systemlib210
#cp $srcPath/Utils/ExtractData/target/scala-2.10/extractdata*.jar $systemlib210
#cp $srcPath/KvBase/target/scala-2.10/kvbase*.jar $systemlib210
#cp $srcPath/MetadataAPI/target/scala-2.10/metadataapi*.jar $systemlib210
#cp $srcPath/Storage/TreeMap/target/scala-2.10/*.jar $systemlib210
#cp $srcPath/Utils/KVInit/target/scala-2.10/kvinit*.jar $systemlib210

#cp $srcPath/Utils/Serialize/target/scala-2.10/serialize*.jar $systemlib210
#cp $srcPath/Storage/StorageBase/target/scala-2.10/storagebase*.jar $systemlib210
#cp $srcPath/MetadataAPIService/target/scala-2.10/metadataapiservice*.jar $systemlib210
#cp $srcPath/Utils/ZooKeeper/CuratorClient/target/scala-2.10/zookeeperclient*.jar $systemlib210

#cp $srcPath/Metadata/target/scala-2.10/metadata*.jar $systemlib210
#cp $srcPath/KamanjaBase/target/scala-2.10/kamanjabase*.jar $systemlib210
#cp $srcPath/Utils/CleanUtil/target/scala-2.10/cleanutil*.jar $systemlib210
#cp $srcPath/KamanjaUtils/target/scala-2.10/kamanjautils*.jar $systemlib210
#cp $srcPath/Utils/ZooKeeper/CuratorListener/target/scala-2.10/zookeeperlistener*.jar $systemlib210
#cp $srcPath/InputOutputAdapters/InputOutputAdapterBase/target/scala-2.10/*.jar $systemlib210
#cp $srcPath/Utils/JsonDataGen/target/scala-2.10/jsondatagen*.jar $systemlib210
#cp $srcPath/FactoriesOfModelInstanceFactory/JpmmlFactoryOfModelInstanceFactory/target/scala-2.10/jpmmlfactoryofmodelinstancefactory*.jar $systemlib210
#cp $srcPath/OutputMsgDef/target/scala-2.10/outputmsgdef*.jar $systemlib210
#cp $srcPath/Storage/StorageManager/target/scala-2.10/*.jar $systemlib210
#cp $srcPath/SampleApplication/InterfacesSamples/target/scala-2.10/interfacessamples*.jar $systemlib210
#cp $srcPath/HeartBeat/target/scala-2.10/heartbeat*.jar $systemlib210
#cp $srcPath/MetadataAPIServiceClient/target/scala-2.10/metadataapiserviceclient*.jar $systemlib210
#cp $srcPath/Utils/SaveContainerDataComponent/target/scala-2.10/savecontainerdatacomponent*.jar $systemlib210
#cp $srcPath/Storage/HashMap/target/scala-2.10/hashmap*.jar $systemlib210
#cp $srcPath/InputOutputAdapters/FileSimpleInputOutputAdapters/target/scala-2.10/*.jar $systemlib210
#cp $srcPath/KamanjaManager/target/scala-2.10/kamanjamanager*.jar $systemlib210
#cp $srcPath/Exceptions/target/scala-2.10/exceptions*.jar $systemlib210
#cp $srcPath/EnvContexts/SimpleEnvContextImpl/target/scala-2.10/simpleenvcontextimpl*.jar $systemlib210
#cp $srcPath/FileDataConsumer/target/scala-2.10/filedataconsumer*.jar $systemlib210
#cp $srcPath/Pmml/MethodExtractor/target/scala-2.10/methodextractor*.jar $systemlib210
#cp $srcPath/Utils/JdbcDataCollector/target/scala-2.10/jdbcdatacollector*.jar $systemlib210
#cp $srcPath/Utils/SimpleKafkaProducer/target/scala-2.10/simplekafkaproducer*.jar $systemlib210
#cp $srcPath/Utils/UtilsForModels/target/scala-2.10/utilsformodels*.jar $systemlib210
#cp $srcPath/Utils/ZooKeeper/CuratorLeaderLatch/target/scala-2.10/zookeeperleaderlatch*.jar $systemlib210
#cp $srcPath/Pmml/PmmlRuntime/target/scala-2.10/pmmlruntime*.jar $systemlib210
#cp $srcPath/AuditAdapters/AuditAdapterBase/target/scala-2.10/auditadapterbase*.jar $systemlib210
#cp $srcPath/Pmml/PmmlUdfs/target/scala-2.10/pmmludfs*.jar $systemlib210
#cp $srcPath/Utils/NodeInfoExtract/target/scala-2.10/nodeinfoextract*.jar $systemlib210
#cp $srcPath/Utils/UtilitySerivce/target/scala-2.10/utilityservice*.jar $systemlib210
#cp $srcPath/MessageDef/target/scala-2.10/messagedef*.jar $systemlib210
#cp $srcPath/SampleApplication/CustomUdfLib/target/scala-2.10/customudflib*.jar $systemlib210
#cp $srcPath/SecurityAdapters/SecurityAdapterBase/target/scala-2.10/securityadapterbase*.jar $systemlib210
#cp $srcPath/Storage/HBase/target/scala-2.10/hbase*.jar $systemlib210
#cp $srcPath/BaseTypes/target/scala-2.10/basetypes*.jar $systemlib210
#cp $srcPath/BaseFunctions/target/scala-2.10/basefunctions*.jar $systemlib210
#cp $srcPath/Pmml/PmmlCompiler/target/scala-2.10/pmmlcompiler*.jar $systemlib210
#cp $srcPath/DataDelimiters/target/scala-2.10/datadelimiters*.jar $systemlib210
#cp $ivyPath/cache/org.scala-lang/scalap/jars/scalap-2.10.0.jar $systemlib210
#cp $srcPath/MetadataBootstrap/Bootstrap/target/scala-2.10/bootstrap*.jar $systemlib210
#cp $srcPath/Storage/Cassandra/target/scala-2.10/cassandra*.jar $systemlib210
#cp $srcPath/Utils/Security/SimpleApacheShiroAdapter/target/scala-2.10/simpleapacheshiroadapter*.jar $systemlib210
#cp $srcPath/InputOutputAdapters/KafkaSimpleInputOutputAdapters/target/scala-2.10/*.jar $systemlib210
#cp $srcPath/Storage/StorageBase/target/scala-2.10/storagebase_2.10-1.0.jar $systemlib


#cp $ivyPath/cache/org.scala-lang/scala-compiler/jars/scala-compiler-2.10.0.jar $systemlib210
#cp $ivyPath/cache/org.scala-lang/scala-compiler/jars/scala-compiler-2.10.4.jar $systemlib210
#cp $ivyPath/cache/org.scala-lang/scala-compiler/jars/scala-compiler-2.10.5.jar $systemlib210
#cp $ivyPath/cache/org.scala-lang/scala-reflect/jars/scala-reflect-2.10.4.jar $systemlib210
#cp $ivyPath/cache/org.scalatest/scalatest_2.10/bundles/scalatest*.jar $systemlib210
#cp $ivyPath/cache/org.scala-lang/scala-actors/jars/scala-actors-2.10.4.jar $systemlib210
#cp $ivyPath/cache/org.scalatest/scalatest_2.10/bundles/scalatest*.jar $systemlib210
#cp $ivyPath/cache/org.parboiled/parboiled-scala_2.10/jars/parboiled-scala_2.10-1.1.7.jar $systemlib210
#cp $srcPath/lib_managed/bundles/org.scalatest/scalatest_2.10/scalatest*.jar $systemlib210
#cp $srcPath/lib_managed/jars/org.scala-lang/scala-compiler/scala-compiler-2.10.4.jar $systemlib210

#cp $srcPath/Utils/Migrate/MigrateBase/target/migratebase-1.0.jar $systemlib210
#cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_1/target/scala-2.10/migratefrom_v_1_1_2.10-1.0.jar $systemlib210
#cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_2/target/scala-2.10/migratefrom_v_1_2_2.10-1.0.jar $systemlib210
# this should be changed?
#cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_3/target/scala-2.10/migrateto_v_1_3_2.10-1.0.jar $systemlib210


# Copy scala 2.11 jars

cp $srcPath/Storage/SqlServer/target/scala-2.11/sqlserver*.jar $systemlib211
cp $srcPath/Utils/Audit/target/scala-2.11/auditadapters*.jar $systemlib211
cp $srcPath/FactoriesOfModelInstanceFactory/JarFactoryOfModelInstanceFactory/target/scala-2.11/jarfactoryofmodelinstancefactory*.jar $systemlib211
cp $srcPath/TransactionService/target/scala-2.11/transactionservice*.jar $systemlib211
cp $srcPath/Utils/Controller/target/scala-2.11/controller*.jar $systemlib211
cp $srcPath/Utils/ExtractData/target/scala-2.11/extractdata*.jar $systemlib211
cp $srcPath/KvBase/target/scala-2.11/kvbase*.jar $systemlib211
cp $srcPath/MetadataAPI/target/scala-2.11/metadataapi*.jar $systemlib211
cp $srcPath/Storage/TreeMap/target/scala-2.11/*.jar $systemlib211
cp $srcPath/Utils/KVInit/target/scala-2.11/kvinit*.jar $systemlib211

cp $srcPath/Utils/Serialize/target/scala-2.11/serialize*.jar $systemlib211
cp $srcPath/Storage/StorageBase/target/scala-2.11/storagebase*.jar $systemlib211
cp $srcPath/MetadataAPIService/target/scala-2.11/metadataapiservice*.jar $systemlib211
cp $srcPath/Utils/ZooKeeper/CuratorClient/target/scala-2.11/zookeeperclient*.jar $systemlib211

cp $srcPath/Metadata/target/scala-2.11/metadata*.jar $systemlib211
cp $srcPath/KamanjaBase/target/scala-2.11/kamanjabase*.jar $systemlib211
cp $srcPath/Utils/CleanUtil/target/scala-2.11/cleanutil*.jar $systemlib211
cp $srcPath/KamanjaUtils/target/scala-2.11/kamanjautils*.jar $systemlib211
cp $srcPath/Utils/ZooKeeper/CuratorListener/target/scala-2.11/zookeeperlistener*.jar $systemlib211
cp $srcPath/InputOutputAdapters/InputOutputAdapterBase/target/scala-2.11/*.jar $systemlib211
cp $srcPath/Utils/JsonDataGen/target/scala-2.11/jsondatagen*.jar $systemlib211
cp $srcPath/FactoriesOfModelInstanceFactory/JpmmlFactoryOfModelInstanceFactory/target/scala-2.11/jpmmlfactoryofmodelinstancefactory*.jar $systemlib211
cp $srcPath/OutputMsgDef/target/scala-2.11/outputmsgdef*.jar $systemlib211
cp $srcPath/Storage/StorageManager/target/scala-2.11/*.jar $systemlib211
cp $srcPath/SampleApplication/InterfacesSamples/target/scala-2.11/interfacessamples*.jar $systemlib211
cp $srcPath/HeartBeat/target/scala-2.11/heartbeat*.jar $systemlib211
cp $srcPath/MetadataAPIServiceClient/target/scala-2.11/metadataapiserviceclient*.jar $systemlib211
cp $srcPath/Utils/SaveContainerDataComponent/target/scala-2.11/savecontainerdatacomponent*.jar $systemlib211
cp $srcPath/Storage/HashMap/target/scala-2.11/hashmap*.jar $systemlib211
cp $srcPath/InputOutputAdapters/FileSimpleInputOutputAdapters/target/scala-2.11/*.jar $systemlib211
cp $srcPath/KamanjaManager/target/scala-2.11/kamanjamanager*.jar $systemlib211
cp $srcPath/Exceptions/target/scala-2.11/exceptions*.jar $systemlib211
cp $srcPath/EnvContexts/SimpleEnvContextImpl/target/scala-2.11/simpleenvcontextimpl*.jar $systemlib211
cp $srcPath/FileDataConsumer/target/scala-2.11/filedataconsumer*.jar $systemlib211
cp $srcPath/Pmml/MethodExtractor/target/scala-2.11/methodextractor*.jar $systemlib211
cp $srcPath/Utils/JdbcDataCollector/target/scala-2.11/jdbcdatacollector*.jar $systemlib211
cp $srcPath/Utils/SimpleKafkaProducer/target/scala-2.11/simplekafkaproducer*.jar $systemlib211
cp $srcPath/Utils/UtilsForModels/target/scala-2.11/utilsformodels*.jar $systemlib211
cp $srcPath/Utils/ZooKeeper/CuratorLeaderLatch/target/scala-2.11/zookeeperleaderlatch*.jar $systemlib211
cp $srcPath/Pmml/PmmlRuntime/target/scala-2.11/pmmlruntime*.jar $systemlib211
cp $srcPath/AuditAdapters/AuditAdapterBase/target/scala-2.11/auditadapterbase*.jar $systemlib211
cp $srcPath/Pmml/PmmlUdfs/target/scala-2.11/pmmludfs*.jar $systemlib211
cp $srcPath/Utils/NodeInfoExtract/target/scala-2.11/nodeinfoextract*.jar $systemlib211
cp $srcPath/Utils/UtilitySerivce/target/scala-2.11/utilityservice*.jar $systemlib211
cp $srcPath/MessageDef/target/scala-2.11/messagedef*.jar $systemlib211
cp $srcPath/SampleApplication/CustomUdfLib/target/scala-2.11/customudflib*.jar $systemlib211
cp $srcPath/SecurityAdapters/SecurityAdapterBase/target/scala-2.11/securityadapterbase*.jar $systemlib211
cp $srcPath/Storage/HBase/target/scala-2.11/hbase*.jar $systemlib211
cp $srcPath/BaseTypes/target/scala-2.11/basetypes*.jar $systemlib211
cp $srcPath/BaseFunctions/target/scala-2.11/basefunctions*.jar $systemlib211
cp $srcPath/Pmml/PmmlCompiler/target/scala-2.11/pmmlcompiler*.jar $systemlib211
cp $srcPath/DataDelimiters/target/scala-2.11/datadelimiters*.jar $systemlib211
cp $ivyPath/cache/org.scala-lang/scalap/jars/scalap-2.11.0.jar $systemlib211
cp $srcPath/MetadataBootstrap/Bootstrap/target/scala-2.11/bootstrap*.jar $systemlib211
cp $srcPath/Storage/Cassandra/target/scala-2.11/cassandra*.jar $systemlib211
cp $srcPath/Utils/Security/SimpleApacheShiroAdapter/target/scala-2.11/simpleapacheshiroadapter*.jar $systemlib211
cp $srcPath/InputOutputAdapters/KafkaSimpleInputOutputAdapters/target/scala-2.11/*.jar $systemlib211

cp $ivyPath/cache/org.scala-lang/scala-compiler/jars/scala-compiler-2.11.7.jar $systemlib211

cp $ivyPath/cache/org.scala-lang/scala-reflect/jars/scala-reflect-2.11.7.jar $systemlib211
cp $ivyPath/cache/org.scalatest/scalatest_2.11/bundles/scalatest*.jar $systemlib211
cp $ivyPath/cache/org.scala-lang/scala-actors/jars/scala-actors-2.11.7.jar $systemlib211
cp $ivyPath/cache/org.scalatest/scalatest_2.11/bundles/scalatest*.jar $systemlib211
cp $ivyPath/cache/org.parboiled/parboiled-scala_2.11/jars/parboiled-scala_2.11-1.1.7.jar $systemlib211
cp $srcPath/lib_managed/bundles/org.scalatest/scalatest_2.11/scalatest*.jar $systemlib211
cp $srcPath/lib_managed/jars/org.scala-lang/scala-compiler/scala-compiler-2.11.7.jar $systemlib211
cp $srcPath/Storage/StorageBase/target/scala-2.11/storagebase_2.11-1.0.jar $systemlib211


cp $srcPath/Utils/Migrate/MigrateBase/target/migratebase-1.0.jar $systemlib211
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_1/target/scala-2.11/migratefrom_v_1_1_2.11-1.0.jar $systemlib211
cp $srcPath/Utils/Migrate/SourceVersion/MigrateFrom_V_1_2/target/scala-2.11/migratefrom_v_1_2_2.11-1.0.jar $systemlib211
# this should be changed?
cp $srcPath/Utils/Migrate/DestinationVersion/MigrateTo_V_1_3/target/scala-2.11/migrateto_v_1_3_2.11-1.0.jar $systemlib211



