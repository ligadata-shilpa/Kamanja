Scenario:

An output adapter message binding has been configured for csv on adapter "TestOut_1".  The customer has changed his mind, desiring json serialization. To fix, it is decided to use an unused adapter, "TestOut_2" with a new kafka topic "testout_2".

Problem:

The initial configuration produces data in csv.  The change is made to json and bound to the unused adapter.  Nothing is produced on subsequent data pushes.

**Steps to reproduce**

0.  start zookeeper, kafka, hbase (or cassandra), set variables, and _build config files with cassandra references.  Change parameter the storeType value to hbase if desired._

export KAMANJA_HOME=/tmp/drdigital/Kamanja-1.4.0_2.11
export KAMANJA_SRCDIR=~/github/dev/1.4.0.Base/kamanja/trunk
export TestBin=$KAMANJA_SRCDIR/MetadataAPI/src/test/resources/bin
export MetadataDir=$KAMANJA_SRCDIR/MetadataAPI/src/test/resources/Metadata
export apiConfigProperties=$KAMANJA_HOME/config/MetadataAPIConfig.properties


_Configure the cluster configuration json file with cassandra storage values... **NOTE: THE CURRENT ClusterConfig.json is overwritten**_

    setPathsFor.scala --installDir $KAMANJA_HOME --templateFile $MetadataDir/templates/ClusterConfig.1.4.0_Template.json --scalaHome $SCALA_HOME --javaHome $JAVA_HOME --storeType cassandra --schemaName testdata --schemaLocation localhost > $KAMANJA_HOME/config/ClusterConfig.json

_Configure the metadata api config file to use... cassandra in this case.  **NOTE: THE CURRENT MetadataAPIConfig.properties is overwritten**_

    setPathsFor.scala --installDir $KAMANJA_HOME --templateFile $MetadataDir/templates/Cassandra_MetadataAPIConfig_Template.properties --scalaHome $SCALA_HOME --javaHome $JAVA_HOME --storeType cassandra --schemaName metadata --schemaLocation localhost > $KAMANJA_HOME/config/MetadataAPIConfig.properties

_Configure the engine config file to use.  In this case, the cassandra values are used. **NOTE: THE CURRENT Engine1Config.properties is overwritten**_

    setPathsFor.scala --installDir $KAMANJA_HOME --templateFile $MetadataDir/templates/Engine1Config_Template.properties --scalaHome $SCALA_HOME --javaHome $JAVA_HOME --storeType cassandra --schemaName metadata --schemaLocation localhost > $KAMANJA_HOME/config/Engine1Config.properties

1. cluster config modifications
  a. add new output adapter below to the generated cluster config from prior step :
    
    {
      "Name": "TestOut_2",
      "TypeString": "Output",
      "TenantId": "tenant1",
      "ClassName": "com.ligadata.OutputAdapters.KafkaProducer$",
      "JarName": "KamanjaInternalDeps_2.11-1.4.0.jar",
      "DependencyJars": [
        "ExtDependencyLibs_2.11-1.4.0.jar",
        "ExtDependencyLibs2_2.11-1.4.0.jar"
      ],
      "AdapterSpecificCfg": {
        "HostList": "localhost:9092",
        "TopicName": "testout_2"
      }
    }, 


  b. Set the NOTIFY_ENGINE=YES in $KAMANJA_HOME/config/MetadataAPIConfig.properties

  c. initialize cluster configuration

    $KAMANJA_HOME/bin/kamanja $apiConfigProperties upload cluster config $KAMANJA_HOME/config/ClusterConfig.json 

2. Start the engine in another console

    $KAMANJA_HOME/bin/kamanja start -v

3. Check kamanja manger, kafka, zk, and db are running

    jps

4. Install standard Helloworld metadata

    $KAMANJA_HOME/input/SampleApplications/bin/HelloWorldApp.sh
    
5. List the adapter message configurations if desired

    $KAMANJA_HOME/bin/kamanja $apiConfigProperties list adaptermessagebindings

6. Start the WatchOutputQueue in another console window.

    $KAMANJA_HOME/bin/WatchOutputQueue.sh

7. Push the hello world data

    $KAMANJA_HOME/input/SampleApplications/bin/PushSampleDataToKafka_HelloWorld.sh 

    Expectation: data in the WatchOutputQueue window (2 lines)

    This data was processed by testout_1 adapter.

8. Remove the old binding for outmsg1 on testout_1

    $KAMANJA_HOME/bin/kamanja $apiConfigProperties remove adaptermessagebinding KEY "testout_1,com.ligadata.kamanja.samples.messages.outmsg1,com.ligadata.kamanja.serializer.csvserdeser"

7. Create a new binding for "outmsg1" on the second (_unused_) adapter, "TestOut_2"  that was added in step 1 above.  This time generate json.

    $KAMANJA_HOME/bin/kamanja $apiConfigProperties add adaptermessagebinding FROMSTRING ' { "AdapterName": "TestOut_2", "MessageNames": [ "com.ligadata.kamanja.samples.messages.outmsg1" ], "Serializer": "com.ligadata.kamanja.serializer.JsonSerDeser" }'

8. Push the hello world data again.

    $KAMANJA_HOME/input/SampleApplications/bin/PushSampleDataToKafka_HelloWorld.sh 

    Expectation: data in the WatchOutputQueue window (2 lines) formatted in json

    Actual: No data 


