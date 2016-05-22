#!/usr/bin/env bash
KAMANJA_HOME={InstallDirectory}

java -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs2_2.11-1.4.1.jar:$KAMANJA_HOME/lib/system/ExtDependencyLibs_2.11-1.4.1.jar:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.11-1.4.1.jar:$KAMANJA_HOME/lib/system/simplekafkaproducer_2.11-1.4.1.jar com.ligadata.tools.SimpleKafkaProducer --gz true --topics "telecominput" --threads 1 --topicpartitions 8 --brokerlist "localhost:9092" --files "$KAMANJA_HOME/input/SampleApplications/data/SubscriberUsage_Telecom.dat.gz" --partitionkeyidxs "0" --format CSV
