#!/bin/bash

# Start the engine with cassandra backed metadata configuration.  Cassandra must be running, not to mention the zookeeper and your queue software 
ipport="8998"

if [ "$1" != "debug" ]; then
	java -Dlog4j.configurationFile=file:{InstallDirectory}/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs_2.11-1.0:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.11-1.0:{InstallDirectory}/bin/KamanjaManager-1.0 com.ligadata.kamanjamanager --config {InstallDirectory}/config/Engine1Config_Cassandra.properties
else
	java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -Dlog4j.configurationFile=file:{InstallDirectory}/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs_2.11-1.0:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.11-1.0:{InstallDirectory}/bin/KamanjaManager-1.0 com.ligadata.kamanjamanager --config {InstallDirectory}/config/Engine1Config_Cassandra.properties
fi
