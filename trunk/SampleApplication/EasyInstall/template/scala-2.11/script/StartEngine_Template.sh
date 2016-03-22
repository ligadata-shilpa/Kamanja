#!/bin/sh
KAMANJA_HOME={InstallDirectory}

# Start the engine with hashdb backed metadata configuration.  The zookeeper and your queue software should be running
ipport="8998"

if [ "$1" != "debug" ]; then
	java -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs_2.11-1.0:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.11-1.0:{InstallDirectory}/bin/KamanjaManager-1.0 com.ligadata.kamanjamanager --config $KAMANJA_HOME/config/Engine1Config.properties
else
	java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs_2.11-1.0:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.11-1.0:{InstallDirectory}/bin/KamanjaManager-1.0 com.ligadata.kamanjamanager --config $KAMANJA_HOME/config/Engine1Config.properties
fi
