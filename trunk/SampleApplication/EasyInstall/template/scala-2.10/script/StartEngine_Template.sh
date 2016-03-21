#!/bin/sh
KAMANJA_HOME={InstallDirectory}

# Start the engine with hashdb backed metadata configuration.  The zookeeper and your queue software should be running
ipport="8998"

if [ "$1" != "debug" ]; then
	#java -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -jar $KAMANJA_HOME/bin/KamanjaManager-1.0 --config $KAMANJA_HOME/config/Engine1Config.properties
    java -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs-1.0:$KAMANJA_HOME/lib/system/KamanjaInternalDeps-assembly-1.0.jar:$KAMANJA_HOME/bin/KamanajaManager-1.0 com.ligadata.KamanjaManager --config $KAMANJA_HOME/config/Engine1Config.properties
else
	#java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -jar $KAMANJA_HOME/bin/KamanjaManager-1.0 --config $KAMANJA_HOME/config/Engine1Config.properties
	java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs-1.0:$KAMANJA_HOME/lib/system/KamanjaInternalDeps-assembly-1.0.jar:$KAMANJA_HOME/bin/KamanajaManager-1.0 com.ligadata.KamanjaManager --config $KAMANJA_HOME/config/Engine1Config.properties
fi

