#!/bin/sh
JVMOPTIONS=$@
KAMANJA_HOME={InstallDirectory}

# Start the engine with hashdb backed metadata configuration.  The zookeeper and your queue software should be running
ipport="8998"

if [ "$1" != "debug" ]; then
	java $JVMOPTIONS -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -jar $KAMANJA_HOME/bin/KamanjaManager-1.0 --config $KAMANJA_HOME/config/Engine1Config.properties
else
	java $JVMOPTIONS -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -jar $KAMANJA_HOME/bin/KamanjaManager-1.0 --config $KAMANJA_HOME/config/Engine1Config.properties
fi
