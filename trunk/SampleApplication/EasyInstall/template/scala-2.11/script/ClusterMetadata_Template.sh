#!/bin/bash

# Install the cluster metadata (hashdb).  If the "debug" is supplied as the first parameter, start the MetadataAPI-1.0 with the 
# debugger.

ipport="8998"

if [ "$1" != "debug" ]; then
	java -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs_2.11-1.4.0.jar:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.11-1.4.0.jar:{InstallDirectory}/bin/MetadataAPI-1.0 com.ligadata.metadataapi --config {InstallDirectory}/config/ClusterCfgMetadataAPIConfig.properties
else
	java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs_2.11-1.4.0.jar:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.11-1.4.0.jar:{InstallDirectory}/bin/MetadataAPI-1.0 com.ligadata.metadataapi --config {InstallDirectory}/config/ClusterCfgMetadataAPIConfig.properties
fi

