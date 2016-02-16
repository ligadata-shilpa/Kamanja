#!/bin/bash

# Install the cluster metadata (hashdb).  If the "debug" is supplied as the first parameter, start the MetadataAPI-1.0 with the 
# debugger.

ipport="8998"

if [ "$1" != "debug" ]; then
	java -jar {InstallDirectory}/KamanjaInstall-2.11/bin/MetadataAPI-1.0 --config {InstallDirectory}/KamanjaInstall-2.11/config/ClusterCfgMetadataAPIConfig.properties
else
	java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -jar {InstallDirectory}/KamanjaInstall-2.11/bin/MetadataAPI-1.0 --config {InstallDirectory}/KamanjaInstall-2.11/config/ClusterCfgMetadataAPIConfig.properties
fi

