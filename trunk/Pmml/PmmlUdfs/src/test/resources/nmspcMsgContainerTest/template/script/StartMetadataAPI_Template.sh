#!/bin/bash

ipport="8998"

if [ "$1" != "debug" ]; then
	java -jar {InstallDirectory}/bin/MetadataAPI-1.0 --config {InstallDirectory}/input/Medical/metadata/config/MetadataAPIConfig.properties
else	
	java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -jar {InstallDirectory}/bin/MetadataAPI-1.0 --config {InstallDirectory}/input/Medical/metadata/config/MetadataAPIConfig.properties
fi

