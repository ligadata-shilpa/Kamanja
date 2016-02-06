#!/usr/bin/env bash
echo "NODEID=$NODEID" >> /app/Kamanja/config/EngineConfig.properties
echo "MetadataDataStore={\"StoreType\":\"$DATASTORE\", \"SchemaName\":\"$SCHEMANAME\", \"Location\":\"$LOCATION\"}" >> /app/Kamanja/config/EngineConfig.properties
java -jar /app/Kamanja/bin/KamanjaManager-1.0 --config /app/Kamanja/config/EngineConfig.properties