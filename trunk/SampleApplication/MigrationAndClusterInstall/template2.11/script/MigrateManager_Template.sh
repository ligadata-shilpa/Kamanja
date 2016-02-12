#!/bin/sh

MIGRATION_HOME={InstallDirectory}/MigrationAndClusterInstall-2.11/MigrateAndClusterInstall

java -Dlog4j.configurationFile=file:$MIGRATION_HOME/config/log4j2.xml -jar $MIGRATION_HOME/bin/MigrateManager-1.0 -–config $MIGRATION_HOME/config/MigrateConfig.json
