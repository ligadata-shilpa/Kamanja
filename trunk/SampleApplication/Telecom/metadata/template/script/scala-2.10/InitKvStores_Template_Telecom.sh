#!/usr/bin/env bash
KAMANJA_HOME={InstallDirectory}

java -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs2_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/ExtDependencyLibs_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/kvinit_2.10-1.4.0.jar com.ligadata.tools.kvinit.KVInit --typename com.ligadata.kamanja.samples.containers.AccountAggregatedUsage          --config $KAMANJA_HOME/config/Engine1Config.properties --datafiles $KAMANJA_HOME/input/SampleApplications/data/AccountAggregatedUsage_Telecom.dat           --keyfields actNo --delimiter "," --ignorerecords 1 --format "delimited"
java -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs2_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/ExtDependencyLibs_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/kvinit_2.10-1.4.0.jar com.ligadata.tools.kvinit.KVInit --typename com.ligadata.kamanja.samples.containers.AccountInfo                     --config $KAMANJA_HOME/config/Engine1Config.properties --datafiles $KAMANJA_HOME/input/SampleApplications/data/AccountInfo_Telecom.dat                      --keyfields actNo --delimiter "," --ignorerecords 1 --format "delimited"
java -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs2_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/ExtDependencyLibs_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/kvinit_2.10-1.4.0.jar com.ligadata.tools.kvinit.KVInit --typename com.ligadata.kamanja.samples.containers.SubscriberAggregatedUsage       --config $KAMANJA_HOME/config/Engine1Config.properties --datafiles $KAMANJA_HOME/input/SampleApplications/data/SubscriberAggregatedUsage_Telecom.dat        --keyfields msisdn --delimiter "," --ignorerecords 1 --format "delimited"
java -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs2_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/ExtDependencyLibs_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/kvinit_2.10-1.4.0.jar com.ligadata.tools.kvinit.KVInit --typename com.ligadata.kamanja.samples.containers.SubscriberGlobalPreferences     --config $KAMANJA_HOME/config/Engine1Config.properties --datafiles $KAMANJA_HOME/input/SampleApplications/data/SubscriberGlobalPreferences_Telecom.dat      --keyfields PrefType --delimiter "," --ignorerecords 1 --format "delimited"
java -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs2_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/ExtDependencyLibs_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/kvinit_2.10-1.4.0.jar com.ligadata.tools.kvinit.KVInit --typename com.ligadata.kamanja.samples.containers.SubscriberInfo                  --config $KAMANJA_HOME/config/Engine1Config.properties --datafiles $KAMANJA_HOME/input/SampleApplications/data/SubscriberInfo_Telecom.dat                   --keyfields msisdn --delimiter "," --ignorerecords 1 --format "delimited"
java -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -cp $KAMANJA_HOME/lib/system/ExtDependencyLibs2_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/ExtDependencyLibs_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/KamanjaInternalDeps_2.10-1.4.0.jar:$KAMANJA_HOME/lib/system/kvinit_2.10-1.4.0.jar com.ligadata.tools.kvinit.KVInit --typename com.ligadata.kamanja.samples.containers.SubscriberPlans                 --config $KAMANJA_HOME/config/Engine1Config.properties --datafiles $KAMANJA_HOME/input/SampleApplications/data/SubscriberPlans_Telecom.dat                  --keyfields planName --delimiter "," --ignorerecords 1 --format "delimited"
