#!/usr/bin/env bash
###################################################################
#
#  Copyright 2015 ligaDATA
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
###################################################################


echo "Setting up paths"
KafkaRootDir=$1
if [ -d "$KafkaRootDir" ]; then
	KafkaRootDir=$(echo $KafkaRootDir | sed 's/[\/]*$//')
fi
jar_full_path=$(which jar)
if [ "$?" != "0" ]; then
	jar_full_path=$JAVA_HOME/bin/jar
	if [ $JAVA_HOME == "" ]; then
		echo "The command 'which jar' failed and the environment variable $JAVA_HOME is not set. Please set the $JAVA_HOME environment variable."
		exit 1
	fi
fi

scala_full_path=$(which scala)
if [ "$?" != "0" ]; then
	scala_full_path=$SCALA_HOME/bin/scala
	if [$SCALA_HOME == ""]; then
		echo "The command 'which scala' failed and the environment variable $SCALA_HOME is not set. Please set the $SCALA_HOME environment variable."
		exit 1
	fi
fi

pwdnm=$(pwd -P)

java_home=$(dirname $(dirname $jar_full_path))
scala_home=$(dirname $(dirname $scala_full_path))

dirnm=$(dirname "$0")
cd $dirnm

install_dir=$(dirname $(pwd -P))

java_home_repl=$(echo $java_home | sed 's/\//\\\//g')
scala_home_repl=$(echo $scala_home | sed 's/\//\\\//g')
install_dir_repl=$(echo $install_dir | sed 's/\//\\\//g')

# changing path in script files
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/script/StartEngine_Template.sh > $install_dir/bin/StartEngine.sh
sed "s/{InstallDirectory}/$install_dir_repl/g" $install_dir/template/script/ContainersUtility_Template.sh > $install_dir/bin/ContainersUtility.sh
sed "s/{InstallDirectory}/$install_dir_repl/g" $install_dir/template/script/JsonChecker_Template.sh > $install_dir/bin/JsonChecker.sh
sed "s/{InstallDirectory}/$install_dir_repl/g" $install_dir/template/script/FileDataConsumer_Template.sh > $install_dir/bin/FileDataConsumer.sh
sed "s/{InstallDirectory}/$install_dir_repl/g" $install_dir/template/script/ExtractData_Template.sh > $install_dir/bin/ExtractData.sh
sed "s/{InstallDirectory}/$install_dir_repl/g" $install_dir/template/script/JdbcDataCollector_Template.sh > $install_dir/bin/JdbcDataCollector.sh

#new one
#HelloWorld
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/SampleApplications/template/script/HelloWorldApp_Template.sh > $install_dir/input/SampleApplications/bin/HelloWorldApp.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/SampleApplications/template/script/PushSampleDataToKafka_Template_HelloWorld.sh > $install_dir/input/SampleApplications/bin/PushSampleDataToKafka_HelloWorld.sh
#HelloWorld

#Medical
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/SampleApplications/template/script/PushSampleDataToKafka_Template_Medical.sh > $install_dir/input/SampleApplications/bin/PushSampleDataToKafka_Medical.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/SampleApplications/template/script/InitKvStores_Template_Medical.sh > $install_dir/input/SampleApplications/bin/InitKvStores_Medical.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/SampleApplications/template/script/MedicalApp_Template.sh > $install_dir/input/SampleApplications/bin/MedicalApp.sh
#Medical

#Telecom
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/SampleApplications/template/script/SubscriberUsageApp_Template.sh > $install_dir/input/SampleApplications/bin/SubscriberUsageApp.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/SampleApplications/template/script/InitKvStores_Template_Telecom.sh > $install_dir/input/SampleApplications/bin/InitKvStores_Telecom.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/SampleApplications/template/script/PushSampleDataToKafka_Template_Telecom.sh > $install_dir/input/SampleApplications/bin/PushSampleDataToKafka_Telecom.sh
#Telecom

#Finance
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/SampleApplications/template/script/LowBalanceAlertApp_Template.sh > $install_dir/input/SampleApplications/bin/LowBalanceAlertApp.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/SampleApplications/template/script/InitKvStores_Template_Finance.sh > $install_dir/input/SampleApplications/bin/InitKvStores_Finance.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/SampleApplications/template/script/PushSampleDataToKafka_Template_Finance.sh > $install_dir/input/SampleApplications/bin/PushSampleDataToKafka_Finance.sh
#Finance

#new one

# logfile
sed "s/{InstallDirectory}/$install_dir_repl/g" $install_dir/template/config/log4j2_Template.xml > $install_dir/config/log4j2.xml

# changing path in config files
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/config/ClusterConfig_Template.json > $install_dir/config/ClusterConfig.json
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/config/EngineConfig_Template.properties > $install_dir/config/Engine1Config.properties
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/config/MetadataAPIConfig_Template.properties > $install_dir/config/MetadataAPIConfig.properties


# HelloWorld

# Expecting 1st Parameter as Kafka Install directory
if [ "$#" -ne 1 ] || ! [ -d "$KafkaRootDir" ]; then
	echo "WARN: Not given/found Kafka install directory. Not going to create CreateQueues.sh, WatchOutputQueue.sh, WatchStatusQueue.sh, WatchFailedEventQueue.sh and WatchInputQueue.sh"
else
	kafkatopics="$KafkaRootDir/bin/kafka-topics.sh"
	if [ ! -f "$kafkatopics" ]; then
		echo "WARN: Not found bin/kafka-topics.sh in given Kafka install directory $KafkaRootDir. Not going to create CreateQueues.sh, WatchOutputQueue.sh, WatchStatusQueue.sh and WatchInputQueue.sh"
	else
		KafkaRootDir_repl=$(echo $KafkaRootDir | sed 's/\//\\\//g')
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/CreateQueues_Template.sh > $install_dir/bin/CreateQueues.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/CreateQueue_Template.sh > $install_dir/bin/CreateQueue.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchQueue_Template.sh > $install_dir/bin/WatchQueue.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchOutputQueue_Template.sh > $install_dir/bin/WatchOutputQueue.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchStatusQueue_Template.sh > $install_dir/bin/WatchStatusQueue.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchInputQueue_Template.sh > $install_dir/bin/WatchInputQueue.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchFailedEventQueue_Template.sh > $install_dir/bin/WatchFailedEventQueue.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchMessageEventQueue_Template.sh > $install_dir/bin/WatchMessageEventQueue.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchFinanceQueue_Template.sh > $install_dir/bin/WatchFinanceQueue.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchHelloWorldQueue_Template.sh > $install_dir/bin/WatchHelloWorldQueue.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchMedicalQueue_Template.sh > $install_dir/bin/WatchMedicalQueue.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchTelecomQueue_Template.sh > $install_dir/bin/WatchTelecomQueue.sh
		sed "s/{InstallDirectory}/$install_dir_repl/g;s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/PushSampleDataToKafka_Template.sh > $install_dir/bin/PushSampleDataToKafka.sh
	fi
fi

chmod 777 $install_dir/bin/*.*
chmod 777 $install_dir/bin/kamanja

cd $pwdnm
