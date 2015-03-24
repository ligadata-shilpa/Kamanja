KafkaRootDir=$1
if [ ! -d "$KafkaRootDir" ]; then
        echo "Not valid Kafka path supplied."
        echo "$0 <kafka installation path>"
        exit 1
fi

KafkaRootDir=$(echo $KafkaRootDir | sed 's/[\/]*$//')

jar_full_path=$(which jar)

if [ "$?" != "0" ]; then
	echo "Not found java home directory."
	exit 1
fi

scala_full_path=$(which scala)

if [ "$?" != "0" ]; then
	echo "Not found scala home directory."
	exit 1
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
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/script/ClusterMetadata_Template.sh > $install_dir/bin/ClusterMetadata.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/script/StartEngine_Template.sh > $install_dir/bin/StartEngine.sh

sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/application1/template/script/InitKvStores_Template.sh > $install_dir/input/application1/bin/InitKvStores.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/application1/template/script/StartMetadataAPI_Template.sh > $install_dir/input/application1/bin/ApplicationMetadata.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/application1/template/script/PushSampleDataToKafka_Template.sh > $install_dir/input/application1/bin/PushSampleDataToKafka.sh

# application-1-HelloWorld
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/application-1-HelloWorld/template/script/InitKvStores_Template.sh > $install_dir/input/application-1-HelloWorld/bin/InitKvStores.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/application-1-HelloWorld/template/script/StartMetadataAPI_Template.sh > $install_dir/input/application-1-HelloWorld/bin/ApplicationMetadata.sh
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/application-1-HelloWorld/template/script/PushSampleDataToKafka_Template.sh > $install_dir/input/application-1-HelloWorld/bin/PushSampleDataToKafka.sh
# application-1-HelloWorld


# changing path in config files
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/config/ClusterCfgMetadataAPIConfig_Template.properties > $install_dir/config/ClusterCfgMetadataAPIConfig.properties
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/config/ClusterConfig_Template.json > $install_dir/config/ClusterConfig.json
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/config/EngineConfig_Template.properties > $install_dir/config/Engine1Config.properties

sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/application1/template/config/MetadataAPIConfig_Template.properties > $install_dir/input/application1/metadata/config/MetadataAPIConfig.properties

# application-1-HelloWorld
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/input/application-1-HelloWorld/template/config/MetadataAPIConfig_Template.properties > $install_dir/input/application-1-HelloWorld/metadata/config/MetadataAPIConfig.properties

# application-1-HelloWorld

# Expecting 1st Parameter as Kafka Install directory
if [ "$#" -ne 1 ] || ! [ -d "$KafkaRootDir" ]; then
	echo "WARN: Not given/found Kafka install directory. Not going to create CreateQueues.sh, WatchOutputQueue.sh, WatchStatusQueue.sh and WatchInputQueue.sh"
else
	kafkatopics="$KafkaRootDir/bin/kafka-topics.sh"
	if [ ! -f "$kafkatopics" ]; then
		echo "WARN: Not found bin/kafka-topics.sh in given Kafka install directory $KafkaRootDir. Not going to create CreateQueues.sh, WatchOutputQueue.sh, WatchStatusQueue.sh and WatchInputQueue.sh"
	else
		KafkaRootDir_repl=$(echo $KafkaRootDir | sed 's/\//\\\//g')
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/CreateQueues_Template.sh > $install_dir/bin/CreateQueues.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchOutputQueue_Template.sh > $install_dir/bin/WatchOutputQueue.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchStatusQueue_Template.sh > $install_dir/bin/WatchStatusQueue.sh
		sed "s/{KafkaInstallDir}/$KafkaRootDir_repl/g" $install_dir/template/script/WatchInputQueue_Template.sh > $install_dir/bin/WatchInputQueue.sh
	fi
fi

chmod 777 $install_dir/bin/*.*

cd $pwdnm
