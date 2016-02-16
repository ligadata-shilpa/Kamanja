
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

# changing path in script files for 2.10 version

echo $install_dir

sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/script/MigrateManager_Template.sh > $install_dir/bin/MigrateManager.sh

sed "s/{InstallDirectory}/$install_dir_repl/g" $install_dir/template/config/log4j2_Template.xml > $install_dir/config/log4j2.xml

sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/config/MigrateConfig_Template.properties > $install_dir/config/MigrateConfig.json

sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/config/ClusterConfig_Template.json > $install_dir/config/ClusterConfig.json

sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/template/config/MetadataAPIConfig_Template.properties > $install_dir/config/MetadataAPIConfig.properties


cd $pwdnm

