
echo "Setting up paths"

install_dir=$1

if [ ! -d "$install_dir" ]; then
        echo "Not valid install path supplied.  It should be a directory that can be written to and whose current content is of no value (will be overwritten) "
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

nstall_dir=$(echo $ivyPath | sed 's/[\/]*$//')

KafkaRootDir=$2
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

#install_dir=$(dirname $(pwd -P))

java_home_repl=$(echo $java_home | sed 's/\//\\\//g')
scala_home_repl=$(echo $scala_home | sed 's/\//\\\//g')
install_dir_repl=$(echo $install_dir | sed 's/\//\\\//g')

# changing path in script files for 2.10 version

echo $install_dir

sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/KamanjaInstall-2.10/template/script/MigrateManager_Template.sh > $install_dir/KamanjaInstall-2.10/bin/MigrateManager.sh

# logfile for 2.10 version
sed "s/{InstallDirectory}/$install_dir_repl/g" $install_dir/KamanjaInstall-2.10/template/config/log4j2_Template.xml > $install_dir/KamanjaInstall-2.10/config/log4j2.xml

# changing path in config files for 2.10 version
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/KamanjaInstall-2.10/template/config/MigrateConfig_Template.json > $install_dir/KamanjaInstall-2.10/config/MigrateConfig.json
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/KamanjaInstall-2.10/template/config/ClusterConfig_Template.json > $install_dir/KamanjaInstall-2.10/config/ClusterConfig.json
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/KamanjaInstall-2.10/template/config/MetadataAPIConfig_Template.properties > $install_dir/KamanjaInstall-2.10/config/MetadataAPIConfig.properties

# changing path in script files for 2.11 version

sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/KamanjaInstall-2.11/template/script/MigrateManager_Template.sh > $install_dir/KamanjaInstall-2.11/bin/MigrateManager.sh

# logfile for 2.11 version
sed "s/{InstallDirectory}/$install_dir_repl/g" $install_dir/KamanjaInstall-2.11/template/config/log4j2_Template.xml > $install_dir/KamanjaInstall-2.11/config/log4j2.xml

# changing path in config files for 2.11 version
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/KamanjaInstall-2.11/template/config/MigrateConfig_Template.properties > $install_dir/KamanjaInstall-2.11/config/MigrateConfig.json
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/KamanjaInstall-2.11/template/config/ClusterConfig_Template.properties > $install_dir/KamanjaInstall-2.11/config/ClusterConfig.json
sed "s/{InstallDirectory}/$install_dir_repl/g;s/{ScalaInstallDirectory}/$scala_home_repl/g;s/{JavaInstallDirectory}/$java_home_repl/g" $install_dir/KamanjaInstall-2.11/template/config/MetadataAPIConfig_Template.properties > $install_dir/KamanjaInstall-2.11/config/MetadataAPIConfig.properties

chmod 777 $install_dir/KamanjaInstall-2.10/bin/*.*

chmod 777 $install_dir/KamanjaInstall-2.11/bin/*.*

cd $pwdnm

