#!/bin/bash

set -e

installPath=$1
srcPath=$2
ivyPath=$3
KafkaRootDir=$4
ver210=2.10
ver211=2.11

if [ ! -d "$installPath" ]; then
        echo "Not valid install path supplied.  It should be a directory that can be written to and whose current content is of no value (will be overwritten) "
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

if [ ! -d "$srcPath" ]; then
        echo "Not valid src path supplied.  It should be the trunk directory containing the jars, files, what not that need to be supplied."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

if [ ! -d "$ivyPath" ]; then
        echo "Not valid ivy path supplied.  It should be the ivy path for dependency the jars."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

if [ ! -d "$KafkaRootDir" ]; then
        echo "Not valid Kafka path supplied."
        echo "$0 <install path> <src tree trunk directory> <ivy directory path for dependencies> <kafka installation path>"
        exit 1
fi

installPath=$(echo $installPath | sed 's/[\/]*$//')
srcPath=$(echo $srcPath | sed 's/[\/]*$//')
ivyPath=$(echo $ivyPath | sed 's/[\/]*$//')


# *******************************
# Make the directories as needed for version-2.10
# *******************************
mkdir -p $installPath/MigrationAndClusterInstall-$ver210/bin
#mkdir -p $installPath/MigrationAndClusterInstall-$ver210/lib
mkdir -p $installPath/MigrationAndClusterInstall-$ver210/lib/system
mkdir -p $installPath/MigrationAndClusterInstall-$ver210/lib/application
mkdir -p $installPath/MigrationAndClusterInstall-$ver210/storage
mkdir -p $installPath/MigrationAndClusterInstall-$ver210/logs
mkdir -p $installPath/MigrationAndClusterInstall-$ver210/config
mkdir -p $installPath/MigrationAndClusterInstall-$ver210/documentation
mkdir -p $installPath/MigrationAndClusterInstall-$ver210/output
mkdir -p $installPath/MigrationAndClusterInstall-$ver210/workingdir
#mkdir -p $installPath/Kamanja-$ver210/template
mkdir -p $installPath/MigrationAndClusterInstall-$ver210/template/config
mkdir -p $installPath/MigrationAndClusterInstall-$ver210/template/script

# *******************************
# Make the directories as needed for version-2.11
# *******************************
mkdir -p $installPath/MigrationAndClusterInstall-$ver211/bin
#mkdir -p $installPath/MigrationAndClusterInstall-$ver211/lib
mkdir -p $installPath/MigrationAndClusterInstall-$ver211/lib/system
mkdir -p $installPath/MigrationAndClusterInstall-$ver211/lib/application
mkdir -p $installPath/MigrationAndClusterInstall-$ver211/storage
mkdir -p $installPath/MigrationAndClusterInstall-$ver211/logs
mkdir -p $installPath/MigrationAndClusterInstall-$ver211/config
mkdir -p $installPath/MigrationAndClusterInstall-$ver211/documentation
mkdir -p $installPath/MigrationAndClusterInstall-$ver211/output
mkdir -p $installPath/MigrationAndClusterInstall-$ver211/workingdir
#mkdir -p $installPath/MigrationAndClusterInstall-$ver211/template
mkdir -p $installPath/MigrationAndClusterInstall-$ver211/template/config
mkdir -p $installPath/MigrationAndClusterInstall-$ver211/template/script

bin210=$installPath/MigrationAndClusterInstall-$ver210/bin
systemlib210=$installPath/MigrationAndClusterInstall-$ver210/lib/system
applib210=$installPath/MigrationAndClusterInstall-$ver210/lib/application

bin211=$installPath/MigrationAndClusterInstall-$ver211/bin
systemlib211=$installPath/MigrationAndClusterInstall-$ver211/lib/system
applib211=$installPath/MigrationAndClusterInstall-$ver211/lib/application

cd $srcPath 

cd $srcPath
cp KamanjaManager/target/scala-2.10/KamanjaManager* $bin210

cp KamanjaManager/target/scala-2.11/KamanjaManager* $bin211
# only for 2.11 ?
cp Utils/Migrate/MigrateManager/target/MigrateManager* $bin211

cp $srcPath/Utils/NodeInfoExtract/target/scala-2.10/NodeInfoExtract* $bin210
cp $srcPath/Utils/NodeInfoExtract/target/scala-2.11/NodeInfoExtract* $bin211

srcPath=$1
systemlib=$2
ivyPath=$3
cd $srcPath/SampleApplication/EasyInstall
bash $srcPath/SampleApplication/EasyInstall/CopyCommonJars.sh $srcPath $systemlib210 $ivyPath
bash $srcPath/SampleApplication/EasyInstall/CopyCommonJars.sh $srcPath $systemlib211 $ivyPath
bash $srcPath/SampleApplication/EasyInstall/CopyJars.sh $srcPath $ivyPath $systemlib210 $systemlib211

cd $srcPath/SampleApplication/MigrationAndClusterInstall/template2.10
cp -rf * $installPath/MigrationAndClusterInstall-$ver210/template

cd $srcPath/SampleApplication/MigrationAndClusterInstall/template2.11
cp -rf * $installPath/MigrationAndClusterInstall-$ver211/template

bash $srcPath/SampleApplication/EasyInstall/SetPathsMigrateClusterInstall.sh $KafkaRootDir


chmod 0700 $installPath/MigrationAndClusterInstall-$ver211/input/SampleApplications/bin/*sh
chmod 0700 $installPath/MigrationAndClusterInstall-$ver211/input/SampleApplications/bin/*sh


echo "MigrationAndClusterInstall install complete..."
