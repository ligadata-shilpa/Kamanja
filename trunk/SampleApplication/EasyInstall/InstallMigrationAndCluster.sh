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
mkdir -p $installPath/KamanjaInstall-$ver210/bin
mkdir -p $installPath/KamanjaInstall-$ver210/lib/system
mkdir -p $installPath/KamanjaInstall-$ver210/lib/application
mkdir -p $installPath/KamanjaInstall-$ver210/logs
mkdir -p $installPath/KamanjaInstall-$ver210/config
mkdir -p $installPath/KamanjaInstall-$ver210/template/config
mkdir -p $installPath/KamanjaInstall-$ver210/template/script

# *******************************
# Make the directories as needed for version-2.11
# *******************************
mkdir -p $installPath/KamanjaInstall-$ver211/bin
mkdir -p $installPath/KamanjaInstall-$ver211/lib/system
mkdir -p $installPath/KamanjaInstall-$ver211/lib/application
mkdir -p $installPath/KamanjaInstall-$ver211/logs
mkdir -p $installPath/KamanjaInstall-$ver211/config
mkdir -p $installPath/KamanjaInstall-$ver211/template/config
mkdir -p $installPath/KamanjaInstall-$ver211/template/script

kamanjainstallbin210=$installPath/KamanjaInstall-$ver210/bin
kamanjainstallsystemlib210=$installPath/KamanjaInstall-$ver210/lib/system
kamanjainstallapplib210=$installPath/KamanjaInstall-$ver210/lib/application

kamanjainstallbin211=$installPath/KamanjaInstall-$ver211/bin
kamanjainstallsystemlib211=$installPath/KamanjaInstall-$ver211/lib/system
kamanjainstallapplib211=$installPath/KamanjaInstall-$ver211/lib/application

cd $srcPath 

cd $srcPath
cp KamanjaManager/target/scala-2.10/KamanjaManager* $kamanjainstallbin210

cp KamanjaManager/target/scala-2.11/KamanjaManager* $kamanjainstallbin211
# only for 2.11 ?
cp Utils/Migrate/MigrateManager/target/MigrateManager* $kamanjainstallbin211

cp $srcPath/Utils/NodeInfoExtract/target/scala-2.10/NodeInfoExtract* $kamanjainstallbin210
cp $srcPath/Utils/NodeInfoExtract/target/scala-2.11/NodeInfoExtract* $kamanjainstallbin211

cd $srcPath/SampleApplication/MigrationAndClusterInstall/template2.10
cp -rf * $installPath/KamanjaInstall-$ver210/template/.

cd $srcPath/SampleApplication/MigrationAndClusterInstall/template2.11
cp -rf * $installPath/KamanjaInstall-$ver211/template/.


cd $srcPath/SampleApplication/EasyInstall
cp SetPathsMigrateClusterInstall.sh $installPath/KamanjaInstall-$ver210/bin/
cp SetPathsMigrateClusterInstall.sh $installPath/KamanjaInstall-$ver211/bin/

bash $installPath/KamanjaInstall-$ver211/bin/SetPathsMigrateClusterInstall.sh $installPath $KafkaRootDir

chmod 0700 $installPath/KamanjaInstall-$ver211/bin/*sh
chmod 0700 $installPath/KamanjaInstall-$ver211/bin/*sh


echo "KamanjaInstall install complete..."
