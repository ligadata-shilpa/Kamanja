#!/bin/bash

set -e

installPath=$1
srcPath=$2
ivyPath=$3
KafkaRootDir=$4

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
# Clean out prior installation
# *******************************
rm -Rf $installPath

# *******************************
# Make the directories as needed
# *******************************
mkdir -p $installPath/bin
mkdir -p $installPath/lib
mkdir -p $installPath/lib/system
mkdir -p $installPath/lib/application
mkdir -p $installPath/storage
mkdir -p $installPath/logs
mkdir -p $installPath/config
mkdir -p $installPath/documentation
mkdir -p $installPath/output
mkdir -p $installPath/workingdir
mkdir -p $installPath/template
mkdir -p $installPath/template/config
mkdir -p $installPath/template/script
mkdir -p $installPath/input
#new one
mkdir -p $installPath/input/SampleApplications
mkdir -p $installPath/input/SampleApplications/bin
mkdir -p $installPath/input/SampleApplications/data
mkdir -p $installPath/input/SampleApplications/metadata
mkdir -p $installPath/input/SampleApplications/metadata/config
mkdir -p $installPath/input/SampleApplications/metadata/container
mkdir -p $installPath/input/SampleApplications/metadata/function
mkdir -p $installPath/input/SampleApplications/metadata/message
mkdir -p $installPath/input/SampleApplications/metadata/model
mkdir -p $installPath/input/SampleApplications/metadata/script
mkdir -p $installPath/input/SampleApplications/metadata/type
mkdir -p $installPath/input/SampleApplications/template
#new one

bin=$installPath/bin
systemlib=$installPath/lib/system
applib=$installPath/lib/application

echo $installPath
echo $srcPath
echo $bin

# *******************************
# Build fat-jars
# *******************************

echo "clean, package and assemble $srcPath ..."

cd $srcPath
sbt clean package Kamanja/assembly

# recreate eclipse projects
#echo "refresh the eclipse projects ..."
#cd $srcPath
#sbt eclipse

# Move them into place
echo "copy the fat jars to $installPath ..."

# *******************************
# Copy jars required (more than required if the fat jars are used)
# *******************************

# Base Types and Functions, InputOutput adapters, and original versions of things
echo "copy Kamanja fat jar to $systemlib"

cp $srcPath/Kamanja/target/scala-2.10/Kamanja* $bin
cp $srcPath/Storage/SqlServer/src/test/resources/sqljdbc4-2.0.jar $systemlib

# sample configs
#echo "copy sample configs..."
cp $srcPath/Utils/KVInit/src/main/resources/*cfg $systemlib

# Generate keystore file
#echo "generating keystore..."
#keytool -genkey -keyalg RSA -alias selfsigned -keystore $installPath/config/keystore.jks -storepass password -validity 360 -keysize 2048

#copy kamanja to bin directory
cp $srcPath/Utils/Script/kamanja $bin
#cp $srcPath/Utils/Script/MedicalApp.sh $bin
cp $srcPath/MetadataAPI/target/scala-2.10/classes/HelpMenu.txt $installPath/input
# *******************************
# COPD messages data prep
# *******************************

# Prepare test messages and copy them into place

echo "Prepare test messages and copy them into place..."
# *******************************
# Copy documentation files
# *******************************
cd $srcPath/Documentation
cp -rf * $installPath/documentation

# *******************************
# copy models, messages, containers, config, scripts, types  messages data prep
# *******************************

#HelloWorld
cd $srcPath/SampleApplication/HelloWorld/data
cp * $installPath/input/SampleApplications/data

cd $srcPath/SampleApplication/HelloWorld/message
cp * $installPath/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/HelloWorld/model
cp * $installPath/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/HelloWorld/template
cp -rf * $installPath/input/SampleApplications/template


cd $srcPath/SampleApplication/HelloWorld/config
cp -rf * $installPath/config
#HelloWorld

#Medical
cd $srcPath/SampleApplication/Medical/SampleData
cp *.csv $installPath/input/SampleApplications/data
cp *.csv.gz $installPath/input/SampleApplications/data

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Containers
cp * $installPath/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/Medical/Functions
cp * $installPath/input/SampleApplications/metadata/function

cd $srcPath/SampleApplication/Medical/MessagesAndContainers/Fixed/Messages
cp * $installPath/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/Medical/Models
cp *.* $installPath/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/Medical/Types
cp * $installPath/input/SampleApplications/metadata/type

cd $srcPath/SampleApplication/Medical/template
cp -rf * $installPath/input/SampleApplications/template

cd $srcPath/SampleApplication/Medical/Configs
cp -rf * $installPath/config
#Medical

#Telecom
cd $srcPath/SampleApplication/Telecom/data
cp * $installPath/input/SampleApplications/data

cd $srcPath/SampleApplication/Telecom/metadata/container
cp * $installPath/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/Telecom/metadata/message
cp * $installPath/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/Telecom/metadata/model
cp *.* $installPath/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/Telecom/metadata/template
cp -rf * $installPath/input/SampleApplications/template

cd $srcPath/SampleApplication/Telecom/metadata/config
cp -rf * $installPath/config
#Telecom

#Finance
cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/data
cp * $installPath/input/SampleApplications/data

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/container
cp * $installPath/input/SampleApplications/metadata/container

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/message
cp * $installPath/input/SampleApplications/metadata/message

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/model
cp *.* $installPath/input/SampleApplications/metadata/model

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/type
cp * $installPath/input/SampleApplications/metadata/type

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/template
cp -rf * $installPath/input/SampleApplications/template

cd $srcPath/SampleApplication/InterfacesSamples/src/main/resources/sample-app/metadata/config
cp -rf * $installPath/config
#Finance

cd $srcPath/SampleApplication/EasyInstall/template
cp -rf * $installPath/template

cd $srcPath/SampleApplication/EasyInstall
cp SetPaths.sh $installPath/bin/

bash $installPath/bin/SetPaths.sh $KafkaRootDir

chmod 0700 $installPath/input/SampleApplications/bin/*sh

echo "Kamanja install complete..."
