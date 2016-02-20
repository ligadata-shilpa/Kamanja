#!/bin/bash

# KamanjaClusterInstall.sh

script_dir=$(dirname "$0")

name1=$1

Usage()
{
    echo
    echo "Usage:"
    echo "      GetComponentsVersions.sh --componentVersionJarAbsolutePath <componentVersionJarAbsolutePath> "
    echo "                               --componentVersionJarFileName <componentVersionJarFileName> "
    echo "                               --remoteNodeIp <remoteNodeIp> "
    echo "                               --resultsFileAbsolutePath <resultsFileAbsolutePath> "
    echo "                               --resultFileName <resultFileName> "
    echo "                               --jsonArg <jsonArg> "
    echo "                               --rootDirPath <rootDirPath> "
    echo "                               --pathOutputFileName <pathOutputFlName> "
    echo 
}

# Collect the named parameters 
componentVersionJarAbsolutePath=""
componentVersionJarFileName=""
remoteNodeIp=""
resultsFileAbsolutePath=""
resultFileName=""
jsonArg="" 
rootDirPath=""
pathOutputFileName="" 

while [ "$1" != "" ]; do
    case $1 in
        --componentVersionJarAbsolutePath )   shift
                                componentVersionJarAbsolutePath=$1
                                ;;
        --componentVersionJarFileName )    shift
                                componentVersionJarFileName=$1
                                ;;
        --remoteNodeIp )      shift
                                remoteNodeIp=$1
                                ;;
        --resultsFileAbsolutePath )         shift
                                resultsFileAbsolutePath=$1
                                ;;
        --resultFileName )          shift
                                resultFileName=$1
                                ;;
        --jsonArg )          shift
                                jsonArg=$1
                                ;;
        --rootDirPath )          shift
                                rootDirPath=$1
                                ;;
        --pathOutputFileName )          shift
                                pathOutputFileName=$1
                                ;;
        * )                     echo 
                                echo "Problem: Argument $1 is invalid named parameter."
                                Usage
                                exit 1
                                ;;
    esac
    shift
done

echo "componentVersionJarAbsolutePath:$componentVersionJarAbsolutePath, componentVersionJarFileName:$componentVersionJarFileName, remoteNodeIp:$remoteNodeIp, resultsFileAbsolutePath:$resultsFileAbsolutePath"
echo "resultFileName:$resultFileName, rootDirPath:$rootDirPath, pathOutputFileName:$pathOutputFileName, jsonArg:$jsonArg"

rm -rf /tmp/Get-Component.log

scp -o StrictHostKeyChecking=no "$componentVersionJarAbsolutePath" "$remoteNodeIp:/tmp/$componentVersionJarFileName"

ssh -o StrictHostKeyChecking=no -T $remoteNodeIp  <<-EOF
	rm -rf /tmp/$resultFileName
	java -jar /tmp/$componentVersionJarFileName '/tmp/resultFileName_local' '$jsonArg'
	if [ -d "$rootDirPath" ]; then
		cd $rootDirPath
		echo "\$(pwd -P)" > "/tmp/pathOutputFileName_local"
	else
		echo "" > "/tmp/pathOutputFileName_local"
	fi
EOF

scp -o StrictHostKeyChecking=no "$remoteNodeIp:/tmp/resultFileName_local" "$resultsFileAbsolutePath"
scp -o StrictHostKeyChecking=no "$remoteNodeIp:/tmp/pathOutputFileName_local" "/tmp/$pathOutputFileName"
scp -o StrictHostKeyChecking=no "$remoteNodeIp:/tmp/Get-Component.log" "/tmp/Get-Component.log"

