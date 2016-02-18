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

scp -o StrictHostKeyChecking=no "$componentVersionJarAbsolutePath" "$remoteNodeIp:/tmp/$componentVersionJarFileName"

ssh -o StrictHostKeyChecking=no -T $remoteNodeIp  <<-EOF
	rm -rf /tmp/$resultFileName
	java -jar /tmp/$componentVersionJarFileName /tmp/$resultFileName $jsonArg 
	if [ -d "$rootDirPath" ]; then
		if [ ! -L $rootDirPath ]; then
		    pwdnm=$(pwd -P)
		    cd $rootDirPath
		    lnPath=$(pwd -P)
			echo "$lnPath" > "/tmp/$pathOutputFileName"
			cd $pwdnm
		else
			echo "$rootDirPath" > "/tmp/$pathOutputFileName"
		fi
	else
		echo "" > "/tmp/$pathOutputFileName"
	fi
EOF

scp -o StrictHostKeyChecking=no "$remoteNodeIp:/tmp/$resultFileName" "$resultsFileAbsolutePath"
scp -o StrictHostKeyChecking=no "$remoteNodeIp:/tmp/$pathOutputFileName" "/tmp/$pathOutputFileName"

