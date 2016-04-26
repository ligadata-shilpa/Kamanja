#!/bin/bash

# GenerateAdapterBindings.sh
#

Usage()
{
    echo 
    echo "Usage:"
    echo "      GenerateAdapterBindings.sh --MetadataAPIConfig  <metadataAPICfgPath>"
    echo "      --ClusterConfig <ClusterCfgPath> --OutputFile <OutFilePath>"
    echo                      
    echo "  NOTES: Generate Message Adapter Bindings Json using cluster configuration to locate"
    echo "         the appropriate adapter information. "
    echo 
}

name1=$1

if [ "$#" -eq 6 ]; then
	echo
else 
    echo "Problem: Incorrect number of arguments"
    echo 
    Usage
    exit 1
fi

if [[ "$name1" != "--MetadataAPIConfig" && "$name1" != "--ClusterId" && "$name1" != "--OutputFile" ]]; then
	echo "Problem: Bad arguments"
	echo 
	Usage
	exit 1
fi

# Collect the named parameters 
metadataAPIConfig=""
clusterConfig=""
outputFile="/tmp/AdapterMessageBindings.json"

while [ "$1" != "" ]; do
    echo "parameter is $1"
    case $1 in
        --MetadataAPIConfig )   shift
                                metadataAPIConfig=$1
                                ;;
        --ClusterConfig )       shift
                                clusterConfig=$1
                                ;;
        --OutputFile )          shift
                                outputFile=$1
                                ;;

        * )                     echo "Problem: Argument $1 is invalid named parameter."
                                Usage
                                exit 1
                                ;;
    esac
    shift
done
installDir=`cat $metadataAPIConfig | grep '[Rr][Oo][Oo][Tt]_[Dd][Ii][Rr]' | sed 's/.*=\(.*\)$/\1/g'`

java -Dlog4j.configurationFile=file:$installDir/config/log4j2.xml -cp $installDir/lib/system/ExtDependencyLibs2_2.11-1.4.0.jar:$installDir/lib/system/ExtDependencyLibs_2.11-1.4.0.jar:$installDir/lib/system/KamanjaInternalDeps_2.11-1.4.0.jar:$installDir/lib/system/generateadapterbindings_2.11-1.4.0.jar com.ligadata.Migrate.GenerateAdapterBindings --config $clusterConfig --outfile $outputFile
echo result = $?
