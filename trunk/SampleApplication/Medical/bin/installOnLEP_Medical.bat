set installPath=%1
set srcPath=%2

IF %1 == "" (
        echo "No install path supplied.  It should be a directory that can be written to and whose current content is of no value (will be overwritten) "
        echo "%0 <install path> <src tree trunk directory>"
        exit 1
)
IF %2 == "" (
        echo "No src path supplied.  It should be the trunk directory containing the jars, files, what not that need to be supplied."
        echo "%0 <install path> <src tree trunk directory>"
        exit 1
)

set ONLEPLIBPATH=%installPath%

REM *******************************
REM Clean out prior installation
REM *******************************
del /s %ONLEPLIBPATH%

REM *******************************
REM Make the directories as needed
REM *******************************
mkdir %ONLEPLIBPATH%\msgdata
mkdir %ONLEPLIBPATH%\kvstores
mkdir %ONLEPLIBPATH%\logs

REM *******************************
REM Build fat-jars
REM *******************************

echo "clean, package and assemble %srcPath% ..."

cd %srcPath%
call sbt clean 
call sbt package 
call sbt OnLEPManager/assembly 
call sbt MetadataAPI/assembly 
call sbt KVInit/assembly 
call sbt MethodExtractor/assembly 
call sbt NodeInfoExtract/assembly

REM recreate eclipse projects
REM echo "refresh the eclipse projects ..."
REM cd %srcPath%
REM sbt eclipse

REM Move them into place
echo "xcopy the fat jars to %ONLEPLIBPATH% ..."

cd %srcPath%
xcopy Utils\KVInit\target\scala-2.10\KVInit* %ONLEPLIBPATH%
xcopy MetadataAPI\target\scala-2.10\MetadataAPI* %ONLEPLIBPATH%
xcopy OnLEPManager\target\scala-2.10\OnLEPManager* %ONLEPLIBPATH%

REM *******************************
REM xcopy jars required (more than required if the fat jars are used)
REM *******************************

REM Base Types and Functions, InputOutput adapters, and original versions of things
echo "xcopy Base Types and Functions, InputOutput adapters..."
xcopy %srcPath%\BaseFunctions\target\scala-2.10\basefunctions_2.10-0.1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%\BaseTypes\target\scala-2.10\basetypes_2.10-0.1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%\InputOutputAdapters\FileSimpleInputOutputAdapters\target\scala-2.10\filesimpleinputoutputadapters_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%\InputOutputAdapters\KafkaSimpleInputOutputAdapters\target\scala-2.10\kafkasimpleinputoutputadapters_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%\EnvContexts\SimpleEnvContextImpl\target\scala-2.10\simpleenvcontextimpl_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%\MetadataBootstrap\Bootstrap\target\scala-2.10\bootstrap_2.10-1.0.jar %ONLEPLIBPATH%

REM Storage jars
echo "xcopy Storage jars..."
xcopy %srcPath%\Storage\target\scala-2.10\storage_2.10-0.0.0.2.jar %ONLEPLIBPATH%

REM Metadata jars
echo "xcopy Metadata jars..."
xcopy %srcPath%\Metadata\target\scala-2.10\metadata_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%\MessageDef\target\scala-2.10\messagedef_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%\MetadataAPI\target\scala-2.10\metadataapi_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%\MetadataAPIService\target\scala-2.10\metadataapiservice_2.10-0.1.jar %ONLEPLIBPATH%
xcopy %srcPath%\MetadataAPIServiceClient\target\scala-2.10\metadataapiserviceclient_2.10-0.1.jar %ONLEPLIBPATH% : Didn't build... Not included anymore?
xcopy %srcPath%\Pmml\MethodExtractor\target\scala-2.10\methodextractor_2.10-1.0.jar %ONLEPLIBPATH%

REM OnLEP jars
echo "xcopy OnLEP jars..."
xcopy %srcPath%\OnLEPBase\target\scala-2.10\onlepbase_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%\OnLEPManager\target\scala-2.10\onlepmanager_2.10-1.0.jar %ONLEPLIBPATH%

REM Pmml compile and runtime jars
echo "xcopy Pmml compile and runtime jars..."
xcopy %srcPath%\Pmml\PmmlRuntime\target\scala-2.10\pmmlruntime_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%\Pmml\PmmlUdfs\target\scala-2.10\pmmludfs_2.10-1.0.jar %ONLEPLIBPATH%
xcopy %srcPath%\Pmml\PmmlCompiler\target\scala-2.10\pmmlcompiler_2.10-1.0.jar %ONLEPLIBPATH%

REM sample configs
REMecho "xcopy sample configs..."
xcopy %srcPath%\Utils\KVInit\src\main\resources\*cfg %ONLEPLIBPATH%

REM other jars 
echo "xcopy other jars..."
xcopy %srcPath%\..\externals\log4j\log4j-1.2.17.jar %ONLEPLIBPATH%

REM *******************************
REM COPD messages data prep
REM *******************************

REM Prepare test messages and xcopy them into place

echo "Prepare test messages and xcopy them into place..."
cd %srcPath%\Utils\KVInit\src\main\resources
REM 7za -tgzip beneficiaries.csv.gz beneficiaries.csv
REM 7za -tgzip messages_new_format.csv.gz messages_new_format.csv
REM 7za -tgzip messages_old_format.csv.gz messages_old_format.csv
REM 7za -tgzip messages_new_format_all.csv.gz messages_new_format_all.csv
REM 7za -tgzip messages50_2014_BIOH.csv.gz messages50_2014_BIOH.csv

xcopy *gz %ONLEPLIBPATH%\msgdata\

REM *******************************
REM All that is left is to run the OnLEPManager
REM *******************************

REM no debug
REM java -jar %ONLEPLIBPATH%\OnLEPManager-1.0 --config /tmp/OnLEPInstall/COPD.cfg

REM debug version intended for eclipse attached debugging
REM java -Xdebug -Xrunjdwp:transport=dt_socket,address=8998,server=y -jar %ONLEPLIBPATH%/OnLEPManager-1.0 --config /tmp/OnLEPInstall/COPD.cfg


echo "installOnLEP complete..."
