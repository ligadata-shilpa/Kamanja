##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**SETUP   <<<<<<<<<<<<<<<<<<<<<<<**
##**1) initialize Kamanja home (KAMANJA_HOME) and the repo trunk directory (KAMANJA_SRCDIR)**
##**2) Debug flag can be supplied on command line now.**
##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**Env setup**
export KAMANJA_HOME=/tmp/drdigital
export KAMANJA_SRCDIR=/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk
cd $KAMANJA_HOME

##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**Kamanja Pmml <<<<<<<<<<<<<<<<<<**
##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**Add messages and containers**

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties upload cluster config $KAMANJA_HOME/config/ClusterConfig.json

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add container $KAMANJA_SRCDIR/Pmml/PmmlUdfs/src/test/resources/nmspcMsgContainerTest/metadata/container/CoughCodes.json
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add container $KAMANJA_SRCDIR/Pmml/PmmlUdfs/src/test/resources/nmspcMsgContainerTest/metadata/container/DyspnoeaCodes.json
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add container $KAMANJA_SRCDIR/Pmml/PmmlUdfs/src/test/resources/nmspcMsgContainerTest/metadata/container/EnvCodes.json
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add container $KAMANJA_SRCDIR/Pmml/PmmlUdfs/src/test/resources/nmspcMsgContainerTest/metadata/container/SmokeCodes.json
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add container $KAMANJA_SRCDIR/Pmml/PmmlUdfs/src/test/resources/nmspcMsgContainerTest/metadata/container/SputumCodes.json
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add message $KAMANJA_SRCDIR/Pmml/PmmlUdfs/src/test/resources/nmspcMsgContainerTest/metadata/message/beneficiary.json
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add message $KAMANJA_SRCDIR/Pmml/PmmlUdfs/src/test/resources/nmspcMsgContainerTest/metadata/message/hl7.json
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add message $KAMANJA_SRCDIR/Pmml/PmmlUdfs/src/test/resources/nmspcMsgContainerTest/metadata/message/inpatientclaim.json
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add message $KAMANJA_SRCDIR/Pmml/PmmlUdfs/src/test/resources/nmspcMsgContainerTest/metadata/message/outpatientclaim.json

##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**Kamanja Pmml <<<<<<<<<<<<<<<<<<**
##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**Add copdv3**

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add model pmml $KAMANJA_SRCDIR/Pmml/PmmlUdfs/src/test/resources/nmspcMsgContainerTest/metadata/model/COPDv3.xml


