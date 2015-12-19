
##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**UNIT TESTS**
##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**


##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**SETUP   <<<<<<<<<<<<<<<<<<<<<<<**
##**1) initialize Kamanja home (KAMANJA_HOME) and the repo trunk directory (KAMANJA_SRCDIR)**
##**2) Debug flag can be supplied on command line now.**
##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**Env setup**
export KAMANJA_HOME=/tmp/drdigital
export KAMANJA_SRCDIR=/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk
cd $KAMANJA_HOME

##**If you want to debug a command, insert the *'debug'* keyword as the first argument in kamanja command ...e.g.,**
##$KAMANJA_HOME/bin/kamanja debug $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (jpmml) name(com.botanical.jpmml.IrisDecisionTree) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.01) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'


##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**Unit Test 1  <<<<<<<<<<<<<<<<<<**
##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**

##**Add the message (v 1.01) for this decision_tree_iris.pmml model**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add message $KAMANJA_SRCDIR/Pmml/JpmmlAdapter/src/main/resources/metadata/message/IrisMsg1.json

##**Get all messages**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties getallmessages
$KAMANJA_HOME/bin/kamanja debug $KAMANJA_HOME/config/MetadataAPIConfig.properties getallmessages

##**Add the decision_tree_iris.pmml model (version 000000.000001.000001)**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (jpmml) name(com.botanical.jpmml.IrisDecisionTree) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.01) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'
$KAMANJA_HOME/bin/kamanja debug $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (jpmml) name(com.botanical.jpmml.IrisDecisionTree) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.01) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'

##**Get all models**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties getallmodels

##**Add a new version of the message (v 1.02) for this decision_tree_iris.pmml model (this says it succeeds but really it fails ... multiple messages of same name but different versions are not supported... is this documented?)**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add message $KAMANJA_SRCDIR/Pmml/JpmmlAdapter/src/main/resources/metadata/message/IrisMsg2.json
$KAMANJA_HOME/bin/kamanja debug $KAMANJA_HOME/config/MetadataAPIConfig.properties add message $KAMANJA_SRCDIR/Pmml/JpmmlAdapter/src/main/resources/metadata/message/IrisMsg2.json

##**Update messsage v1.01 with version v1.02... this works**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties update message $KAMANJA_SRCDIR/Pmml/JpmmlAdapter/src/main/resources/metadata/message/IrisMsg2.json
$KAMANJA_HOME/bin/kamanja debug $KAMANJA_HOME/config/MetadataAPIConfig.properties update message $KAMANJA_SRCDIR/Pmml/JpmmlAdapter/src/main/resources/metadata/message/IrisMsg2.json

##**Update the version model (version 000000.000001.000001 to 000000.000001.000002)**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'updateModel type(jpmml) name(com.botanical.jpmml.IrisDecisionTree) newVersion(0.1.2) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'

##**Get all models**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties getallmodels

##**Remove the models**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties remove model com.botanical.jpmml.IrisDecisionTree.000000000001000002
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties remove model com.botanical.jpmml.irisdecisiontree.000000000001000001


##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**Unit Test 2  <<<<<<<<<<<<<<<<<<**
##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**Add model bad argument tests**

##**missing pmml**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (jpmml) name(com.botanical.jpmml.IrisDecisionTree) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.00) '
##**wrong model type**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (pmml) name(com.botanical.jpmml.IrisDecisionTree) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.00) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (scala) name(com.botanical.jpmml.IrisDecisionTree) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.00) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (java) name(com.botanical.jpmml.IrisDecisionTree) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.00) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (binary) name(com.botanical.jpmml.IrisDecisionTree) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.00) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (foo) name(com.botanical.jpmml.IrisDecisionTree) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.00) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'
##**missing name**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (jpmml) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.00) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'
##**missing modelversion**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (jpmml) name(com.botanical.jpmml.IrisDecisionTree) message(System.IrisMsg) messageversion(00.01.00) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'
##**missing message name**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (jpmml) name(com.botanical.jpmml.IrisDecisionTree) modelversion(000000.000001.000001)  messageversion(00.01.00) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'
##**wrong message version**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (jpmml) name(com.botanical.jpmml.IrisDecisionTree) modelversion(000000.000001.000001)  messageversion(00.11.00) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'


##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**Unit Test 3  <<<<<<<<<<<<<<<<<<**
##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**Update model bad argument tests**

##**missing pmml**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'updateModel type (jpmml) name(com.botanical.jpmml.IrisDecisionTree) newVersion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.00) '
##**wrong model type**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'updateModel type(pmml) name(com.botanical.jpmml.IrisDecisionTree) newVersion(000000.000001.000002) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'
##**missing name**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'updateModel type(jpmml) newVersion(000000.000001.000002) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'
##**missing newVersion**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'updateModel type(jpmml) name(com.botanical.jpmml.IrisDecisionTree) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'


##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**SETUP FOR ENGINE TESTING**
##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**

##**Env setup**
export KAMANJA_HOME=/tmp/drdigital
export KAMANJA_SRCDIR=/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk
cd $KAMANJA_HOME

##**Establish cluster configuration**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties upload cluster config $KAMANJA_HOME/config/ClusterConfig.json

##**Add the jpmml message (IrisMsg1.json has version = 00.01.01)**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add message $KAMANJA_SRCDIR/Pmml/JpmmlAdapter/src/main/resources/metadata/message/IrisMsg1.json

##**Install the JPMML Models that consume the Iris dataset... there are four of them... three generated by the Rattle extension to R and one by KNIME**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (jpmml) name(com.botanical.jpmml.IrisDecisionTree) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.01) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/KNIME/single_iris_dectree.xml)'
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (jpmml) name(com.botanical.jpmml.IrisKMeans) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.01) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/Rattle/k_means_iris_pmml.xml)'
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (jpmml) name(com.botanical.jpmml.IrisRandomForest) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.01) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/Rattle/random_forest_iris_pmml.xml)'
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties 'addModel type (jpmml) name(com.botanical.jpmml.DecisionTreeIris) modelversion(000000.000001.000001) message(System.IrisMsg) messageversion(00.01.01) pmml(/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk/Pmml/JpmmlAdapter/src/main/resources/metadata/model/Rattle/decision_tree_iris.pmml)'

##**Get all models**
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties getallmodels
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties getallmessages


##**Establish queues**
$KAMANJA_HOME/bin/CreateQueues.sh

##**Stop the kafka and the zookeeper as needed**
$KAFKA_HOME/bin/kafka-server-stop.sh
rich@pepper:~/tarballs/zookeeper/zookeeper-3.4.6$ bin/zkServer.sh stop
##**Clean up the old fashioned way**
rich@pepper:~/tarballs/kafka/2.10/kafka_2.10-0.8.1.1$ rm -Rf /tmp/zookeeper /tmp/kafka-logs
##**Start up the zookeeper and the kafka**
rich@pepper:~/tarballs/zookeeper/zookeeper-3.4.6$ bin/zkServer.sh start 
rich@pepper:~/tarballs/kafka/2.10/kafka_2.10-0.8.1.1$ bin/kafka-server-start.sh config/server.properties

##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**
##**ENGINE TESTING**
##**<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<**

##**watch output console**
export KAMANJA_HOME=/tmp/drdigital
export KAMANJA_SRCDIR=/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk
cd $KAMANJA_HOME
bin/WatchOutputQueue.sh

##**start engine console**
export KAMANJA_HOME=/tmp/drdigital
export KAMANJA_SRCDIR=/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk
cd $KAMANJA_HOME
bin/StartEngine.sh
##**bin/StartEngine.sh debug**

##**push data console**
export KAMANJA_HOME=/tmp/drdigital
export KAMANJA_SRCDIR=/home/rich/github/dev/Sprint8FeaturesWithJpmml/kamanja/trunk
cd $KAMANJA_HOME
export useCompressed=0
$KAMANJA_SRCDIR/Pmml/JpmmlAdapter/src/main/resources/bin/JpmmlPushKafka.sh $useCompressed $KAMANJA_SRCDIR/Pmml/JpmmlAdapter/src/main/resources/data/iris.csv
export useCompressed=1
$KAMANJA_SRCDIR/Pmml/JpmmlAdapter/src/main/resources/bin/JpmmlPushKafka.sh $useCompressed $KAMANJA_SRCDIR/Pmml/JpmmlAdapter/src/main/resources/data/iris.csv.gz
