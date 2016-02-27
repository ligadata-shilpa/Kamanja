**Introduction**

This project contains a simple application that can be used by those that have some familiarity with the pmml source providers (e.g., R/Rattle, KNIME, RapidMiner, SAS) and consumers (KNIME, Zementis' Adapa).

Kamanja also consumes pmml, but in a way that accomodates the Kamanja engine.  Its goal is to run as many models as desired as quickly as desired through the kamanja cluster.  To do so, Kamanja insists that all incoming messages from queues (e.g., Kafka, Ibm Mq, etc.) and files received through one of Kamanja's file adapters be mapped to a message.  

In other words, the DataFields that comprise the input message for a pmml model must have a message declared for it and cataloged before the pmml can be added to the Kamanja metadata and data processed/scored by the model.

For someone new to Kamanja, they just want to try it out with models that they perhaps are running in other
scoring engines.

To this end, this application will immediately solve part of this: you will be able to submit your pmml and some dataset to Kamanja via one of its file adapter instances, BUT you will for the time being have to create the message declaration yourself.  What are these?  Here is how the ubiquitous Iris data set is expressed in a fashion that can be consumed by the Kamanja metadata ingestion:

{
  "Message": {
    "NameSpace": "system",
    "Name": "irismsg",
    "Version": "00.01.01",
    "Description": "Iris data message",
    "Fixed": "true",
    "Persist": "true",
    "Elements": [
      {
        "Field": {
          "Name": "serialnumber",
          "Type": "system.Double"
        }
      },
      {
        "Field": {
          "Name": "sepal_length",
          "Type": "system.Double"
        }
      },
      {
        "Field": {
          "Name": "sepal_width",
          "Type": "system.Double"
        }
      },
      {
        "Field": {
          "Name": "petal_length",
          "Type": "system.Double"
        }
      },
      {
        "Field": {
          "Name": "petal_width",
          "Type": "system.Double"
        }
      },
      {
        "Field": {
          "Name": "irisclass",
          "Type": "system.String"
        }
      }
    ],
  }
}

It is just a *simple JSON* definition.  The command that would ingest it into Kamanja metadata is like this:

    $KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add message /home/myacct/KamanjaKickTheTires/Utils/message/IrisMsg1.json


The KAMANJA_HOME environment variable would be the location of your Kamanja installation.  The path to the Iris message (Iris1.json) could be anything.

If you were to add this message to the Kamanja cluster, then you can run any of the pmml tests that consume the Iris dataset.  Of course the dataset header in the csv dataset must share the same field names as are used in the model.

**Running the PmmlTestTool**

Assuming that you have cataloged the message as described in the **Introduction**, you would use the following command to execute run the tool:

    jar -Dlog4j.configurationFile=file:<KAMANJA_HOME>/bin/log4j2.xml -jar KAMANJA_HOME>/bin/PmmlTestTool-1.0 --pmml <path to your pmml file> --msg msgNamespace.msgName --dataset <path to your dataset>

This is pretty straight forward.  The only element that might be confusing is the --msg value.  The *msgNamespace.msgName* is taken from the message declaration.  In the Iris example in the **Introduction**, this value would be "system.irismsg".  Note that the namespace and the name are totally described by this. If desired, your namespace could look very much like a java or scala namespace (e.g., *com.yourcompany.kamanja.msg*) making your name be *com.yourcompany.kamanja.msg.irismsg*

Executing this tool with the same command can be done over and over.  It is idempotent.  What happens is that the model is added, the data is pushed to the engine, the engine loads your model, and gives all of the data from each line of your dataset to the model.  After the model dataset is consumed, a respectful time is waited (5 seconds by default), and then the model is deleted from Kamanja, putting the script back to a ready state for resubmitting the same command.

The full syntax for the command is:

	jar -Dlog4j.configurationFile=file:<KAMANJA_HOME>/bin/log4j2.xml -jar KAMANJA_HOME>/bin/PmmlTestTool-1.0 
			--pmmlSrc <path to your pmml file> 
      --pmmlId <any name conforming to the java package/identifier syntax... e.g., some.company.pmml.model.MyModel>
			--msg msgNamespace.msgName ... the names from the message declaration
			--dataset <path to your dataset>
      [--separator <any of {"\t", ";", ","}> ... default is ","]
      [--quiesceSecs <5 as default> ... time after all records pushed to wait for engine to complete its work]
			[--keepModel ... will not delete the model when processing completes]
      [--omitInputs ... if supplied, only output and target fields are emitted]

**Next Steps**

In a subsequent sprint, the tool will generate the message and catalog it for you automatically before cataloging the model and scoring the supplied dataset.  As before, there will be a clean up that will allow you to run the command again if desired.

In practice, the messages and models are cataloged once.  The dataset or datasets can then be pushed.  Should the user wish to eliminate or deactivate the model, it would be done with explicit commands.  There is documentation in the wiki that goes into detail about this.

The full syntax for the command is:

    jar -Dlog4j.configurationFile=file:<KAMANJA_HOME>/bin/log4j2.xml -jar KAMANJA_HOME>/bin/PmmlTestTool-1.0 
      --pmmlSrc <path to your pmml file> 
      --pmmlId <any name conforming to the java package/identifier syntax... e.g., some.company.pmml.model.MyModel>
      --msg msgNamespace.msgName ... this can be any identifer comprised of [A-Za-z0-9_.] that meets the Java package and identifer syntax
      --dataset <path to your dataset>
      [--separator <any of {"\t", ";", ","}> ... default is ","]
      [--quiesceSecs <5 as default> ... time after all records pushed to wait for engine to complete its work]
      [--keepModel ... will not delete the model when processing completes]
      [--keepMsg ... will not delete the generated message automatically]
      [--omitInputs ... if supplied, only output and target fields are emitted]


**Sample Execution**

export KAMANJA_SRCDIR=/home/rich/github/dev/KAMANJA-366/kamanja/trunk
export LOG4J_HOME=$KAMANJA_SRCDIR/Utils/PmmlTestTool/src/main/resources
export ipport=8998
cd $KAMANJA_SRCDIR

$KAMANJA_SRCDIR/Utils/PmmlTestTool/target/PmmlTestTool-1.0 --pmmlSrc $KAMANJA_SRCDIR/Utils/PmmlTestTool/src/main/resources/metadata/model/KNIME/DecisionTreeEnsembleIris.pmml --dataset $KAMANJA_SRCDIR/Utils/PmmlTestTool/src/main/resources/data/iris.raw.csv | wc -l

$KAMANJA_SRCDIR/Utils/PmmlTestTool/target/PmmlTestTool-1.0 --pmmlSrc $KAMANJA_SRCDIR/Utils/PmmlTestTool/src/main/resources/metadata/model/KNIME/DecisionTreeEnsembleIris.pmml --dataset $KAMANJA_SRCDIR/Utils/PmmlTestTool/src/main/resources/data/iris.raw.csv --omitInputs >/tmp/results.txt
