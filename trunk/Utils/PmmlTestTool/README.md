**Introduction**

This project contains a simple application that can be used by those that have some familiarity with the pmml source providers (e.g., R/Rattle, KNIME, RapidMiner, SAS) and consumers (KNIME, Zementis' Adapa).

This tool *does not* use Kamanja, but allows the user to quickly test their generated pmml model from their model development environment.


**Running the PmmlTestTool**

Assuming that you have cataloged the message as described in the **Introduction**, you would use the following command to execute run the tool:

    jar -Dlog4j.configurationFile=file:<KAMANJA_HOME>/bin/log4j2.xml -jar KAMANJA_HOME>/bin/PmmlTestTool-1.0 --pmml <path to your pmml file> --msg msgNamespace.msgName --dataset <path to your dataset>

This is pretty straight forward.  The only element that might be confusing is the --msg value.  The *msgNamespace.msgName* is taken from the message declaration.  In the Iris example in the **Introduction**, this value would be "system.irismsg".  Note that the namespace and the name are totally described by this. If desired, your namespace could look very much like a java or scala namespace (e.g., *com.yourcompany.kamanja.msg*) making your name be *com.yourcompany.kamanja.msg.irismsg*

Executing this tool with the same command can be done over and over.  It is idempotent.  What happens is that the model is added, the data is pushed to the engine, the engine loads your model, and gives all of the data from each line of your dataset to the model.  After the model dataset is consumed, a respectful time is waited (5 seconds by default), and then the model is deleted from Kamanja, putting the script back to a ready state for resubmitting the same command.

The full syntax for the command is:

	jar -Dlog4j.configurationFile=file:<KAMANJA_HOME>/bin/log4j2.xml -jar KAMANJA_HOME>/bin/PmmlTestTool-1.0 
			--pmmlSrc <path to your pmml file> 
			--dataset <path to your dataset>
      [--separator <any of {"\t", ";", ","}> ... default is ","]
      [--omitInputs ... if supplied, only output and target fields are emitted]


**Sample Execution**

export KAMANJA_SRCDIR=/home/rich/github/dev/KAMANJA-366/kamanja/trunk
export LOG4J_HOME=$KAMANJA_SRCDIR/Utils/PmmlTestTool/src/main/resources
export ipport=8998
cd $KAMANJA_SRCDIR

$KAMANJA_SRCDIR/Utils/PmmlTestTool/target/PmmlTestTool-1.0 --pmmlSrc $KAMANJA_SRCDIR/Utils/PmmlTestTool/src/main/resources/metadata/model/KNIME/DecisionTreeEnsembleIris.pmml --dataset $KAMANJA_SRCDIR/Utils/PmmlTestTool/src/main/resources/data/iris.raw.csv | wc -l

$KAMANJA_SRCDIR/Utils/PmmlTestTool/target/PmmlTestTool-1.0 --pmmlSrc $KAMANJA_SRCDIR/Utils/PmmlTestTool/src/main/resources/metadata/model/KNIME/DecisionTreeEnsembleIris.pmml --dataset $KAMANJA_SRCDIR/Utils/PmmlTestTool/src/main/resources/data/iris.raw.csv --omitInputs >/tmp/results.txt

#java -Xdebug -Xrunjdwp:transport=dt_socket,address="$ipport",server=y -Dlog4j.configurationFile=file:$KAMANJA_HOME/config/log4j2.xml -jar $KAMANJA_SRCDIR/Utils/PmmlTestTool/target/PmmlTestTool-1.0 --pmmlSrc $KAMANJA_SRCDIR/Utils/PmmlTestTool/src/main/resources/metadata/model/KNIME/DecisionTreeEnsembleIris.pmml --dataset $KAMANJA_SRCDIR/Utils/PmmlTestTool/src/main/resources/data/iris.raw.csv --omitInputs >/tmp/results.txt
