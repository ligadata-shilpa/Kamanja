#!/bin/bash

##########################################################################################
# pmml-evaluator-test.sh
#
# This script may be used to collect comma delimited outputs from the model supplied by the
# user using the jpmml-evaluator's unit test tool.  This is useful to collect "expected" results
# that can be used to compare with outputs produced by kamanja for the same model.
#
# Method: Invoke the supplied pmml and process the supplied input.  Produce results to stdout.
#
# This script will invoke the EvaluationExample found in the $JPMML_TEST_BASE location's target directory
# This ENV variable must be set to the installation of the jpmml-evaluator/pmml-evaluator-example directory that
# is to be used to produce results.
#
# Both the model and input variables are supplied.  They must exist.
##########################################################################################

model=
input=
if [ "$# != 2" ]; then
	model="$1"
	input="$2"
fi
if [ ! -f "$model" ]; then
	echo "Bad model path..."
	echo "Usage: $0 <pmml model file> <data file>"
	exit 1
fi

if [ ! -f "$input" ]; then
	echo "Bad input data path..."
	echo "Usage: $0 <pmml model file> <data file>"
	exit 1
fi

#echo
#echo Execute:
#echo java -cp $JPMML_TEST_BASE/target/example-1.2-SNAPSHOT.jar org.jpmml.evaluator.EvaluationExample --model "$model" --input "$input" --output /tmp/output.tsv
#echo
java -cp $JPMML_TEST_BASE/target/example-1.2-SNAPSHOT.jar org.jpmml.evaluator.EvaluationExample --model "$model" --input "$input" --output /tmp/output.tsv
if [ -f "/tmp/output.tsv" ]; then
	cat /tmp/output.tsv
	echo
else
	echo "org.jpmml.evaluator.EvaluationExample produced had a problem producing output"
fi

