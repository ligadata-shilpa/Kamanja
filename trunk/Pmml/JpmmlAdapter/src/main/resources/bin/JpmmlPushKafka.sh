#/bin/bash

# Accept the files on the command line and send them to the kafka producer.  All files assumed to be compressed CSV

if [[ $# -gt 1 ]]; then
	useCompressed="$1"
	shift
	dataFiles="$*"
	if [ "$useCompressed" == "1" ]; then
		compressedInput="true"
	else
		compressedInput="false"
	fi

	FORMAT="CSV"
	java -jar $KAMANJA_HOME/bin/SimpleKafkaProducer-0.1.0 --gz "$compressedInput" --topics "testin_1" --threads 1 --topicpartitions 8 --brokerlist "localhost:9092" --files "$dataFiles" --partitionkeyidxs "1" --format $FORMAT
else
	echo
	echo "Push data files through Kafka Input Adapter.  Supply a flag indicating if the data is compressed or not followed by "
	echo "an arbitrary number of input paths to be pushed through the input adapter."
	echo
	echo "Note: all files currently assumed to be CSV format."
	echo
	echo "Usage:"
	echo "   JpmmlPushKafka.sh <compressedInputFlag {0|1} <filepath1> [<filepath2 <filepath3> ... <filepathN>]"
	echo
fi
