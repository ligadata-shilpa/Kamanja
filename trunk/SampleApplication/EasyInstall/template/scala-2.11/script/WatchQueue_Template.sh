#!/bin/sh
if [ "$KAFKA_HOME" = "" ]; then
    echo "Please set KAFKA_HOME"
    exit 1
fi
if [ $# -eq 2 ]; then
    echo "Usage: WatchQueue.sh -topic <TOPIC NAME>  -zookeeper <HOSTNAME:PORT>"
    exit 1
fi

for (( i=1; i<=$#; i++)); do
	case ${!i} in
  "-zookeeper")
    i=$((i+1))
    ZOOKEEPER="${!i}"
     ;;
  "-topic")
    i=$((i+1))
    TOPIC="${!i}"
     ;;
esac
done

$KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic $TOPIC --from-beginning
