#!/bin/sh
if [ "$KAFKA_HOME" = "" ]; then
    echo "Please set KAFKA_HOME"
    exit 1
fi

if [ $# -ne 6 ]; then
    echo "Usage: WatchQueue.sh -topic <TOPIC NAME>  -zookeeper <HOSTNAME:PORT> -from-beginning <true/false>"
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
  "-from-beginning")
    i=$((i+1))
    if [ "${!i}" == "true" ]; then
      FROM_BEGIN="--from-beginning"
    else
      FROM_BEGIN=""
    fi 
     ;;
esac
done

$KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic $TOPIC $FROM_BEGIN
