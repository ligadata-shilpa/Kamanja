#!/bin/sh
if [ "$KAFKA_HOME" = "" ]; then
    echo "Please set KAFKA_HOME"
    exit 1
fi
if [ $# -eq 0 ] || [ $# -gt 2 ]; then
    echo "Usage: WatchQueue.sh -zookeeper <hostname:port>  -replication-factor <rep factor value>  -partitions <no of partitions> -topic <topic name>"
    echo "Minimum usage: WatchQueue.sh -topic <topic name>"
    exit 1
fi

if [ $# -eq 2 ] && [ $1 != "-topic" ]; then
 echo "Minimum usage: WatchQueue.sh -topic <topic name>"
 exit 1
fi

if [ $# -eq 2 ] && [ $1 == "-topic" ]; then
 echo "Loading default values for others: "
 echo "zookeeper localhost:2181"
 echo "replication-factor 1"
 echo "partitions 8"
 ZOOKEEPER="localhost:2181"
 REP_FACTOR=1
 PARTITION=8
fi

for (( i=1; i<=$#; i++)); do
	case ${!i} in
  "-zookeeper")
    i=$((i+1))
    ZOOKEEPER="${!i}"
     ;;
  "-replication-factor")
    i=$((i+1))
   REP_FACTOR="${!i}"
     ;;
  "-partitions")
    i=$((i+1))
    PARTITION="${!i}"
     ;;
  "-topic")
    i=$((i+1))
    TOPIC="${!i}"
     ;;
esac
done

$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper $ZOOKEEPER  --replication-factor $REP_FACTOR  --partitions $PARTITION  --topic $TOPIC