java -jar {InstallDirectory}/bin/SimpleKafkaProducer-0.1.0 --gz true --topics "testin_1" --threads 1 --topicpartitions 8 --brokerlist "localhost:9092" --files "{InstallDirectory}/input/HelloWorld/data/HelloWorld_Input_Data.csv.gz" --partitionkeyidxs "1" --format CSV
