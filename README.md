# kafka-tutorial
Code followed from the YouTube tutorial: Apache Kafka Tutorials from Learning Journal

#Basic commands
- Start zookeeper
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
- Start a broker
kafka-server-start /usr/local/etc/kafka/server.properties
- Create a topic
kafka-topics --zookeeper localhost:2181 --create --topic <topic_name> --partitions 2 --replication-factor 3
- Start a Consumer
kafka-console-consumer --zookeeper localhost:2181 --topic test-topic --from-beginning
- Start a Producer
kafka-console-producer --broker-list localhost:9092 --topic test-topic
