# Remove all logs form spark startup	
In conf/log4j.properties change log4j.rootCategory=ERROR, console
# Running spark job
spark-submit rdd/WordCount.py 

# Kafka connect
# send message
echo “this is just a test” | ./kafka-console-producer.sh --broker-list localhost:9092 --topic new_topic

# receive message
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic new_topic --from-beginning

# run spark with kafka
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 ./kafka_spark_connect.py localhost:9092 new_topic