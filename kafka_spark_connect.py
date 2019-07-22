# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils

# # Create a local StreamingContext with two working thread and batch interval of 1 second
# sc = SparkContext("local[2]", "KafkaStreamingConsumer")
# ssc = StreamingContext(sc, 10)


# # kafkaStream = KafkaUtils.createStream(streamingContext, \
# #      [ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume])

# kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181", "consumer-group", {"testTopic": 1})

# kafkaStream.saveAsTextFiles('out.txt')

# # print('Event recieved in window: ', kafkaStream.pprint())

# ssc.start()
# ssc.awaitTermination()

import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 0.1) #(sparkContext, Duration in sec) 
    
    brokers, topic = sys.argv[1:]
    
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})

    lines = kvs.map(lambda x: x[1])
    
    counts = lines.flatMap(lambda line: line.split(" ")) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a+b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()