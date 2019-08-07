import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

reload(sys)
sys.setdefaultencoding('utf-8')
topic = 0
def processer(lines):
    counts = lines.flatMap(lambda line: line[1].split(" ") if len(line) > 0 else "") \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a+b).collect()
    if (len(counts)):
        producer.send("topic-send", "deepak kafka test")
        producer.flush()
        print(counts)

if __name__ == "__main__":

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2) #(sparkContext, Duration in sec) 
    
    brokers, topic = sys.argv[1:]
    
    kvs = KafkaUtils.createDirectStream(ssc, [topic],{"metadata.broker.list": brokers})
    kvs.foreachRDD(processer)

    ssc.start()
    ssc.awaitTermination()