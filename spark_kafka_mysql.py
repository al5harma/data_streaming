# Import necessary libraries
import logging
import sys
import findspark
findspark.init('/usr/spark/spark-3.0.0-preview2-bin-hadoop2.7')
import pyspark
import subprocess
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import mysql.connector

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    filename="logfile.log", filemode="w")

# Set up the Spark context and the streaming context

sc = pyspark.SparkContext(appName="TwitterStream")
ssc = StreamingContext(sc, 10)  # batch interval of 10 seconds

# Get user input for the required variables
spark_home = input("Enter the path to your Spark installation: ")
zk_quorum = input("Enter the Zookeeper quorum (host:port): ")
consumer_group = input("Enter the consumer group name: ")
kafka_topic = input("Enter the Kafka topic name: ")
batch_interval = int(input("Enter the batch interval in seconds: "))
mysql_host = input("Enter the MySQL hostname: ")
mysql_user = input("Enter the MySQL username: ")
mysql_password = input("Enter the MySQL password: ")
mysql_database = input("Enter the MySQL database name: ")

# Set up the Kafka stream
kafka_stream = KafkaUtils.createStream(
    ssc, "localhost:2181", "spark-streaming-consumer", {"twitter-topic": 1})

# Process the stream
tweets = kafka_stream.map(lambda x: x[1])  # x[1] contains the tweet text

# Start the netcat listener in a separate terminal window
subprocess.call(["open", "-a", "Terminal", "nc", "-lk", "9999"])

# Write the tweets to the MySQL database and pipe them to netcat


def write_to_mysql(rdd):
    try:
        rdd.foreachPartition(lambda records: insert_to_mysql(records))
    except Exception as e:
        logging.error("Error writing to MySQL: %s", e)
        sys.exit(1)


def insert_to_mysql(records):
    try:
        for record in records:
            subprocess.call(["echo", record, "|", "nc", "localhost", "9999"])
        conn = mysql.connector.connect(
            host="localhost", user="root", password="password", database="twitter")
        cursor = conn.cursor()
        for record in records:
            cursor.execute("INSERT INTO tweets (text) VALUES (%s)", (record,))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error("Error inserting records to MySQL: %s", e)
        sys.exit(1)


tweets.foreachRDD(write_to_mysql)

# Start the streaming context and process the stream
ssc.start()
ssc.awaitTermination()
ssc.stop()
