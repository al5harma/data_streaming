# Data Streaming using Spark, Kafka and MySQL

This script appears to be for a Spark streaming application that consumes data from a Kafka topic, processes it, and writes it to a MySQL database. It also pipes the data to the nc (netcat) command, which listens on port 9999 for incoming data and writes it to standard output.

The script starts by importing a number of libraries, including logging, sys, findspark, pyspark, subprocess, and mysql.connector. It then configures logging with logging.basicConfig().

Next, the script sets up the Spark context and the streaming context using pyspark.SparkContext() and StreamingContext(), respectively. It then prompts the user for various input values, such as the path to the Spark installation, the Kafka topic, and the MySQL database login details.

After setting up the Kafka stream with KafkaUtils.createStream(), the script processes the stream by extracting the tweet text and creating a new tweets object with tweets = kafka_stream.map(lambda x: x[1]).

The script then opens a new terminal window and starts the nc command in it with subprocess.call(["open", "-a", "Terminal", "nc", "-lk", "9999"]).

Finally, the script defines two functions: write_to_mysql() and insert_to_mysql(). The write_to_mysql() function takes an RDD as input and calls the insert_to_mysql() function on its partitions. The insert_to_mysql() function connects to the MySQL database, inserts the records from the input RDD into the tweets table, and closes the connection. The insert_to_mysql() function also pipes the records to the nc command through subprocess.call().

The script then starts the streaming context with ssc.start() and waits for it to terminate with ssc.awaitTermination(). When the streaming context is stopped, the script stops the Spark context with ssc.stop().

## To run this code, you will need to have the following prerequisites installed and configured:

- **Python 3**: This code is written in Python, so you will need to have Python 3 installed on your system.
- **TextBlob library**: The code imports the TextBlob library, which is used to perform sentiment analysis on tweets. You can install this library by running pip install textblob on the command line.
- **pyspark**: The code uses the PySpark library to process the stream of tweets from Kafka. You will need to have PySpark installed on your system and the path to the Spark installation added to your system's PATH environment variable.
- **Kafka**: This code streams tweets from Kafka, so you will need to have a Kafka server running and the required topic created.
- **mysql-connector-python**: The code uses this library to connect to and write data to a MySQL database. You will need to have this library installed and have a MySQL server running on your system.
- **netcat**: The code uses netcat (nc) to pipe the tweets to a separate terminal window. You will need to have nc installed on your system.
- **Access to the required input variables**: The code prompts the user for input for various variables, including the path to the Spark installation, the Zookeeper quorum, the consumer group name, the Kafka topic name, the batch interval in seconds, the MySQL hostname, username, password, and database name. 

## You will need to have this information available in order to run the code.

## What to provide as input 

- The spark_home variable should contain the path to your 
  > Spark installation.

- The zk_quorum variable should contain the host and port of the Zookeeper quorum, in the format 
  > host:port.

- The consumer_group variable should contain the 
  > name of the consumer group to use when reading from Kafka.

- The kafka_topic variable should contain the 
  > name of the Kafka topic to read from.
- The batch_interval variable should contain the 
  > batch interval in seconds to use when processing the stream.
- The mysql_host variable should contain the 
  > hostname of the MySQL server.
- The mysql_user variable should contain the 
  > username to use when connecting to the MySQL server.
- The mysql_password variable should contain the 
  > password for the specified MySQL user.
- The mysql_database variable should contain the name of the 
  >  MySQL database to use.

## Here is an example for expected inputs - 

- Enter the path to your Spark installation: 
    > /usr/spark/spark-3.0.0-preview2-bin-hadoop2.7
- Enter the Zookeeper quorum (host:port): 
    > localhost:2181
- Enter the consumer group name: 
    > spark-streaming-consumer
- Enter the Kafka topic name: 
    > twitter-topic
- Enter the batch interval in seconds: 
    > 10
- Enter the MySQL hostname: 
    > localhost
- Enter the MySQL username: 
    > root
- Enter the MySQL password: 
    > password
- Enter the MySQL database name: 
    > twitter
