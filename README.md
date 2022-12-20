# Data Streaming using Spark, Kafka and MySQL

This script appears to be for a Spark streaming application that consumes data from a Kafka topic, processes it, and writes it to a MySQL database. It also pipes the data to the nc (netcat) command, which listens on port 9999 for incoming data and writes it to standard output.

The script starts by importing a number of libraries, including logging, sys, findspark, pyspark, subprocess, and mysql.connector. It then configures logging with logging.basicConfig().

Next, the script sets up the Spark context and the streaming context using pyspark.SparkContext() and StreamingContext(), respectively. It then prompts the user for various input values, such as the path to the Spark installation, the Kafka topic, and the MySQL database login details.

After setting up the Kafka stream with KafkaUtils.createStream(), the script processes the stream by extracting the tweet text and creating a new tweets object with tweets = kafka_stream.map(lambda x: x[1]).

The script then opens a new terminal window and starts the nc command in it with subprocess.call(["open", "-a", "Terminal", "nc", "-lk", "9999"]).

Finally, the script defines two functions: write_to_mysql() and insert_to_mysql(). The write_to_mysql() function takes an RDD as input and calls the insert_to_mysql() function on its partitions. The insert_to_mysql() function connects to the MySQL database, inserts the records from the input RDD into the tweets table, and closes the connection. The insert_to_mysql() function also pipes the records to the nc command through subprocess.call().

The script then starts the streaming context with ssc.start() and waits for it to terminate with ssc.awaitTermination(). When the streaming context is stopped, the script stops the Spark context with ssc.stop().
