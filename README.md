# Data Streaming using Spark, Kafka and MySQL

This code sets up a Spark Streaming context and creates a Kafka stream for processing Twitter data that is stored in a Kafka topic. The tweets are then written to a MySQL database and piped to a netcat listener running on port 9999.

The code starts by importing the necessary libraries, including pyspark and mysql.connector, and configuring logging. Then, it creates a SparkContext and a StreamingContext with a batch interval of 10 seconds.

Next, the code prompts the user to input the necessary variables, including the Zookeeper quorum, consumer group, Kafka topic, and MySQL connection details. With these variables, the code creates a Kafka stream using the KafkaUtils.createStream() method.

The code then processes the stream by extracting the tweet text and storing it in a variable called "tweets". It also starts a netcat listener in a separate terminal window using the subprocess.call() method.

Finally, the code defines a write_to_mysql() function that takes an RDD as input and writes the data in the RDD to the MySQL database. It does this by calling the insert_to_mysql() function, which inserts each record in the RDD into the "tweets" table in the MySQL database and pipes it to the netcat listener.

Once the streaming context has been set up, the code starts it and waits for it to be terminated, at which point it stops the context.
