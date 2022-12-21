import unittest
import subprocess
import socket
import time
import mysql
from spark_kafka_mysql import write_to_mysql, insert_to_mysql
from kafka import KafkaProducer


class TestEndToEnd(unittest.TestCase):
    def test_end_to_end(self):
        # Start the netcat listener
        listener = subprocess.Popen(["nc", "-lk", "9999"])

        # Set up the Kafka producer
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

        # Send a test message to the Kafka topic
        topic = 'twitter-topic'
        message = 'Hello, Kafka!'
        producer.send(topic, message.encode('utf-8'))

        # Wait for the message to be processed
        time.sleep(10)

        # Connect to the netcat listener and verify that the message was received
        
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("localhost", 9999))
        received_message = s.recv(1024).decode("utf-8")
        self.assertEqual(received_message, message)

        # Disconnect from the netcat listener
        s.close()

      
        # Verify that the message was written to the MySQL database
        conn = mysql.connector.connect(
            host="localhost", user="root", password="password", database="twitter")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tweets")
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], message)

        # Close the MySQL connection
        conn.close()

        # Stop the netcat listener
        listener.kill()


class TestEndToEnd(unittest.TestCase):
    # Test functions go here
    


    if __name__ == '__main__':
        unittest.main()
