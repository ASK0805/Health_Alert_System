from kafka import KafkaProducer
import mysql.connector
from json import dumps
import json
from time import sleep

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers='localhost:9092')

import mysql.connector

# Correct connection details without 'http://'
mydb = mysql.connector.connect(
    host="upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com",  # Remove 'http://'
    user="student",   # e.g., "admin"
    password="STUDENT123", # your database password
    database="testdatabase"  # the database name
)

# Check if the connection was successful
if mydb.is_connected():
    print("Connection successful")
else:
    print("Connection failed")


cursor = mydb.cursor()
cursor.execute("Select * from patients_vital_info")
table1 = cursor.fetchall()
cursor.close()
mydb.close()

producer = KafkaProducer (bootstrap_servers = ["localhost:9092"], value_serializer = lambda m:dumps(m).encode('utf-8'))

for i in table1 :
    data = {"customer":i[0], "heartBeat":i[1], "bp":i[2]}
    producer.send("vital-info", data)
    sleep(1)
