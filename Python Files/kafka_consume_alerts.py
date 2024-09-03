# Import necessary libraries
import boto3
import json
from kafka import KafkaConsumer

# Create a kafka consumer
consumer = KafkaConsumer('Alert_Messages', bootstrap_servers = ['54.226.129.188:9092'] , auto_offset_reset = 'earliest', value_deserializer=lambda m: m.decode('utf-8'))

# Create an SNS client using boto
sns_client = boto3.client('sns',region_name='us-east-1')
sns_topic_arn = 'arn:aws:sns:us-east-1:117130372525:health_alert_system'

# Publish messages to SNS topic
for message in consumer:
    message_obj = json.loads(message.value)
    subject_header = 'Health Alert: Patient- ' + message_obj['patientname']
    result = sns_client.publish(TopicArn = sns_topic_arn, Message=  message_obj,  Subject = subject_header )
    print(result)
