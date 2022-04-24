import ast
import kafka
from kafka import KafkaConsumer 
import sys
import json

def json_dserialize(data):
       return json.loads(data)

consumer=KafkaConsumer('test1',bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',
                     value_deserializer=json_dserialize)
for card in consumer:
        print(card.value)
