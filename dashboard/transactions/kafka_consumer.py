
import kafka
from kafka import KafkaConsumer 
import sys
import json

def json_dserialize(data):
       return json.loads(data.decode('utf-8'))

consumer=KafkaConsumer('test1',bootstrap_servers=['localhost:9092'],auto_offset_reset='earliest',
                        value_deserializer=json_dserialize)
for card in consumer:
       print(card)
       card=json.loads(card[6])
       print("card_no: ",card["card_no"])
       print("age: ",card['age'])
       print("gender: ",card['gender'])
       print("card_type: ",card['card_type'])
       print("city: ",card['city'])
       print("state: ",card['state'])
       print("txn_datetime: ",card['txn_datetime'])
       print("amount: ",card['amount'])
       print("expense_type: ",card['expense_type'])
