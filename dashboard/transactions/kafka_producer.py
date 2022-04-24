from datetime import datetime
import os
import configparser
import random
import pandas as pd
from time import sleep
#import boto3
#import fsspec
#import s3fs
import kafka
import json

#import boto3
import configparser

config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation()) 
config.read(os.path.abspath('dataGenerator.config'))

col_list = ["card_no","age","gender","card_type","city","state"]
df_cards=pd.read_csv('cards.csv',usecols=col_list)

limit_transaction_generation_speed=config.get('transaction_generator', 'limit_transaction_generation_speed')
food_min_max=config.get('transaction_generator','food_min_max')
bill_min_max=config.get('transaction_generator','bill_min_max')
grocery_min_max=config.get('transaction_generator','grocery_min_max')
entertainment_min_max=config.get('transaction_generator','entertainment_min_max')
fuel_min_max=config.get('transaction_generator','fuel_min_max')
travel_min_max=config.get('transaction_generator','travel_min_max')
shopping_min_max=config.get('transaction_generator','shopping_min_max')
time_delay_in_seconds=config.get('transaction_generator','time_delay_in_seconds')


 
expense_types=[{'expense':'Bills','expense_min':int(bill_min_max.split(',')[0]),'expense_max':int(bill_min_max.split(',')[1])},  
{'expense':'Grocery','expense_min':int(grocery_min_max.split(',')[0]),'expense_max':int(grocery_min_max.split(',')[1])},  
{'expense':'Food','expense_min':int(food_min_max.split(',')[0]),'expense_max':int(food_min_max.split(',')[1])}, 
{'expense':'Entertainment','expense_min':int(entertainment_min_max.split(',')[0]),'expense_max':int(entertainment_min_max.split(',')[1])}, 
{'expense':'Fuel','expense_min':int(fuel_min_max.split(',')[0]),'expense_max':int(fuel_min_max.split(',')[1])}, 
{'expense':'Travel','expense_min':int(travel_min_max.split(',')[0]),'expense_max':int(travel_min_max.split(',')[1])}, 
{'expense':'Shopping','expense_min':int(shopping_min_max.split(',')[0]),'expense_max':int(shopping_min_max.split(',')[1])}] 

txn_type=['Online','Swipe']
merchant=['M1','M2','M3','M4'] 
payment_type=['P1','P2']

def generate_transaction(card_details):
    card=card_details.sample()
    expense=random.choice(expense_types)
    current_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    transaction_detail=({"card_no":card['card_no'],
              "age":card['age'],
              "gender":card['gender'],
              "card_type":card['card_type'],
              "city":card['city'],
              "state":card['state'],
              "txn_datetime":current_datetime,
              "amount":random.randint(expense['expense_min'],expense['expense_max']),
              "expense_type":expense['expense'],
              "txn_type":random.choice(txn_type),
              "merchant":random.choice(merchant),
              "payment_type":random.choice(payment_type)
                        })
    return transaction_detail

def get_transaction(card_details):
    df=generate_transaction(df_cards)
    return {"card_no":df['card_no'].to_string(index=False),
        "age":df['age'].to_string(index=False),
        "gender":df['gender'].to_string(index=False),
        "card_type":df['card_type'].to_string(index=False),
        "city":df['city'].to_string(index=False),
        "state":df['state'].to_string(index=False),
        "txn_datetime":df['txn_datetime'],
        "amount":df['amount'],
        "expense_type":df['expense_type'],
        "txn_type":df['txn_type'],
        "merchant":df['merchant'],
        "payment_type":df['payment_type']
    }

from kafka import KafkaProducer
import json

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

#producer = KafkaProducer(bootstrap_servers=['b-1.demo-cluster-1.q1lynb.c16.kafka.us-east-1.amazonaws.com:9092','b-2.demo-cluster-1.q1lynb.c16.kafka.us-east-1.amazonaws.com:9092']
 #                        ,value_serializer=json_serializer
  #                       )
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=json_serializer)

if(1==1):
    while(True):
        cc_data = get_transaction(df_cards)
        print(cc_data)
        producer.send("test1", cc_data)
        sleep(2)
