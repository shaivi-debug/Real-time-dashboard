#!/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime
import os
import configparser
import random
import pandas as pd
from time import sleep
from influxdb_client import InfluxDBClient


# In[2]:


config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
config.read(os.path.abspath('dataGenerator.config'))


# In[3]:


df_cards=pd.read_csv(os.path.abspath('cards.csv'))


# In[4]:


limit_transaction_generation_speed=config.get('transaction_generator', 'limit_transaction_generation_speed')
food_min_max=config.get('transaction_generator','food_min_max')
bill_min_max=config.get('transaction_generator','bill_min_max')
grocery_min_max=config.get('transaction_generator','grocery_min_max')
entertainment_min_max=config.get('transaction_generator','entertainment_min_max')
fuel_min_max=config.get('transaction_generator','fuel_min_max')
travel_min_max=config.get('transaction_generator','travel_min_max')
shopping_min_max=config.get('transaction_generator','shopping_min_max')
time_delay_in_seconds=config.get('transaction_generator','time_delay_in_seconds')


# In[5]:


expense_types=[{'expense':'Bills','expense_min':int(bill_min_max.split(',')[0]),'expense_max':int(bill_min_max.split(',')[1])},
     {'expense':'Grocery','expense_min':int(grocery_min_max.split(',')[0]),'expense_max':int(grocery_min_max.split(',')[1])},
     {'expense':'Food','expense_min':int(food_min_max.split(',')[0]),'expense_max':int(food_min_max.split(',')[1])},
     {'expense':'Entertainment','expense_min':int(entertainment_min_max.split(',')[0]),'expense_max':int(entertainment_min_max.split(',')[1])},
     {'expense':'Fuel','expense_min':int(fuel_min_max.split(',')[0]),'expense_max':int(fuel_min_max.split(',')[1])},
     {'expense':'Travel','expense_min':int(travel_min_max.split(',')[0]),'expense_max':int(travel_min_max.split(',')[1])},
     {'expense':'Shopping','expense_min':int(shopping_min_max.split(',')[0]),'expense_max':int(shopping_min_max.split(',')[1])}]


# In[6]:


curr_dt=datetime.now()
filename='transactions_'+curr_dt.strftime('%Y%m%d_%H%M%S')+'.csv'


# In[7]:


def generate_transaction(card_details):
    card=card_details.sample()
    expense=random.choice(expense_types)
    current_datetime=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    transaction_detail=pd.DataFrame({"card_no":card['card_no'],
              "age":card['age'],
              "gender":card['gender'],
              "card_type":card['card_type'],
              "city":card['city'],
              "state":card['state'],
              "txn_datetime":current_datetime,
              "amount":random.randint(expense['expense_min'],expense['expense_max']),
              "expense_type":expense['expense']})
    return transaction_detail


# In[8]:


def get_transaction(card_details):
    df_transaction_details = pd.DataFrame(columns = ['card_no', 'age', 'gender','card_type','city','state','txn_datetime','amount','expense_type'])
    df_transaction_details=df_transaction_details.append(generate_transaction(card_details))
    return df_transaction_details


# In[9]:
import sqlalchemy as sq
con=sq.create_engine("mysql+pymysql://ubuntu:ubuntu@localhost/transaction")

if(limit_transaction_generation_speed=='Y'):
    while(True):
        df=get_transaction(df_cards)
        df.to_sql("TRANSACTION_DETAIL",con ,if_exists="append",index=False)
        sleep(float(time_delay_in_seconds))
else:
    while(True):
        df=get_transaction(df_cards)
        df.to_sql("TRANSACTION_DETAIL",con,if_exists="append",index=False)


# In[ ]:




