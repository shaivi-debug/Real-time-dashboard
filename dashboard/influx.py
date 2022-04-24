import json
from influxdb import InfluxDBClient
client=InfluxDBClient(host='localhost',port=8086,database='transaction')
client.create_database('transaction')
import kafka
from kafka import KafkaConsumer

consumer=KafkaConsumer('test2',bootstrap_servers=['localhost:9092'])
import pyspark
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import *

for message in consumer:
    json_body=[
               {
                 "measurement":'transaction_details',
                 "tags": {
                  },
                 "fields":json.loads(message.value)
               }
             ]

    client.write_points(json_body)
