import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType
import os
#os.environ['PYSPARK_SUBMIT_ARGS']='--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 pyspark-shell'
os.environ['PYSPARK_PYTHON']="usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON']="usr/bin/python3"
def main():
    spark = SparkSession \
            .builder \
            .appName('sparktest') \
            .master("spark://ip-172-31-4-90.ap-south-1.compute.internal:7077") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.shuffle.partitions", 2) \
            .config("spark.driver.host","172.31.4.90") \
            .getOrCreate()
    schema = StructType([
        StructField('card_no', StringType(), True),
        StructField('age', StringType(), True),
        StructField('gender', StringType(), True),
        StructField('card_type', StringType(), True),
        StructField('city', StringType(), True),
        StructField('state', StringType(), True),
        StructField('txn_datetime', TimestampType(), True),
        StructField('amount', IntegerType(), True),
        StructField('expense_type', StringType(), True),
        StructField('txn_type', StringType(), True),
        StructField('merchant', StringType(), True),
        StructField('payment_type', StringType(), True)
    ])

    input_stream = spark\
        .readStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', 'localhost:9092')\
        .option('subscribe', 'test1')\
        .option("failOnDataLoss", "false")\
        .load()\
        .select(from_json(col("value").cast("string"), schema).alias("json_struct"))
    new_df=input_stream.select("json_struct.*") \
        .withColumn("Card_no",col("card_no")) \
        .withColumn("Datetime",to_timestamp(col("txn_datetime"),"yyyy-MM-dd HH:mm:ss")) \
        .withColumn("Male", expr("case when gender=='M' then 1 else 0 end")) \
        .withColumn("Female",expr("case when gender=='F' then 1 else 0 end")) \
        .withColumn("Revenue",col("amount")) \
        .withColumn("Platinum",expr("case when card_type=='Platinum' then 1 else 0 end")) \
        .withColumn("Silver",expr("case when card_type=='Silver' then 1 else 0 end")) \
        .withColumn("Gold",expr("case when card_type=='Gold' then 1 else 0 end")) \
        .withColumn("Signature",expr("case when card_type=='Signature' then 1 else 0 end")) \
        .withColumn("Expense_type",col("expense_type")) \
        .withColumn("State",col("state"))
    final_df=new_df.selectExpr("Card_no as key","""to_json(struct(Datetime,Male,Female,Platinum,Silver,Gold,Signature,State,Revenue,Expense_type)) as value""")
    output=final_df.writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers","localhost:9092") \
        .option("topic","test2") \
        .option("checkpointLocation","/home/ubuntu/spark") \
        .trigger(processingTime="5 seconds") \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    main()
