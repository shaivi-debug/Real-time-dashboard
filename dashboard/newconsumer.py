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
        .withColumn("Datetime",to_timestamp(col("txn_datetime"),"yyyy-MM-dd HH-mm-ss")) \
        .withColumn("Male", expr("case when gender=='M' then 1 else 0 end")) \
        .withColumn("Female",expr("case when gender=='F' then 1 else 0 end")) \
        .withColumn("Amount",col("amount")) \
        .withColumn("Card_type",col("card_type")) \
        .withColumn("State",col("state"))
    window_agg_df=new_df \
                 .groupby(col("Card_type"),col("State"),window(col("Datetime"),"5 minutes")) \
                 .agg(sum("Male").alias("totalMale"),sum("Female").alias("totalFemale"),sum("Amount").alias("Amount"))
    output_df=window_agg_df.select(window_agg_df.window.start.cast("string").alias("start"),window_agg_df.window.end.cast("string").alias("end"),"Card_type","State","totalMale","totalFemale","Amount")
    # write data to any of these formats : csv, json, orc, parquet at frequency of 15 seconds
    output=output_df.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("checkpointLocation","/home/ubuntu/spark") \
        .trigger(processingTime="1 minute") \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    main()
