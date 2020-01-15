from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from time import sleep

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-streaming-kafka-0-10_2.11-2.3.2.jar,kafka-clients-1.1.0.jar,spark-sql-kafka-0-10_2.11-2.3.2.jar pyspark-shell'


spark = SparkSession.builder.master("local[*]").appName("Spark Kafka Consumer")\
     .getOrCreate()

dsraw = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "CustOrder")\
    .option("startingOffsets", "earliest")\
    .load()

ds = dsraw.selectExpr("CAST(value AS STRING) as value")

ds.printSchema()


customer_schema = StructType()\
    .add("custID", StringType())\
    .add("itemID", StringType())\
    .add("amount", StringType())

custJson = ds.select(from_json(col('value'), customer_schema).alias("orders"))

custJson.printSchema()

qstruct = custJson.select("orders.*")

qstruct.printSchema()

rawquery = qstruct.writeStream.queryName("qraw").format("memory").start()

for i in range(10):
    raw = spark.sql("select * from qraw")
    raw.show()
    sleep(5)

rawquery.awaitTermination()
