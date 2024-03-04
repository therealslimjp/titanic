import os

from pyspark.sql import SparkSession
import time

from titanic.testsuite import testdata
from titanic.testsuite.testdata import getdata
from titanic.decorator import timed





@timed("Generation took:")
def generate_data():
    for i in range(0, 100):
        data =[]
        for j in range(0,100):
            data.extend(getdata())
        df = spark.createDataFrame(data, testdata.ingest_schema)
        df.write.mode("append").json("./filesink/")
        print(f"batch {i}")



@timed("Generation took:")
def generate_data_kafka():
    for i in range(0, 1):

        data = []
        for j in range(0, 1000):
            data.extend(getdata())
        df = spark.createDataFrame(data, testdata.ingest_schema)
        df_kafka = df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value","headers")
        kafka_bootstrap_servers = "localhost:9092"  # Update with your Kafka bootstrap servers
        target_topic = "mytopic"  # Specify your target topic
        df_kafka.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", target_topic) \
            .save()
        print(f"batch {i}")

def generate_update_data():
    for i in range(0, 100):
        data = []
        for j in range(0, 1000):
            data.extend(getdata())
        df = spark.createDataFrame(data, testdata.ingest_schema)
        df.write.mode("append").json("./filesink/")
        print(f"batch {i}")

# Assuming `getdata()` is your function that dynamically generates data
if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName("WriteToSink") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .master("local[*]").getOrCreate()
    update=False
    kafks=True
    if kafks:
        generate_data_kafka()
        exit(0)


    if not update:
        for file in os.listdir("filesink"):
            os.remove(os.getcwd()+"/filesink/"+file)
        time.sleep(3)
        generate_data()

    else:
        generate_update_data()

