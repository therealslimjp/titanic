import time

from pyspark.sql import SparkSession

from titanic.reader.kafka import KafkaReader
from titanic.processor.kafkaToIceberg.kafkaToIceberg import KafkaToIceberg
from titanic.testsuite import testdata

#
# def createTables(spark):
#
#     spark.sql("""CREATE TABLE IF NOT EXISTS spark_catalog.default.sensor_data (
#     kafka_key BINARY,
#     kafka_topic STRING,
#     kafka_partition INT,
#     kafka_offset LONG,
#     kafka_ts TIMESTAMP,
#     kafka_ts_type INT,
#     temperature DOUBLE,
#     sensorId STRING,
#     humidity DOUBLE,
#     status STRING
# ) USING iceberg
# PARTITIONED BY (kafka_ts)
# TBLPROPERTIES(  'write.spark.accept-any-schema'='true')
# """)
#     spark.sql("""CREATE TABLE IF NOT EXISTS spark_catalog.default.inventory (
#     kafka_key BINARY,
#     kafka_topic STRING,
#     kafka_partition INT,
#     kafka_offset LONG,
#     kafka_ts TIMESTAMP,
#     kafka_ts_type INT,
#     quantity INT,
#     product STRING,
#     in_stock BOOLEAN
# ) USING iceberg
# PARTITIONED BY (kafka_ts)
# TBLPROPERTIES(  'write.spark.accept-any-schema'='true')
# """)
#     spark.sql("""CREATE TABLE IF NOT EXISTS spark_catalog.default.user_info (
#     kafka_key BINARY,
#     kafka_topic STRING,
#     kafka_partition INT,
#     kafka_offset LONG,
#     kafka_ts TIMESTAMP,
#     kafka_ts_type INT,
#     id INT,
#     name STRING,
#     email STRING
# ) USING iceberg
# PARTITIONED BY (kafka_ts)
# TBLPROPERTIES(  'write.spark.accept-any-schema'='true')
#
# """)




if __name__ == '__main__':

    a= time.time()
    # Initialize SparkSession (In actual use, this should be initialized once per application)

    # Initialize a SparkSession with Iceberg support
    spark = SparkSession.builder \
        .appName("KafkaToIceberg") \
        .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1')\
        .master("local[*]")\
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse", "/Users/work/PycharmProjects/workbench/datalake/testsuite/icebergsink/") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # create table
    #createTables(spark)

    print(f"Init time {time.time()-a}")
    # Create a streaming DataFrame that reads from the queue
    k= KafkaReader(spark,bootstrap_servers="dsf",topic="dfdsf")
    file= False
    if file:
        df = spark \
            .readStream \
            .schema(testdata.ingest_schema).load("/Users/work/PycharmProjects/workbench/datalake/testsuite/filesink/", format="json")
    else:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("startingOffsets", "latest") \
            .option("subscribe", "mytopic") \
            .option("includeHeaders", True) \
            .load()

    streamingDF= k.transform(df)
    k2= KafkaToIceberg(spark)
    k2.process(streamingDF)


