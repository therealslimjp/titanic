
import os

from pyspark.sql import SparkSession
"""for console use: 
spark-sql \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hadoop \
    --conf spark.sql.catalog.spark_catalog.warehouse="/Users/work/PycharmProjects/workbench/datalake/testsuite/icebergsink/"
"""
if __name__ == '__main__':

    spark = SparkSession.builder \
        .appName("KafkaToIceberg") \
        .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3') \
        .master("local") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
        .config("spark.sql.catalog.spark_catalog.warehouse",
                "/Users/work/PycharmProjects/workbench/datalake/testsuite/icebergsink/") \
        .getOrCreate()

    spark.sql("show tblproperties default.inventory").show(truncate=False)
    spark.sql("Select count(*) from inventory where returns is not NULL limit 10").show()
