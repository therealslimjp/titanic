import itertools
import json
import datetime
import random
import time
import uuid

from pyspark.sql.types import *

ingest_schema=StructType([
    StructField("key", BinaryType(), True),
    StructField("value", BinaryType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("timestampType", IntegerType(), True),
    StructField("headers", ArrayType(
        StructType([
            StructField("key", StringType(), True),
            StructField("value", BinaryType(), True),
        ])
    ), True)])


def getdata():
    # Original sample JSON data for reference, modified to directly include in the tuple creation
    data_template = [
        {"id": 1, "name": "Alice", "lastname":"bolek","gender":"male"},
        {"product": "Laptop", "seller": "Gruber", "quantity": 5, "returns": 2},
        {"sensorId": "sensor_01", "temperature": 22.5, "humidity": 45.2, "runtime":13},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
        {"product": "Smartphone", "seller": "töpfer", "quantity": 10, "returns": 2, "in_stock": True},
        {"sensorId": "sensor_02", "temperature": 18.3, "humidity": 50.0, "status": "active"},
        {"name": "Charlie"},
        {"product": "Tablet", "seller": "toepfer"},
        {"sensorId": "sensor_03", "humidity": 55.5}
    ]
    table_suffixes = ["user_info", "inventory", "sensor_data", "user_info2", "inventory2", "sensor_data2"]
    table_suffix_iterator = itertools.cycle(table_suffixes)

    additional_data = []

    for index, item in enumerate(data_template):
        # Add a unique integer (using UUID4 for uniqueness)
        item['unique_id'] = int(time.time_ns()/1000)
        item['ingested_to_component']=str(datetime.datetime.now())
        json_data = json.dumps(item).encode()
        table_suffix = next(table_suffix_iterator)

        additional_tuple = (
            bytearray(f"key{index + 1}", "utf-8"),
            bytearray(json_data),
            f"mytopic",
            0,  # Partition
            123456 + index,  # Simulated offset
            datetime.datetime.strptime("2021-01-0" + str(index % 9 + 1) + " 00:00:00", "%Y-%m-%d %H:%M:%S"),  # Date
            1,
            [("table_suffix", bytearray(table_suffix, "utf-8"))]  # Add the table_suffix as a header
        )
        additional_data.append(additional_tuple)

    return additional_data