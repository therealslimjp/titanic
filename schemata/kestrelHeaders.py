from pyspark.sql.types import StructType, StructField, StringType

# not yet in use. Might be smart to collect schemata here to use them for deserialization
schema = StructType([
    StructField("id", StringType(), True),
    StructField("value", StringType(), True)
])
