from pyspark.sql.types import BinaryType, StringType, IntegerType, LongType, TimestampType, DoubleType, BooleanType

spark_to_sql_type_mapping = {
    BinaryType: "BINARY",
    StringType: "STRING",
    IntegerType: "INT",
    LongType: "BIGINT",
    TimestampType: "TIMESTAMP",
    DoubleType: "DOUBLE",
    BooleanType: "BOOLEAN"
}