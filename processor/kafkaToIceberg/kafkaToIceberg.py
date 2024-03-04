import datetime
import json
from titanic.processor.utils import spark_to_sql_type_mapping
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType
from pyspark.errors.exceptions.captured import AnalysisException
from titanic.processor.AbstractProcessor import AbstractProcessor
from titanic.processor.functions import json_to_schema, merge_schemas, make_df_and_target_table_schema_compatible
from decorator import timed


class KafkaToIceberg(AbstractProcessor):
    def __init__(self, spark: SparkSession, schema: StructType = None, table_prefix: str = ""):
        super().__init__(spark, schema)
        self.table_prefix = table_prefix

    @staticmethod
    @timed("Splitting by Suffix took:")
    def _split_dataframe_by_table_suffix(df: DataFrame) -> list[tuple[str, DataFrame]]:
        distinct_table_suffixes = df.select("table_suffix").distinct().collect()

        return [(row.table_suffix, df.filter(col("table_suffix") == row.table_suffix)) for row in
                distinct_table_suffixes]

    @staticmethod
    @timed("Inferring Schema took:")
    def _dynamic_schema_inference(df: DataFrame) -> StructType:
        values = df.select("value")
        # transform df into rdds, map each col to the json.loads result
        rdd_json_dicts = values.rdd.map(lambda row: json_to_schema(json.loads(row.value)))
        # Reduce by merging schemas
        return rdd_json_dicts.reduce(lambda schema1, schema2: merge_schemas(schema1, schema2))

    @staticmethod
    def _explode_value(df: DataFrame, inferred_schema) -> DataFrame:
        # apply schema on value column, explode it, remove original value column
        # and table_suffix (we don't need it anymore)
        return (df
                .withColumn("value", from_json(col("value"), inferred_schema))
                .select("*", "value.*")
                .drop("value", "table_suffix"))

    @timed("Aligning Schema took:")
    def align_table_schema(self, df: DataFrame, table_suffix):
        try:
            # get table we want to write into
            table = df.sparkSession.table(f"spark_catalog.default.{self.table_prefix}{table_suffix}")
            # this method puts the cols in the right order (it appears that ths is mandatory,
            # but there might be an option to neglect this)
            # also, it checks which cols are new in this df compared to the existing table and adds them at the end
            df_writable, columns_needed_to_add = make_df_and_target_table_schema_compatible(df, table.schema)
            if len(columns_needed_to_add) > 0:
                self.update_table_schema(df_writable,columns_needed_to_add,table_suffix)
        except AnalysisException:
            print(f"The Table {self.table_prefix}{table_suffix} was not found in the catalog. Creating new one...")
            df_writable = self.create_table(df, table_suffix)
        return df_writable

    @timed("Updating table took:")
    def update_table_schema(self, df, columns_needed_to_add, table_suffix):
            print("Detected new Columns. Adding to table... ")
            # Format extra columns for SQL ALTER TABLE statement
            extra_columns_sql = [f"{extra_col} {spark_to_sql_type_mapping[type(df.schema[extra_col].dataType)]}" for
                                 extra_col in columns_needed_to_add]
            columns_to_add = ", ".join(extra_columns_sql)
            alter_table_sql = f"ALTER TABLE spark_catalog.default.{self.table_prefix}{table_suffix} ADD COLUMNS ({columns_to_add})"
            self.spark.sql(alter_table_sql)


    @timed("Creating table took:")
    def create_table(self, df: DataFrame, table_suffix) -> DataFrame:
        sql_statement = f"CREATE TABLE IF NOT exists {self.table_prefix}{table_suffix} ("
        # Process each field in the schema
        for field in df.schema.fields:
            field_name = field.name
            field_type = spark_to_sql_type_mapping[type(field.dataType)]
            sql_statement += f"{field_name} {field_type}, "
        # Remove the last comma and space, then close the parentheses
        sql_statement = sql_statement[:-2] + ")"
        # unfortunately, with spark 3.5 i cant use  "TBLPROPERTIES('write.spark.accept-any-schema'='true')" anymore,
        # see https://github.com/apache/iceberg/issues/9827
        sql_statement += ("USING iceberg "
                          "PARTITIONED BY (kafka_ts) ")
        # "TBLPROPERTIES('write.spark.accept-any-schema'='true')")
        print(sql_statement)
        self.spark.sql(sql_statement)
        return df

    @timed("Writing Table took:")
    def process_table(self, dataframe: DataFrame, suffix: str):
        # for a table suffix in a micro batch, infer schema and create a merged schema suited for all
        # rows of the table_suffix in micro batch
        # add a ingestion ts
        inferred_schema = KafkaToIceberg._dynamic_schema_inference(dataframe)
        df_transformed = KafkaToIceberg._explode_value(dataframe, inferred_schema)
        df_transformed= df_transformed.withColumn("ingested_to_iceberg",lit(datetime.datetime.now()))
        df_writable = self.align_table_schema(df_transformed, suffix)
        df_writable.createOrReplaceTempView("source_df")
        # df_writable.writeTo(f"spark_catalog.default.{suffix}").option("mergeSchema", "true").append() # this only works when aceppt-any-schema
        merge_sql = f"""
        MERGE INTO spark_catalog.default.{self.table_prefix}{suffix} AS target
        USING source_df AS source
        ON target.unique_id = source.unique_id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        df_writable.sparkSession.sql(merge_sql)

    def process(self, input_stream):
        @timed("Processing of microbatch took:")
        def process_micro_batch(df, epoch_id):
            # this should normally be in the value
            df = df.withColumn("value", col("value").cast("string"))
            # Splitting the DataFrame based on 'table_suffix'
            dfs = KafkaToIceberg._split_dataframe_by_table_suffix(df)
            for suffix, dataframe in dfs:
                # for each different table call write the data
                self.process_table(dataframe, suffix)

        query = input_stream.writeStream.foreachBatch(process_micro_batch).trigger(processingTime='1 minutes').start()
        query.awaitTermination()
