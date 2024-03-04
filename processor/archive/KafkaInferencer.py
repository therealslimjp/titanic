import json
import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, DataType

from titanic.datalake.processor.functions import json_to_schema, merge_schemas


def sort_dataframe_to_match_table_schema(df, table_schema):
    # Get a list of column names in the order they appear in the table schema
    table_columns_ordered = [field.name for field in table_schema.fields]

    # Identify DataFrame columns that match the table schema
    matching_columns = [col for col in table_columns_ordered if col in df.columns]

    # Identify DataFrame columns not present in the table schema
    extra_columns = [col for col in df.columns if col not in table_columns_ordered]

    # Combine columns: first those matching the table schema, then any extra columns
    all_columns_ordered = matching_columns + extra_columns

    # Reorder DataFrame columns
    df_ordered = df.select(all_columns_ordered)

    return df_ordered


class KafkaInferencer:
    def __init__(self, schema: StructType = None):
        self.schema = schema
        self.use_distributed_schema_merging=True

    def _get_iceberg_table_schemas(self):
        # get current iceberg table schemas from metastore
        # with those,
        pass

    @staticmethod
    def _split_dataframe_by_table_suffix(df:DataFrame)-> list[tuple[str, DataFrame]]:
        distinct_table_suffixes = df.select("table_suffix").distinct().collect()

        return [(row.table_suffix,df.filter(col("table_suffix") == row.table_suffix)) for row in distinct_table_suffixes]



    def inference(self, df: DataFrame) -> list[DataFrame]:
        df = df.withColumn("value", col("value").cast("string"))
        df_list= KafkaInferencer._split_dataframe_by_table_suffix(df)[1]
        return_list=[]
        for df_filtered_by_suffix in df_list:
            schema= self._dynamic_schema_inference(df_filtered_by_suffix)
            #schema = StructType([StructField(field_name, data_type, True) for field_name, data_type in colnames_and_types])
            return_list.append(df_filtered_by_suffix.withColumn("value", from_json(col("value"),schema)).select("*", "value.*").drop("value"))
        return return_list

    def _dynamic_schema_inference(self, df:DataFrame)-> list[str,DataType]:
        values= df.select("value")
        if self.use_distributed_schema_merging:
                    # transform df into rdds, map each col to the json.loads result
            rdd_json_dicts = values.rdd.map(lambda row: json_to_schema(json.loads(row.value)))
        # Reduce by merging schemas
            return rdd_json_dicts.reduce(lambda schema1, schema2: merge_schemas(schema1, schema2))
        else:
            fields= values.rdd.flatMap(lambda row: json_to_schema(json.loads(row.value)))
            # remove_duplicates
            cols= fields.distinct().collect()
            return cols

    def process_stream(self, input_stream):
        a = time.time()

        def process_micro_batch(df, epoch_id):
            df = df.withColumn("value", col("value").cast("string"))
            # Splitting the DataFrame based on 'table_suffix'
            dfs = KafkaInferencer._split_dataframe_by_table_suffix(df)
            for suffix,dataframe in dfs:


                # Here, apply your schema inference logic to df_filtered
                # For simplicity, let's assume we have a function to infer schema and apply it
                inferred_schema = self._dynamic_schema_inference(dataframe)
                # Assuming inferred_schema is a StructType obtained from your schema inference logic

                df_transformed = dataframe\
                    .withColumn("value", from_json(col("value"), inferred_schema))\
                    .select("*","value.*")\
                    .drop("value","table_suffix")

                # make schemas compatible
                schema = df_transformed.sparkSession.table(f"spark_catalog.default.{suffix}").schema
                df_sorted = sort_dataframe_to_match_table_schema(df_transformed, schema)

                df_sorted.writeTo(f"spark_catalog.default.{suffix}").option("mergeSchema", "true").append()
            print(time.time() - a)



        # Apply the custom processing logic to each micro-batch
        query = input_stream.writeStream.foreachBatch(process_micro_batch).start()

        query.awaitTermination()

