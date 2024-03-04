from titanic.reader.abstractReader import AbstractReader
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr


class KafkaReader(AbstractReader):
    required_params = ['bootstrap_servers', 'topic']

    def __init__(self, spark, **config):
        super().__init__(spark)
        self.validate_config(config)
        self.bootstrap_servers = config['bootstrap_servers']
        self.topic = config['topic']
        self.starting_offsets = config.get('starting_offsets', 'latest')
        self.ending_offsets = config.get('ending_offsets', 'latest')

    def validate_config(self, config):
        missing_params = [param for param in self.required_params if param not in config]
        if missing_params:
            raise ValueError(f"Missing required configuration parameters: {', '.join(missing_params)}")

    def read(self):
        df = self.spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.bootstrap_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", self.starting_offsets) \
            .option("endingOffsets", self.ending_offsets) \
            .load()

        return df

    def transform(self,df: DataFrame)-> DataFrame:
        return (df
                .transform(self._transform_header)
                .transform(self._transform_kafka_metadata)
                )


    def read_transform(self)-> DataFrame:
        return self.transform(self.read())


    @staticmethod
    def _transform_header(df: DataFrame) -> DataFrame:
        df_transformed = df.withColumn("table_suffix",expr("CAST(headers[0].value AS STRING)")).drop("headers")
        return df_transformed

    @staticmethod
    def _transform_value():
        pass

    @staticmethod
    def _transform_kafka_metadata(df:DataFrame)->DataFrame:
        return (df.withColumnRenamed("topic","kafka_topic")
        .withColumnRenamed("key", "kafka_key")
        .withColumnRenamed("partition", "kafka_partition")
        .withColumnRenamed("offset", "kafka_offset")
        .withColumnRenamed("timestamp", "kafka_ts")
        .withColumnRenamed("timestampType","kafka_ts_type"))


