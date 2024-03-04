import yaml
from pyspark.sql import SparkSession,DataFrame
from titanic.reader.factory import Factory as ReaderFactory
from titanic.processor.factory import Factory as ProcessorFactory


class Application:
    def __init__(self, config_path):
        self.config_path = config_path
        self.config = self.load_config()
        self.spark = self.start_spark_session()

    def load_config(self):
        with open(self.config_path, 'r') as file:
            return yaml.safe_load(file)

    def start_spark_session(self):
        spark_config = self.config['spark']
        spark = SparkSession.builder \
            .appName(spark_config['app_name']) \
            .master(spark_config['master']) \
            .getOrCreate()
        return spark

    def get_module(self, module_type):
        if module_type == 'reader':
            reader_entry = self.config['modules'][module_type]
            reader = ReaderFactory.get_reader(reader_entry["class"],self.spark, **reader_entry['config'])
            return reader
        if module_type == 'processor':
            processor_entry = self.config['modules'][module_type]
            processor= ProcessorFactory.get_processor(processor_entry["class"],self.spark, **processor_entry['config'])
            return processor

    def run(self):
        reader = self.get_module('reader')
        processor = self.get_module('processor')
        df:DataFrame= reader.read()
        processor.process(df)


kafka_to_iceberg_app = Application("config.yaml")
kafka_to_iceberg_app.run()


