# Ensure you have pyspark and kafka installed in your environment
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from abc import ABC, abstractmethod


class AbstractProcessor(ABC):
    """
    Abstract base class for a data reader.
    """
    def __init__(self,spark: SparkSession, schema: StructType = None):
        self.spark=spark
        self.schema=schema

    @abstractmethod
    def process(self,df) -> None:
        pass


