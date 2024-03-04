# Ensure you have pyspark and kafka installed in your environment
from pyspark.sql import SparkSession, DataFrame
from abc import ABC, abstractmethod


class AbstractReader(ABC):
    """
    Abstract base class for a data reader.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def read(self) -> DataFrame:
        pass
