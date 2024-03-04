from titanic.reader.abstractReader import AbstractReader
from titanic.reader.kafka import KafkaReader


class Factory:
    _readers = {}

    @classmethod
    def register_reader(cls, name, reader_class):
        if not issubclass(reader_class, AbstractReader):
            raise TypeError(f"Class {reader_class.__name__} must implement ReaderInterface")
        cls._readers[name] = reader_class

    @classmethod
    def get_reader(cls, name, spark, **config):
        reader_class = cls._readers.get(name)
        if not reader_class:
            raise ValueError(f"Reader not found: {name}")
        reader_instance = reader_class(spark, **config)
        if not isinstance(reader_instance, AbstractReader):
            raise TypeError(f"The instance must be a Implementation of ReaderInterface, found {type(reader_instance)}")
        return reader_instance


Factory.register_reader('KafkaReader', KafkaReader)
