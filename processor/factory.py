from titanic.processor.AbstractProcessor import AbstractProcessor
from titanic.processor.kafkaToIceberg.kafkaToIceberg import KafkaToIceberg


class Factory:
    _processors = {}

    @classmethod
    def register_processor(cls, name, processor_class):
        if not issubclass(processor_class, AbstractProcessor):
            raise TypeError(f"Class {processor_class.__name__} must implement AbstractProcessor")
        cls._processors[name] = processor_class

    @classmethod
    def get_processor(cls, name, spark, **config):
        processor_class = cls._processors.get(name)
        if not processor_class:
            raise ValueError(f"processor not found: {name}")
        processor_instance = processor_class(spark, **config)
        if not isinstance(processor_instance, AbstractProcessor):
            raise TypeError(f"The instance must be a Implementation of AbstractProcessor, found {type(processor_instance)}")
        return processor_instance


Factory.register_processor('KafkaProcessor', KafkaToIceberg)
