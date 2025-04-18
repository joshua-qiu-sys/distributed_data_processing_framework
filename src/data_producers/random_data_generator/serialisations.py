from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer, StringDeserializer, SerializationContext
from typing import Dict, Any, Type, Callable
from dataclasses import fields, is_dataclass
from abc import ABC, abstractmethod
from typing import Dict, Any, Callable

class AbstractSerialisation(ABC):
    @abstractmethod
    def serialise():
        raise NotImplementedError
    
    @abstractmethod
    def deserialise():
        raise NotImplementedError

class AbstractConfluentKafkaSerialisation(AbstractSerialisation):
    pass

class ConfluentKafkaStringSerialisation(AbstractConfluentKafkaSerialisation):
    def __init__(self):
        self.serialiser = None
        self.deserialiser = None

    def _setup_serialiser(self, serialiser_cfg: Dict = None) -> StringSerializer:
        string_serialiser = StringSerializer(**serialiser_cfg)
        self.serialiser = string_serialiser
        return string_serialiser
    
    def _setup_deserialiser(self, deserialiser_cfg: Dict = None) -> StringDeserializer:
        string_deserialiser = StringDeserializer(**deserialiser_cfg)
        self.deserialiser = string_deserialiser
        return string_deserialiser
    
    def serialise(self, msg_obj: Any, serialiser_cfg: Dict = None, serialisation_cfg: Dict = None) -> bytes:
        if self.serialiser is None:
            self._setup_serialiser(serialiser_cfg=serialiser_cfg)
        serialised_obj = self.serialiser(msg_obj)
        return serialised_obj
    
    def deserialise(self, bytes_obj: bytes, deserialiser_cfg: Dict = None, deserialisation_cfg: Dict = None) -> Any:
        if self.deserialiser is None:
            self._setup_deserialiser(deserialiser_cfg=deserialiser_cfg)
        deserialised_obj = self.deserialiser(bytes_obj)
        return deserialised_obj

class ConfluentKafkaAvroSerialisation(AbstractConfluentKafkaSerialisation):
    def __init__(self):
        self.serialiser = None
        self.deserialiser = None

    def _setup_serialiser(self, serialiser_cfg: Dict) -> AvroSerializer:
        avro_serialiser = self._setup_confluent_kafka_serialiser(**serialiser_cfg)
        self.serialiser = avro_serialiser
        return avro_serialiser
        
    def _setup_deserialiser(self, deserialiser_cfg: Dict) -> AvroDeserializer:
        avro_deserialiser = self._setup_confluent_kafka_deserialiser(**deserialiser_cfg)
        self.deserialiser = avro_deserialiser
        return avro_deserialiser

    def _setup_confluent_kafka_serialiser(self, schema_registry_client: SchemaRegistryClient, schema_str: str, to_dict: Callable) -> AvroSerializer:
        avro_serialiser = AvroSerializer(schema_registry_client=schema_registry_client, schema_str=schema_str, to_dict=to_dict)
        return avro_serialiser
    
    def _setup_confluent_kafka_deserialiser(self, schema_registry_client: SchemaRegistryClient, schema_str: str, from_dict: Callable) -> AvroDeserializer:
        avro_deserialiser = AvroDeserializer(schema_registry_client=schema_registry_client, schema_str=schema_str, from_dict=from_dict)
        return avro_deserialiser
    
    def serialise(self, msg_obj: Any, serialiser_cfg: Dict = None, serialisation_cfg: Dict = None) -> bytes:
        if self.serialiser is None:
            self._setup_serialiser(serialiser_cfg=serialiser_cfg)
        serialised_obj = self.serialiser(msg_obj, **serialisation_cfg)
        return serialised_obj
    
    def deserialise(self, bytes_obj: bytes, deserialiser_cfg: Dict = None, deserialisation_cfg: Dict = None) -> Any:
        if self.deserialiser is None:
            self._setup_deserialiser(deserialiser_cfg=deserialiser_cfg)
        deserialised_obj = self.deserialiser(bytes_obj, **deserialisation_cfg)
        return deserialised_obj
    
class KafkaMsgConverter:
    def __init__(self, msg_dataclass: Type[Any]):
        if not is_dataclass(msg_dataclass):
            raise TypeError("Message class must be a dataclass type")
        self.msg_dataclass = msg_dataclass

    def get_kafka_msg_to_dict_callable(self) -> Callable:
        return self.kafka_msg_to_dict
    
    def get_kafka_msg_from_dict_callable(self) -> Callable:
        return self.kafka_msg_from_dict
    
    def kafka_msg_to_dict(self, msg_obj: Any, ctx: SerializationContext = None) -> Dict:
        if not is_dataclass(msg_obj):
            raise TypeError("Message object must be a dataclass type")
        msg_dict = {}
        for attribute in fields(msg_obj):
            msg_dict[attribute.name] = getattr(msg_obj, attribute.name)
        return msg_dict
    
    def kafka_msg_from_dict(self, msg_dict: Dict, ctx: SerializationContext = None) -> Any:
        msg_obj = self.msg_dataclass(**msg_dict)
        return msg_obj