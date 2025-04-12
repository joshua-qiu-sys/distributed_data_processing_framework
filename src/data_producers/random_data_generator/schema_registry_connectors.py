from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from abc import ABC, abstractmethod
from typing import Dict

class AbstractSchemaRegistryClient(ABC):
    @abstractmethod
    def get_schema():
        raise NotImplementedError

class ConfluentKafkaSchemaRegistryClient(AbstractSchemaRegistryClient):
    def __init__(self, schema_registry_client_conf: Dict[str, str]):
        conf = schema_registry_client_conf['conf']
        self.client = SchemaRegistryClient(conf=conf)

    def get_schema(self, schema_props: Dict[str, str]):
        subject_name = schema_props['subject_name']
        fmt = schema_props['fmt'] if 'fmt' in schema_props.keys() else None
        self.get_schema_from_confluent_kafka(subject_name=subject_name, fmt=fmt)

    def get_schema_from_confluent_kafka(self, subject_name: str, fmt: str = None) -> str:
        schema = self.client.get_latest_version(subject_name=subject_name, fmt=fmt)
        schema_str = schema.schema.schema_str
        return schema_str

class AbstractSchemaRegistryConnector(ABC):
    def __init__(self):
        self.schema_registry_client = None
    
    def get_schema_registry_client(self) -> str:
        return self.schema_registry_client
    
    def _set_schema_registry_client(self, schema_registry_client: AbstractSchemaRegistryClient) -> None:
        self.schema_registry_client = schema_registry_client

    @abstractmethod
    def get_schema_registry_client():
        raise NotImplementedError
    
class ConfluentKafkaSchemaRegistryConnector(AbstractSchemaRegistryConnector):
    def __init__(self):
        super().__init__()

    def get_schema_registry_client(self, schema_registry_client_conf: Dict[str, str]) -> ConfluentKafkaSchemaRegistryClient:
        schema_registry_client = ConfluentKafkaSchemaRegistryClient(schema_registry_client_conf=schema_registry_client_conf)
        super()._set_schema_registry_client(schema_registry_client=schema_registry_client)
        return schema_registry_client