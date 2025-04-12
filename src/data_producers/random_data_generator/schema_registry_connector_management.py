from typing import Dict
from data_producers.random_data_generator.schema_registry_connectors import AbstractSchemaRegistryConnector, ConfluentKafkaSchemaRegistryConnector
from src.utils.constructors import AbstractFactory, AbstractFactoryRegistry

ACCEPTED_SCHEMA_REGISTRIES = {
    'confluent_kafka': ConfluentKafkaSchemaRegistryConnector
}
    
class SchemaRegistryConnectorFactoryRegistry(AbstractFactoryRegistry):
    def __init__(self):
        super().__init__()

    def lookup_registry(self, schema_registry_type: str) -> AbstractSchemaRegistryConnector:
        if not self.is_registered(schema_registry_type=schema_registry_type):
            return KeyError(f'Schema registry type "{schema_registry_type}" not found in factory registry')
        schema_registry = self._registry[schema_registry_type]
        return schema_registry

    def is_registered(self, schema_registry_type: str) -> bool:
        return schema_registry_type in self._registry.keys()

    def register(self, schema_registry_type: str, schema_registry_connector: AbstractSchemaRegistryConnector) -> None:
        if not self.is_registered(schema_registry_type=schema_registry_type):
            self._registry[schema_registry_type] = schema_registry_connector

    def register_defaults(self, default_schema_registry_dict: Dict[str, AbstractSchemaRegistryConnector]) -> None:
        for schema_registry_type, schema_registry_connector in default_schema_registry_dict.items():
            self.register(schema_registry_type=schema_registry_type, schema_registry_connector=schema_registry_connector)

    def deregister(self, schema_registry_type: str) -> None:
        if not self.is_registered(schema_registry_type=schema_registry_type):
            return KeyError(f'Schema registry type "{schema_registry_type}" not found in factory registry')
        self._registry.pop(schema_registry_type)
        
    def reset_registry(self):
        self._registry.clear()

class SchemaRegistryConnectorFactory(AbstractFactory):
    def __init__(self, factory_registry: SchemaRegistryConnectorFactoryRegistry):
        super().__init__(factory_registry)

    def create(self, schema_registry_type: str) -> AbstractSchemaRegistryConnector:
        if not self.factory_registry.is_registered(schema_registry_type=schema_registry_type):
            raise KeyError(f'Schema registry type "{schema_registry_type}" not found in factory registry')
        schema_registry_connector_cls = self.factory_registry.lookup_registry(schema_registry_type=schema_registry_type)
        schema_registry_connector = schema_registry_connector_cls(schema_registry_type=schema_registry_type)
        return schema_registry_connector