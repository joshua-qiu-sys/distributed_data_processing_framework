from typing import Dict
from src.data_producers.random_data_generator.schema_registry_connectors import AbstractSchemaRegistryClient

class SchemaHandler:
    def __init__(self, schema_registry_client: AbstractSchemaRegistryClient, schema_details: Dict[str, str]):
        self.schema_registry_client = schema_registry_client
        self.schema_details = schema_details

    def get_schema(self) -> str:
        schema = self.schema_registry_client.get_schema(schema_details=self.schema_details)
        return schema