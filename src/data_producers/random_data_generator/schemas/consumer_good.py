from dataclasses import dataclass
from dataclasses_avroschema import AvroModel, types
from typing import Type
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient, Schema

@dataclass
class ConsumerGood(AvroModel):
    item: str
    retailer: str
    price: types.condecimal(max_digits=10, decimal_places=2)

    class Meta:
        namespace = "consumer_good.avro"

def get_avro_schema(dataclass: Type[AvroModel]) -> str:
    return dataclass.avro_schema()

def publish_to_schema_registry(subject_name: str, schema_str: str):

    schema_registry_conf = {
        'url': 'http://localhost:8081'
    }

    schema_registry = SchemaRegistryClient(schema_registry_conf)
    avro_schema = Schema(schema_str=schema_str, schema_type='AVRO')
    schema_id = schema_registry.register_schema(subject_name=subject_name, schema=avro_schema)
    print(f'Successfully registered schema under schema_id {schema_id} and subject {subject_name} in schema registry:\n{schema_str}')

if __name__ == '__main__':
    consumer_good_avro_schema = get_avro_schema(dataclass=ConsumerGood)
    print(f'Generated schema: {consumer_good_avro_schema}')
    publish_to_schema_registry(subject_name='ConsumerGood-value', schema_str=consumer_good_avro_schema)