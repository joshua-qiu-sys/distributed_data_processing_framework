from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from typing import Dict, Optional, Union, Any, Type
import threading
import time
from read_kafka_consumer_cfg import KafkaConsumerCfgReader
from src.data_producers.random_data_generator.serialisations import ObjectSerialisation
from src.data_producers.random_data_generator.schemas.consumer_good import ConsumerGood

class KafkaMsgConsumer:
    def __init__(self,
                 topic: str,
                 schema_subject_name: str,
                 consumer_props_cfg: Dict,
                 msg_val_dataclass: Type[Any],
                 poll_interval: int = 1,
                 commit_count: int = 10,
                 schema_ttl: int = 86400):
        
        self.topic = topic
        self.schema_subject_name = schema_subject_name
        self.consumer_props_cfg = consumer_props_cfg
        self.consumer_props_cfg.update({
            'on_commit': self._commit_callback
        })
        self.consumer = Consumer(self.consumer_props_cfg)

        self.poll_interval = poll_interval
        self.commit_count = commit_count
        self.schema_ttl = schema_ttl
        self.schema_last_fetched_timestamp = None
        self.schema_str = None
        self.schema_registry_client = None
        self._setup_schema_registry_client_conn()
        self._fetch_latest_schema()
        self.msg_val_dataclass = msg_val_dataclass

        self.msg_count = 0
        self.is_rebalancing = threading.Event()
        self.is_rebalancing.set()

    def _commit_callback(self, err, partitions):
        if err:
            print(f'ERROR: Commit failed: {err}')
        else:
            print(f'SUCCESS: Commit succeeded for partitions: {partitions}')

    def _on_assign_callback(self, consumer, partitions):
        print(f'Consumer group rebalance - partitions have been assigned to consumer: {partitions}')
        self.is_rebalancing.clear()

    def _on_revoke_callback(self, consumer, partitions):
        print(f'Consumer group rebalance - partitions have been revoked from consumer: {partitions}')
        self.is_rebalancing.set()

    def _on_lost_callback(self, consumer, partitions):
        print(f'Consumer group rebalance - partitions have been lost from consumer: {partitions}')
        self.is_rebalancing.set()

    def _setup_schema_registry_client_conn(self):
        schema_registry_client_conf = {
            'url': 'http://localhost:8081'
        }
        self.schema_registry_client = SchemaRegistryClient(conf=schema_registry_client_conf)

    def _fetch_latest_schema(self):
        schema_res = self.schema_registry_client.get_latest_version(subject_name=self.schema_subject_name)
        schema_str = schema_res.schema.schema_str
        self.schema_str = schema_str
        self.schema_last_fetched_timestamp = time.time()

    def consume(self):
        if self.poll_interval is None:
            raise ValueError(f'Consumer cannot poll for messages when poll interval is not set')

        self.consumer.subscribe([self.topic], on_assign=self._on_assign_callback, on_revoke=self._on_revoke_callback, on_lost=self._on_lost_callback)
        self.consume_messages()

    def consume_messages(self):

        curr_time = time.time()
        if self.schema_last_fetched_timestamp is None:
            self._fetch_latest_schema()
        elif curr_time - self.schema_last_fetched_timestamp > self.schema_ttl:
            self._fetch_latest_schema()

        try:
            while True:
                msg = self.consumer.poll(self.poll_interval)
                if msg is None:
                    if self.is_rebalancing.is_set():
                        print('Waiting for consumer group rebalancing to complete...')
                    else:
                        print('Waiting for messages to arrive...')
                elif msg.error():
                    print(f'ERROR: {msg.error}')
                else:
                    topic=msg.topic()
                    key=msg.key().decode('utf-8')
                    value=self.deserialise_msg(msg_value=msg.value())
                    print(f'Received event: {{"topic": {topic}, "key": {key}, "value": {value}}}')
                    print(f'Count: {self.msg_count}')

                    if self.msg_count % self.commit_count == 0:
                        self.consumer.commit(asynchronous=True)
                        print(f'Committed offsets asynchronously')
                        assigned_partitions = self.consumer.assignment()
                        curr_partition_offsets = self.consumer.position(assigned_partitions)
                        for tp in curr_partition_offsets:
                            print(f'Current offset position for topic {tp.topic} partition {tp.partition}: {tp.offset}')
                    self.msg_count += 1
        finally:
            self.consumer.close()
    
    def deserialise_msg(self, msg_value: Optional[Union[str, bytes]]):
        msg_val_from_dict_callable = ObjectSerialisation(obj_dataclass=self.msg_val_dataclass).get_obj_from_dict_callable()
        avro_deserialiser = AvroDeserializer(schema_registry_client=self.schema_registry_client, schema_str=self.schema_str, from_dict=msg_val_from_dict_callable)
        deserisalised_msg_value = avro_deserialiser(data=msg_value, ctx=SerializationContext(topic=self.topic, field=MessageField.VALUE))
        return deserisalised_msg_value

if __name__ == '__main__':
    consumer_cfg_reader = KafkaConsumerCfgReader()
    consumer_props_cfg = consumer_cfg_reader.read_consumer_props_cfg()
    kafka_consumer = KafkaMsgConsumer(topic='uncatg_landing_zone',
                                      schema_subject_name='ConsumerGood-value',
                                      consumer_props_cfg=consumer_props_cfg,
                                      msg_val_dataclass=ConsumerGood)
    kafka_consumer.consume()