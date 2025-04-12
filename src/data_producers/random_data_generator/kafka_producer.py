from confluent_kafka import Producer, KafkaException
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import BaseSerializer, AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import Serializer, StringSerializer, StringDeserializer, SerializationContext, MessageField
from abc import ABC, abstractmethod
from typing import Dict, Union
from random import choice
import time
from decimal import Decimal
import logging
from read_kafka_producer_cfg import KafkaProducerCfgReader
from schema_registry_connector_management import SchemaRegistryConnectorFactory
from schemas.consumer_good import ConsumerGood

logger = logging.getLogger(f'random_data_generator')

class KafkaMsgSerialisation:
    def __init__(self,
                 key_serialiser: Union[Serializer, BaseSerializer],
                 key_deserialiser: Union[Serializer, BaseSerializer],
                 val_serialiser: Union[Serializer, BaseSerializer],
                 val_deserialiser: Union[Serializer, BaseSerializer],):
        
        self.key_serialiser = key_serialiser
        self.key_deserialiser = key_deserialiser
        self.val_serialiser = val_serialiser
        self.val_deserialiser = val_deserialiser

    def get_key_serialiser(self) -> Union[Serializer, BaseSerializer]:
        return self.key_serialiser
    
    def get_key_deserialiser(self) -> Union[Serializer, BaseSerializer]:
        return self.key_deserialiser
    
    def get_val_serialiser(self) -> Union[Serializer, BaseSerializer]:
        return self.val_serialiser
    
    def get_val_deserialiser(self) -> Union[Serializer, BaseSerializer]:
        return self.val_deserialiser

class KafkaMsgProducer(Producer):
    def __init__(self,
                 producer_props: Dict[str, Union[str, int]],
                 msg_serialisation: KafkaMsgSerialisation,
                 schema_registry_connector_factory: SchemaRegistryConnectorFactory):
        
        self.producer = Producer(producer_props)
        self.producer_props = producer_props
        self.msg_serialisation = msg_serialisation
        self.schema_registry_connector_factory = schema_registry_connector_factory

        curr_time = time.time()
        self.last_poll_time = curr_time
        self.last_flush_time = curr_time

        self.kafka_schema_handler = None
        self.kafka_msg_serialisation = None

    def _delivery_callback(self):
        pass

    def _message_val_from_dict(self):
        pass

    def _message_val_to_dict(self):
        pass

    def produce_message(self):
        pass

class KafkaTransactionalMsgProducer(KafkaMsgProducer):
    def __init__(self, producer_props: Dict[str, Union[str, int]]):
        super().__init__(producer_props)
        self._init_transactional_mode()

    def _init_transactional_mode(self) -> None:
        self.producer.init_transactions()

    def _delivery_callback(self):
        pass

    def _message_val_from_dict(self):
        pass

    def _message_val_to_dict(self):
        pass

    def produce_message(self):
        pass

def produce_message():

    producer_cfg_reader = KafkaProducerCfgReader()
    producer_props_cfg = producer_cfg_reader.read_producer_props_cfg()
    print(f'Producer properties: {producer_props_cfg}')

    producer = Producer(producer_props_cfg)
    producer.init_transactions()
    print(f'Initiated producer transactions')

    def consumer_good_to_dict(consumer_good: ConsumerGood, ctx: SerializationContext = None) -> Dict:
        consumer_good_dict = {
            'item': consumer_good.item,
            'retailer': consumer_good.retailer,
            'price': consumer_good.price
        }
        return consumer_good_dict

    schema_registry_conf = {
        'url': 'http://localhost:8081'
    }
    schema_registry = SchemaRegistryClient(schema_registry_conf)
    consumer_good_schema = schema_registry.get_latest_version(subject_name='ConsumerGood-value')
    consumer_good_schema_str = consumer_good_schema.schema.schema_str
    avro_serialiser = AvroSerializer(schema_registry_client=schema_registry, schema_str=consumer_good_schema_str, to_dict=consumer_good_to_dict)
    string_serialiser = StringSerializer('utf8')

    def delivery_callback(err, msg):
        def consumer_good_from_dict(consumer_good_dict: Dict, ctx: SerializationContext = None) -> ConsumerGood:
            consumer_good = ConsumerGood(item=consumer_good_dict['price'],
                                         retailer=consumer_good_dict['retailer'],
                                         price=consumer_good_dict['price'])
            return consumer_good
        
        if err:
            print(f'ERROR: Message delivery failed: {err}')
        else:
            topic = msg.topic()
            
            string_deserialiser = StringDeserializer('utf8')
            key = string_deserialiser(msg.key())
            avro_deserialiser = AvroDeserializer(schema_registry_client=schema_registry, schema_str=consumer_good_schema_str, from_dict=consumer_good_from_dict)
            value = avro_deserialiser(msg.value(), SerializationContext(topic, MessageField.VALUE))
            print(f'SUCCESS: Message delivery succeeded: {{"topic": {topic}, "key": {key}, "value": {consumer_good_to_dict(value)}}}')

    topic = 'uncatg_landing_zone'
    products = ['computer', 'television', 'smartphone', 'book', 'clothing', 'alarm clock', 'batteries', 'headphones',
                'toothpaste', 'shampoo', 'laundry detergent', 'paper towels', 'charger', 'sunscreen', 'instant noodles',
                'packaged snacks', 'light bulb', 'bottled water', 'air freshener', 'cooking oil', 'canned soup']
    retailers = ['Woolworths', 'Coles', 'Aldi', 'IGA', 'Amazon', 'Costco']
    prices = [Decimal('5.00'), Decimal('10.00'), Decimal('4.50'), Decimal('9.99'), Decimal('9.83'), Decimal('25.60'),
              Decimal('35.40'), Decimal('23.87'), Decimal('15.00'), Decimal('12.30'), Decimal('2.01'), Decimal('7.45'),
              Decimal('5.07'), Decimal('3.88'), Decimal('75.35'), Decimal('11.00'), Decimal('3.00'), Decimal('1.00'),
              Decimal('9.90'), Decimal('78.39'), Decimal('2.00')]

    poll_interval = 3
    # flush_interval = 30
    curr_time = time.time()
    last_poll_time = curr_time
    # last_flush_time = curr_time

    count = 0
    while True:
        try:
            producer.begin_transaction()

            key = str(products.index(choice(products)))
            consumer_good_item = choice(products)
            consumer_good_retailer = choice(retailers)
            consumer_good_price = choice(prices)
            consumer_good = ConsumerGood(item=consumer_good_item, retailer=consumer_good_retailer, price=consumer_good_price)

            producer.produce(topic=topic,
                             key=string_serialiser(key),
                             value=avro_serialiser(consumer_good, SerializationContext(topic, MessageField.VALUE)),
                             callback=delivery_callback)
            print(f'Sent data to buffer: {{"topic": {topic}, "key": {key}, "value": {consumer_good_to_dict(consumer_good)}}}')
            
            producer.commit_transaction()
            print("Transaction committed successfully")
            print(f'Count: {count}')
            count += 1
        except BufferError:
            print(f'Buffer is full. Pausing for 2 seconds to allow messages to be sent from buffer before resuming.')
            time.sleep(2)
        except KafkaException as e:
            print(f'Kafka error occurred: {str(e)}')

        curr_time = time.time()
        if curr_time >= last_poll_time + poll_interval:
            print('Producer is polling. Handling delivery callback responses from brokers...')
            producer.poll(poll_interval)
            last_poll_time = curr_time
        # if curr_time >= last_flush_time + flush_interval:
        #     print('Producer is flushing records to brokers. Blocking current thread until completion...')
        #     producer.flush()
        #     last_flush_time = curr_time

if __name__ == '__main__':
    produce_message()