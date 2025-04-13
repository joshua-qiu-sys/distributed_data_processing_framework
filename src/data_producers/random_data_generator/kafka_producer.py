from confluent_kafka import Producer, Message, KafkaError, KafkaException
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer, SerializationContext, MessageField
from abc import ABC, abstractmethod, classmethod
from dataclasses import fields, is_dataclass
from typing import Dict, Union, Any, Type, Callable
from random import choice
import time
from decimal import Decimal
import logging
from read_kafka_producer_cfg import KafkaProducerCfgReader
from schema_registry_connectors import AbstractSchemaRegistryClient, ConfluentKafkaSchemaRegistryConnector
from schemas.consumer_good import ConsumerGood
from src.utils.cfg_management import AbstractCfgManager
from src.utils.constructors import AbstractFactory, AbstractFactoryRegistry

logger = logging.getLogger(f'random_data_generator')

class AbstractSerialisation(ABC):
    @abstractmethod
    def get_serialiser():
        raise NotImplementedError
    
    @abstractmethod
    def get_deserialiser():
        raise NotImplementedError

class ConfluentKafkaSerialisation(AbstractSerialisation):
    pass

class ConfluentKafkaStringSerialisation(ConfluentKafkaSerialisation):
    def __init__(self):
        pass

    def get_serialiser(self, serialisation_props: Dict = None) -> StringSerializer:
        string_serialiser = StringSerializer('utf8')
        return string_serialiser
    
    def get_deserialiser(self, deserialisation_props: Dict = None) -> StringDeserializer:
        string_deserialiser = StringDeserializer('utf8')
        return string_deserialiser

class ConfluentKafkaAvroSerialisation(ConfluentKafkaSerialisation):
    def __init__(self):
        pass

    def get_serialiser(self, serialisation_props: Dict) -> AvroSerializer:
        return self.get_confluent_kafka_serialiser(**serialisation_props)
        
    def get_deserialiser(self, deserialisation_props: Dict) -> AvroDeserializer:
        return self.get_confluent_kafka_deserialiser(**deserialisation_props)

    def get_confluent_kafka_serialiser(self, schema_registry_client: SchemaRegistryClient, schema_str: str, to_dict: Callable) -> AvroSerializer:
        avro_serialiser = AvroSerializer(schema_registry_client=schema_registry_client, schema_str=schema_str, to_dict=to_dict)
        return avro_serialiser
    
    def get_confluent_kafka_deserialiser(self, schema_registry_client: SchemaRegistryClient, schema_str: str, from_dict: Callable) -> AvroDeserializer:
        avro_deserialiser = AvroDeserializer(schema_registry_client=schema_registry_client, schema_str=schema_str, from_dict=from_dict)
        return avro_deserialiser
    
class SerialisationFactoryRegistry(AbstractFactoryRegistry):
    def __init__(self):
        super().__init__()

    def lookup_registry(self, serialisation_library: str, serialisation_type: str) -> AbstractSerialisation:
        if not self.is_registered(serialisation_library=serialisation_library, serialisation_type=serialisation_type):
            return KeyError(f'Serialisation type "{serialisation_type}" of serialisation library {serialisation_library} not found in factory registry')
        serialisation = self._registry[serialisation_library][serialisation_type]
        return serialisation

    def is_registered(self, serialisation_library: str, serialisation_type: str) -> bool:
        if serialisation_library in self._registry.keys():
            if serialisation_type in self._registry.keys()[serialisation_library]:
                return True
            else:
                return False
        else:
            return False

    def register(self, serialisation_library: str, serialisation_type: str, serialisation: AbstractSerialisation) -> None:
        if not self.is_registered(serialisation_library=serialisation_library, serialisation_type=serialisation_type):
            self._registry[serialisation_library][serialisation_type] = serialisation

    def register_defaults(self, default_serialisation_dict: Dict) -> None:
        for serialisation_library, serialisation_details in default_serialisation_dict.items():
            serialisation_type = serialisation_details[serialisation_type]
            serialisation = serialisation_type[serialisation]
            self.register(serialisation_library=serialisation_library, serialisation_type=serialisation_type, serialisation=serialisation)

    def deregister(self, serialisation_library: str, serialisation_type: str) -> None:
        if not self.is_registered(serialisation_library=serialisation_library, serialisation_type=serialisation_type):
            return KeyError(f'Serialisation type "{serialisation_type}" of serialisation library {serialisation_library} not found in factory registry')
        self._registry[serialisation_library].pop(serialisation_type)
        
    def reset_registry(self):
        self._registry.clear()
    
class SerialisationFactory(AbstractFactory):
    def __init__(self, factory_registry: SerialisationFactoryRegistry):
        super().__init__(factory_registry)

    def create(self, serialisation_library: str, serialisation_type: str) -> AbstractSerialisation:
        if not self.factory_registry.is_registered(serialisation_library=serialisation_library, serialisation_type=serialisation_type):
            raise KeyError(f'Serialisation type "{serialisation_type}" of serialisation library {serialisation_library} not found in factory registry')
        serialisation_cls = self.factory_registry.lookup_registry(serialisation_library=serialisation_library, serialisation_type=serialisation_type)
        serialisation = serialisation_cls(serialisation_library=serialisation_library, serialisation_type=serialisation_type)
        return serialisation
    
class KafkaMsgSerialisationCfgManager(AbstractCfgManager):
    def __init__(self, key_serialisation_props: Dict, val_serialisation_props: Dict):
        self.key_serialisation_props = key_serialisation_props
        self.val_serialisation_props = val_serialisation_props

    def get_key_serialisation_props(self) -> Dict:
        return self.key_serialisation_props
    
    def get_val_serialisation_props(self) -> Dict:
        return self.val_serialisation_props

class KafkaMsgSerialisation:
    def __init__(self,
                 key_serialisation: AbstractSerialisation,
                 val_serialisation: AbstractSerialisation):
        
        self.key_serialisation = key_serialisation
        self.val_serialisation = val_serialisation

    def get_key_serialisation(self) -> AbstractSerialisation:
        return self.key_serialisation
    
    def get_val_serialisation(self) -> AbstractSerialisation:
        return self.val_serialisation

class KafkaMsgProducer(Producer):
    def __init__(self,
                 producer_props: Dict[str, Union[str, int]],
                 msg_serialisation_cfg_manager: KafkaMsgSerialisationCfgManager,
                 serialisation_factory: SerialisationFactory,
                 schema_registry_client: AbstractSchemaRegistryClient,
                 schema_details: Dict[str, str]):
        
        self.producer = Producer(producer_props)
        self.producer_props = producer_props
        self.msg_serialisation_cfg_manager = msg_serialisation_cfg_manager
        serialisation_factory = serialisation_factory
        self.schema_registry_client = schema_registry_client
        self.schema_details = schema_details

        curr_time = time.time()
        self.last_poll_time = curr_time
        self.last_flush_time = curr_time

    def _delivery_callback(self, err: KafkaError, msg: Message):
        if err:
            print(f'ERROR: Message delivery failed: {err}')
        else:
            topic = msg.topic()
            key_serialisation_props = self.msg_serialisation_cfg_manager.get_key_serialisation_props()
            key_deserialiser = self.kafka_msg_serialisation.get_key_serialisation().get_deserialiser(deserialisaton_props=key_serialisation_props)
            key = key_deserialiser(msg.key())
            val_serialisation_props = self.msg_serialisation_cfg_manager.get_val_serialisation_props()
            val_deserialiser = self.kafka_msg_serialisation.get_val_serialisation().get_deserialiser(deserialisation_props=val_serialisation_props)
            val = val_deserialiser(msg.value(), SerializationContext(topic, MessageField.VALUE))
            print(f'SUCCESS: Message delivery succeeded: {{"topic": {topic}, "key": {key}, "value": {self.__class__._message_val_to_dict(message_val_obj=val)}}}')
    
    @classmethod
    def _message_val_from_dict(cls, message_val_dict: Dict, message_val_dataclass: Type[Any], ctx: SerializationContext = None) -> Any:
        if not is_dataclass(message_val_dataclass):
            raise TypeError("Message value class must be a dataclass type")
        message_val_obj = message_val_dataclass(**message_val_dict)
        return message_val_obj
    
    @classmethod
    def _message_val_to_dict(cls, message_val_obj: Any, ctx: SerializationContext = None) -> Dict:
        if not is_dataclass(message_val_obj):
            raise TypeError("Message value object must be a dataclass type")
        message_val_dict = {}
        for attribute in fields(message_val_obj):
            message_val_dict[attribute] = getattr(message_val_obj, attribute)
        return message_val_dict

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

    def delivery_callback(cls, err: KafkaError, msg: Message):
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
    # produce_message()

    producer_cfg_reader = KafkaProducerCfgReader()
    producer_props_cfg = producer_cfg_reader.read_producer_props_cfg()
    print(f'Producer properties: {producer_props_cfg}')

    ACCEPTED_SERIALISATIONS = {
        'confluent_kafka': {
            'string': ConfluentKafkaStringSerialisation,
            'avro': ConfluentKafkaAvroSerialisation
        }   
    }

    serialisation_factory_registry = SerialisationFactoryRegistry()
    serialisation_factory_registry.register_defaults(ACCEPTED_SERIALISATIONS)
    print(f'Created serialisation factory registry with accepted serialisations {ACCEPTED_SERIALISATIONS}')
    serialisation_factory = SerialisationFactory(factory_registry=serialisation_factory_registry)

    key_serialisation_props = {
        'serialisation_library': 'confluent_kafka',
        'serialisation_type': 'string'
    }
    val_serialisation_props = {
        'serialisation_library': 'confluent_kafka',
        'serialisation_type': 'avro'
    }
    msg_serialisation_cfg_manager = KafkaMsgSerialisationCfgManager(key_serialisation_props=key_serialisation_props,
                                                                    val_serialisation_props=val_serialisation_props)
    schema_details = {
        'subject_name': 'ConsumerGood-value'
    }

    schema_registry_client_conf = {
        'url': 'http://localhost:8081'
    }

    confluent_kafka_schema_registry_connector = ConfluentKafkaSchemaRegistryConnector()
    confluent_kafka_schema_registry_client = confluent_kafka_schema_registry_connector.get_schema_registry_client(schema_registry_client_conf=schema_registry_client_conf)

    kafka_msg_producer = KafkaMsgProducer(producer_props=producer_props_cfg,
                                          msg_serialisation_cfg_manager=msg_serialisation_cfg_manager,
                                          serialisation_factory=serialisation_factory,
                                          schema_registry_client=confluent_kafka_schema_registry_client,
                                          schema_details=schema_details)