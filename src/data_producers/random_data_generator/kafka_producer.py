from confluent_kafka import Producer, Message, KafkaError, KafkaException
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer, SerializationContext, MessageField
from abc import ABC, abstractmethod
from dataclasses import fields, is_dataclass
from typing import Dict, Union, Any, Type, Callable
from random import choice
import time
from decimal import Decimal
import logging
from data_producers.random_data_generator.kafka_producer_cfg_management import KafkaProducerCfgReader
from schema_registry_connectors import AbstractSchemaRegistryClient, ConfluentKafkaSchemaRegistryConnector
from schemas.consumer_good import ConsumerGood
from consumer_good_generator import ConsumerGoodGenerator
from src.utils.cfg_management import AbstractCfgManager
from src.utils.constructors import AbstractFactory, AbstractFactoryRegistry

logger = logging.getLogger(f'random_data_generator')

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
        # serialised_obj = self.serialiser(msg_obj, **serialisation_cfg)
        serialised_obj = self.serialiser(msg_obj, SerializationContext('uncatg_landing_zone', MessageField.VALUE))
        # serialised_obj = self.serialiser(msg_obj, **{'ctx': SerializationContext('uncatg_landing_zone', MessageField.VALUE)})
        return serialised_obj
    
    def deserialise(self, bytes_obj: bytes, deserialiser_cfg: Dict = None, deserialisation_cfg: Dict = None) -> Any:
        if self.deserialiser is None:
            self._setup_deserialiser(deserialiser_cfg=deserialiser_cfg)
        deserialised_obj = self.deserialiser(bytes_obj, **deserialisation_cfg)
        return deserialised_obj
    
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
            if serialisation_type in self._registry[serialisation_library].keys():
                return True
            else:
                return False
        else:
            return False

    def register(self, serialisation_library: str, serialisation_type: str, serialisation: AbstractSerialisation) -> None:
        if not self.is_registered(serialisation_library=serialisation_library, serialisation_type=serialisation_type):
            
            if serialisation_library not in self._registry.keys():
                self._registry[serialisation_library] = {
                    serialisation_type: serialisation
                }
            elif len(self._registry[serialisation_library]) == 0:
                self._registry[serialisation_library][serialisation_type] = serialisation
            else:
                serialisation_library_entry = self._registry[serialisation_library]
                serialisation_library_entry.update({
                    serialisation_type: serialisation
                })
                self._registry.update(serialisation_library_entry)

    def register_defaults(self, default_serialisation_dict: Dict) -> None:

        for serialisation_library in default_serialisation_dict.keys():
            for serialisation_type, serialisation in default_serialisation_dict[serialisation_library].items():
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
        serialisation = serialisation_cls()
        return serialisation
    
class KafkaMsgSerialisationCfgManager(AbstractCfgManager):
    def __init__(self,
                 key_serialisation_factory_cfg: Dict,
                 val_serialisation_factory_cfg: Dict,
                 key_serialiser_cfg: Dict,
                 key_deserialiser_cfg: Dict,
                 key_serialisation_cfg: Dict,
                 key_deserialisation_cfg: Dict,
                 val_serialiser_cfg: Dict,
                 val_deserialiser_cfg: Dict,
                 val_serialisation_cfg: Dict,
                 val_deserialisation_cfg: Dict):
        
        self.key_serialisation_factory_cfg = key_serialisation_factory_cfg
        self.val_serialisation_factory_cfg = val_serialisation_factory_cfg
        self.key_serialiser_cfg = key_serialiser_cfg
        self.key_deserialiser_cfg = key_deserialiser_cfg
        self.key_serialisation_cfg = key_serialisation_cfg
        self.key_deserialisation_cfg = key_deserialisation_cfg
        self.val_serialiser_cfg = val_serialiser_cfg
        self.val_deserialiser_cfg = val_deserialiser_cfg
        self.val_serialisation_cfg = val_serialisation_cfg
        self.val_deserialisation_cfg = val_deserialisation_cfg

    def get_key_serialisation_factory_cfg(self) -> Dict:
        return self.key_serialisation_factory_cfg
    
    def get_val_serialisation_factory_cfg(self) -> Dict:
        return self.val_serialisation_factory_cfg
    
    def get_key_serialiser_cfg(self) -> Dict:
        return self.key_serialiser_cfg
    
    def get_key_deserialiser_cfg(self) -> Dict:
        return self.key_deserialiser_cfg
    
    def get_key_serialisation_cfg(self) -> Dict:
        return self.key_serialisation_cfg
    
    def get_key_deserialisation_cfg(self) -> Dict:
        return self.key_deserialisation_cfg
    
    def get_val_serialiser_cfg(self) -> Dict:
        return self.val_serialiser_cfg
    
    def get_val_deserialiser_cfg(self) -> Dict:
        return self.val_deserialiser_cfg
    
    def get_val_serialisation_cfg(self) -> Dict:
        return self.val_serialisation_cfg
    
    def get_val_deserialisation_cfg(self) -> Dict:
        return self.val_deserialisation_cfg
    
class SchemaHandler:
    def __init__(self, schema_registry_client: AbstractSchemaRegistryClient, schema_details: Dict[str, str]):
        self.schema_registry_client = schema_registry_client
        self.schema_details = schema_details

    def get_schema(self) -> str:
        schema = self.schema_registry_client.get_schema(schema_details=self.schema_details)
        return schema
    
class SerialisationHandler:
    def __init__(self,
                 schema_registry_client: AbstractSchemaRegistryClient,
                 serialisation_factory: SerialisationFactory,
                 serialisation_cfg_manager: KafkaMsgSerialisationCfgManager):
        
        self.schema_registry_client = schema_registry_client
        self.serialisation_factory = serialisation_factory
        self.serialisation_cfg_manager = serialisation_cfg_manager
        self.key_serialisation = None
        self.val_serialisation = None

    def setup_serialisation(self) -> None:
        key_serialisation = self.serialisation_factory.create(**self.serialisation_cfg_manager.get_key_serialisation_factory_cfg())
        self.key_serialisation = key_serialisation
        val_serialisation = self.serialisation_factory.create(**self.serialisation_cfg_manager.get_val_serialisation_factory_cfg())
        self.val_serialisation = val_serialisation

    def get_key_serialisation(self) -> AbstractSerialisation:
        return self.key_serialisation
    
    def get_val_serialisation(self) -> AbstractSerialisation:
        return self.val_serialisation
    
class KafkaMsgConverter:
    def __init__(self):
        pass

    def get_kafka_msg_to_dict_callable(self) -> Callable:
        return self.kafka_msg_to_dict
    
    def get_kafka_msg_from_dict_callable(self) -> Callable:
        return self.kafka_msg_from_dict
    
    def kafka_msg_to_dict(self, message_obj: Any, ctx: SerializationContext = None) -> Dict:
        if not is_dataclass(message_obj):
            raise TypeError("Message object must be a dataclass type")
        message_val_dict = {}
        for attribute in fields(message_obj):
            message_val_dict[attribute] = getattr(message_obj, attribute)
        return message_val_dict
    
    def kafka_msg_from_dict(self, message_dict: Dict, message_dataclass: Type[Any], ctx: SerializationContext = None) -> Any:
        if not is_dataclass(message_dataclass):
            raise TypeError("Message class must be a dataclass type")
        message_val_obj = message_dataclass(**message_dict)
        return message_val_obj

class KafkaMsgProducer(Producer):
    def __init__(self,
                 topic: str,
                 producer_props: Dict[str, Union[str, int]],
                 schema_handler: SchemaHandler,
                 serialisation_handler: SerialisationHandler,
                 serialisation_cfg_manager: KafkaMsgSerialisationCfgManager,
                 poll_interval: float = 3,
                 flush_interval: float = None):
        
        self.topic = topic
        self.producer = Producer(producer_props)
        self.producer_props = producer_props
        self.schema_handler = schema_handler
        self.serialisation_handler = serialisation_handler
        self.serialisation_cfg_manager = serialisation_cfg_manager
        self.poll_interval = poll_interval
        self.flush_interval = flush_interval
        
        self.msg_count = 0

        curr_time = time.time()
        self.last_poll_time = curr_time
        self.last_flush_time = curr_time

    def _delivery_callback(self, err: KafkaError, msg: Message) -> None:
        if err:
            print(f'ERROR: Message delivery failed: {err}')
        else:
            topic = msg.topic()

            key_deserialiser_cfg = self.serialisation_cfg_manager.get_key_deserialiser_cfg()
            key_deserialisation_cfg = self.serialisation_cfg_manager.get_key_deserialisation_cfg()
            key = self.serialisation_handler.get_key_serialisation().deserialise(bytes_obj=msg.key(), deserialiser_cfg=key_deserialiser_cfg, deserisalisation_cfg=key_deserialisation_cfg)

            val_deserialiser_cfg = self.serialisation_cfg_manager.get_val_deserialiser_cfg()
            val_deserialisation_cfg = self.serialisation_cfg_manager.get_val_deserialisation_cfg()
            val = self.serialisation_handler.get_key_serialisation().deserialise(bytes_obj=msg.value(), deserialiser_cfg=val_deserialiser_cfg, deserisalisation_cfg=val_deserialisation_cfg)

            val_to_dict_callable = self.serialisation_cfg_manager.get_key_deserialiser_cfg()['to_dict'] if 'to_dict' in self.serialisation_cfg_manager.get_key_deserialiser_cfg().keys() else None
            print(f'SUCCESS: Message delivery succeeded: {{"topic": {topic}, "key": {key}, "value": {val_to_dict_callable(message_obj=val)}}}')

    def produce(self, msg_key: Any, msg_val: Any, poll_enabled: bool = True, flush_enabled: bool = True) -> None:
        try:
            self.produce_message(msg_key=msg_key, msg_val=msg_val)
            
            print(f'Message count: {self.msg_count}')
            self.count += 1
        except BufferError:
            print(f'Buffer is full. Pausing for 2 seconds to allow messages to be sent from buffer before resuming.')
            time.sleep(2)
        except KafkaException as e:
            print(f'Kafka error occurred: {str(e)}')

        if poll_enabled:
            self.poll_on_interval()
        if flush_enabled:
            self.flush_on_interval()
    
    def produce_message(self, msg_key: Any, msg_val: Any) -> None:
        
        key_serialisation = self.serialisation_handler.get_key_serialisation()
        val_serialisation = self.serialisation_handler.get_val_serialisation()
        key_serialiser_cfg = self.serialisation_cfg_manager.get_key_serialiser_cfg()
        val_serialiser_cfg = self.serialisation_cfg_manager.get_val_serialiser_cfg()
        key_serialisation_cfg = self.serialisation_cfg_manager.get_key_serialisation_cfg()
        val_serialisation_cfg = self.serialisation_cfg_manager.get_val_serialisation_cfg()

        serialised_key = key_serialisation.serialise(msg_obj=msg_key, serialiser_cfg=key_serialiser_cfg, serialisation_cfg=key_serialisation_cfg)
        serialised_val = val_serialisation.serialise(msg_obj=msg_val, serialiser_cfg=val_serialiser_cfg, serialisation_cfg=val_serialisation_cfg)
        
        self.producer.produce(topic=self.topic,
                              key=serialised_key,
                              value=serialised_val,
                              callback=self._delivery_callback)
        
        val_to_dict_callable = self.serialisation_cfg_manager.get_key_deserialiser_cfg()['to_dict'] if 'to_dict' in self.serialisation_cfg_manager.get_key_deserialiser_cfg().keys() else None
        print(f'Sent data to buffer: {{"topic": {self.topic}, "key": {msg_key}, "value": {val_to_dict_callable(msg_val)}}}')

    def poll_on_interval(self) -> None:
        curr_time = time.time()
        if curr_time >= self.last_poll_time + self.poll_interval:
            print('Producer is polling. Handling delivery callback responses from brokers...')
            self.producer.poll(self.poll_interval)
            self.last_poll_time = time.time()

    def flush_on_interval(self) -> None:
        curr_time = time.time()
        if curr_time >= last_flush_time + self.flush_interval:
            print('Producer is flushing records to brokers. Blocking current thread until completion...')
            self.producer.flush()
            last_flush_time = time.time()

class KafkaTransactionalMsgProducer(KafkaMsgProducer):
    def __init__(self, producer_props: Dict[str, Union[str, int]]):
        super().__init__(producer_props)
        self._init_transactional_mode()

    def _init_transactional_mode(self) -> None:
        self.producer.init_transactions()

    def _delivery_callback(self) -> None:
        super()._delivery_callback()

    def produce(self, msg_key: Any, msg_val: Any, poll_enabled: bool = True, flush_enabled: bool = False) -> None:
        try:
            self.producer.begin_transaction()
            super().produce(msg_key=msg_key, msg_val=msg_val)
            self.producer.commit_transaction()
            print("Transaction committed successfully")

            print(f'Message count: {self.msg_count}')
            self.count += 1
        except BufferError:
            print(f'Buffer is full. Pausing for 2 seconds to allow messages to be sent from buffer before resuming.')
            time.sleep(2)
        except KafkaException as e:
            print(f'Kafka error occurred: {str(e)}')

        if poll_enabled:
            self.poll_on_interval()
        if flush_enabled:
            self.flush_on_interval()

class KafkaMsgProducerFactory:
    def __init__(self):
        pass

    def create(self) -> KafkaMsgProducer:

        topic = 'uncatg_landing_zone'
    
        producer_cfg_reader = KafkaProducerCfgReader()
        producer_props_cfg = producer_cfg_reader.read_producer_props_cfg()
        print(f'Producer properties: {producer_props_cfg}')

        schema_registry_client_conf = {
            'conf': {
                'url': 'http://localhost:8081'
            }
        }

        confluent_kafka_schema_registry_connector = ConfluentKafkaSchemaRegistryConnector()
        confluent_kafka_schema_registry_client = confluent_kafka_schema_registry_connector.get_schema_registry_client(schema_registry_client_conf=schema_registry_client_conf)

        ACCEPTED_SERIALISATIONS = {
            'confluent_kafka': {
                'string': ConfluentKafkaStringSerialisation,
                'avro': ConfluentKafkaAvroSerialisation
            }   
        }

        schema_details = {
            'subject_name': 'ConsumerGood-value'
        }

        schema_handler = SchemaHandler(schema_registry_client=confluent_kafka_schema_registry_client,
                                    schema_details=schema_details)
        schema = schema_handler.get_schema()

        kafka_msg_converter = KafkaMsgConverter()
        kafka_msg_from_dict_callable = kafka_msg_converter.get_kafka_msg_from_dict_callable()
        kafka_msg_to_dict_callable = kafka_msg_converter.get_kafka_msg_to_dict_callable()

        serialisation_factory_registry = SerialisationFactoryRegistry()
        serialisation_factory_registry.register_defaults(ACCEPTED_SERIALISATIONS)
        print(f'Created serialisation factory registry with accepted serialisations {ACCEPTED_SERIALISATIONS}')
        serialisation_factory = SerialisationFactory(factory_registry=serialisation_factory_registry)

        key_serialisation_factory_cfg = {
            'serialisation_library': 'confluent_kafka',
            'serialisation_type': 'string'
        }
        val_serialisation_factory_cfg = {
            'serialisation_library': 'confluent_kafka',
            'serialisation_type': 'avro'
        }

        key_serialiser_cfg = {
            'codec': 'utf8'
        }
        key_deserialiser_cfg = {
            'codec': 'utf8'
        }
        key_serialisation_cfg = {}
        key_deserialisation_cfg = {}

        val_serialiser_cfg = {
            'schema_registry_client': confluent_kafka_schema_registry_client,
            'schema_str': schema,
            'to_dict': kafka_msg_to_dict_callable
        }
        val_deserialiser_cfg = {
            'schema_registry_client': confluent_kafka_schema_registry_client,
            'schema_str': schema,
            'from_dict': kafka_msg_from_dict_callable
        }
        val_serialisation_cfg = {
            'ctx': SerializationContext(topic, MessageField.VALUE)
        }
        val_deserialisation_cfg = {
            'ctx': SerializationContext(topic, MessageField.VALUE)
        }

        serialisation_cfg_manager = KafkaMsgSerialisationCfgManager(key_serialisation_factory_cfg=key_serialisation_factory_cfg,
                                                                    val_serialisation_factory_cfg=val_serialisation_factory_cfg,
                                                                    key_serialiser_cfg=key_serialiser_cfg,
                                                                    key_deserialiser_cfg=key_deserialiser_cfg,
                                                                    key_serialisation_cfg=key_serialisation_cfg,
                                                                    key_deserialisation_cfg=key_deserialisation_cfg,
                                                                    val_serialiser_cfg=val_serialiser_cfg,
                                                                    val_deserialiser_cfg=val_deserialiser_cfg,
                                                                    val_serialisation_cfg=val_serialisation_cfg,
                                                                    val_deserialisation_cfg=val_deserialisation_cfg)

        serialisation_handler = SerialisationHandler(schema_registry_client=confluent_kafka_schema_registry_client,
                                                    serialisation_factory=serialisation_factory,
                                                    serialisation_cfg_manager=serialisation_cfg_manager)
        serialisation_handler.setup_serialisation()

        kafka_msg_producer = KafkaMsgProducer(topic=topic,
                                            producer_props=producer_props_cfg,
                                            schema_handler=schema_handler,
                                            serialisation_handler=serialisation_handler,
                                            serialisation_cfg_manager=serialisation_cfg_manager)
        
        return kafka_msg_producer

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

    def delivery_callback(err: KafkaError, msg: Message):
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
    count = 0
    curr_time = time.time()
    last_poll_time = curr_time
    last_flush_time = curr_time

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
        poll_interval = 3
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

    # kafka_msg_producer_factory = KafkaMsgProducerFactory()
    # kafka_msg_producer = kafka_msg_producer_factory.create()