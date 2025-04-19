from confluent_kafka import Producer, Message, KafkaError, KafkaException
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer, SerializationContext, MessageField
from typing import Dict, Union, Any, Optional
from random import choice
import time
from decimal import Decimal
import logging
from src.data_producers.random_data_generator.kafka_producer_cfg_management import KafkaProducerCfgManager, KafkaProducerTopicCfgReader, KafkaProducerPropsCfgReader, KafkaProducerSchemaRegistryConnCfgReader, KafkaProducerMsgSerialisationCfgReader, KafkaProducerMsgSerialisationCfgHandler
from src.data_producers.random_data_generator.schemas.consumer_good import ConsumerGood
from src.data_producers.random_data_generator.schema_registry_connector_management import SchemaRegistryConnectorFactory, SchemaRegistryConnectorFactoryRegistry
from src.data_producers.random_data_generator.serialisation_management import SerialisationHandler, SerialisationFactory, SerialisationFactoryRegistry, SerialisationCfgManager
from src.data_producers.random_data_generator.schema_registry_connector_management import ACCEPTED_SCHEMA_REGISTRIES
from src.data_producers.random_data_generator.serialisation_management import ACCEPTED_SERIALISATIONS

logger = logging.getLogger(f'random_data_generator')

class KafkaMsgProducer(Producer):
    def __init__(self,
                 topic: str,
                 producer_props: Dict[str, Union[str, int]],
                 serialisation_handler: SerialisationHandler,
                 serialisation_cfg_manager: SerialisationCfgManager,
                 poll_interval: Optional[float] = 3,
                 flush_interval: Optional[float] = 15):
        
        self.topic = topic
        self.producer = Producer(producer_props)
        self.producer_props = producer_props
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
            key = self.serialisation_handler.get_key_serialisation().deserialise(bytes_obj=msg.key(), deserialiser_cfg=key_deserialiser_cfg, deserialisation_cfg=key_deserialisation_cfg)

            val_deserialiser_cfg = self.serialisation_cfg_manager.get_val_deserialiser_cfg()
            val_deserialisation_cfg = self.serialisation_cfg_manager.get_val_deserialisation_cfg()
            val = self.serialisation_handler.get_val_serialisation().deserialise(bytes_obj=msg.value(), deserialiser_cfg=val_deserialiser_cfg, deserialisation_cfg=val_deserialisation_cfg)

            val_to_dict_callable = self.serialisation_cfg_manager.get_val_serialiser_cfg()['to_dict'] if 'to_dict' in self.serialisation_cfg_manager.get_val_serialiser_cfg().keys() else None
            print(f'SUCCESS: Message delivery succeeded: {{"topic": {topic}, "key": {key}, "value": {val_to_dict_callable(obj=val)}}}')

    def produce(self, msg_key: Any, msg_val: Any, poll_enabled: bool = True, flush_enabled: bool = True) -> None:

        if poll_enabled and self.poll_interval is None:
            raise ValueError(f'Producer cannot poll when poll interval is not set')
        
        if flush_enabled and self.flush_interval is None:
            raise ValueError(f'Producer cannot flush when flush interval is not set')

        try:
            self.produce_message(msg_key=msg_key, msg_val=msg_val)
            
            print(f'Message count: {self.msg_count}')
            self.msg_count += 1
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
        
        val_to_dict_callable = self.serialisation_cfg_manager.get_val_serialiser_cfg()['to_dict'] if 'to_dict' in self.serialisation_cfg_manager.get_val_serialiser_cfg().keys() else None
        print(f'Sent data to buffer: {{"topic": {self.topic}, "key": {msg_key}, "value": {val_to_dict_callable(obj=msg_val)}}}')

    def poll_on_interval(self) -> None:
        curr_time = time.time()
        if curr_time >= self.last_poll_time + self.poll_interval:
            print('Producer is polling. Handling delivery callback responses from brokers...')
            self.producer.poll(self.poll_interval)
            self.last_poll_time = time.time()

    def flush_on_interval(self) -> None:
        curr_time = time.time()
        if curr_time >= self.last_flush_time + self.flush_interval:
            print('Producer is flushing records to brokers. Blocking current thread until completion...')
            self.producer.flush()
            self.last_flush_time = time.time()

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

    def _setup_cfg(self,
                   producer_topic_cfg: Dict[str, str],
                   producer_props_cfg: Dict[str, Union[str, int]],
                   producer_schema_registry_conn_cfg: Dict,
                   producer_msg_serialisation_cfg: Dict) -> Dict:
        
        schema_registry_type = producer_schema_registry_conn_cfg['schema_registry_connector']['type']

        schema_registry_connector_factory_registry = SchemaRegistryConnectorFactoryRegistry()
        schema_registry_connector_factory_registry.register_defaults(ACCEPTED_SCHEMA_REGISTRIES)
        print(f'Created schema registry connector factory registry with accepted schema registries {ACCEPTED_SCHEMA_REGISTRIES}')
        schema_registry_connector_factory = SchemaRegistryConnectorFactory(factory_registry=schema_registry_connector_factory_registry)
        schema_registry_connector = schema_registry_connector_factory.create(schema_registry_type=schema_registry_type)

        schema_registry_client_conf = producer_schema_registry_conn_cfg['schema_registry_connector']['schema_registry_client_conf']
        schema_registry_client = schema_registry_connector.get_schema_registry_client(schema_registry_client_conf=schema_registry_client_conf)

        serialisation_factory_registry = SerialisationFactoryRegistry()
        serialisation_factory_registry.register_defaults(ACCEPTED_SERIALISATIONS)
        print(f'Created serialisation factory registry with accepted serialisations {ACCEPTED_SERIALISATIONS}')
        serialisation_factory = SerialisationFactory(factory_registry=serialisation_factory_registry)

        key_serialisation_factory_cfg = producer_msg_serialisation_cfg['key_serialisation']['key_serialisation_factory']
        val_serialisation_factory_cfg = producer_msg_serialisation_cfg['val_serialisation']['val_serialisation_factory']
        key_serialiser_cfg = producer_msg_serialisation_cfg['key_serialisation']['key_serialiser']
        key_deserialiser_cfg = producer_msg_serialisation_cfg['key_serialisation']['key_deserialiser']
        key_serialisation_cfg = producer_msg_serialisation_cfg['key_serialisation']['key_serialisation'] if 'key_serialisation' in producer_msg_serialisation_cfg['key_serialisation'].keys() else None
        key_deserialisation_cfg = producer_msg_serialisation_cfg['key_serialisation']['key_deserialisation'] if 'key_deserialisation' in producer_msg_serialisation_cfg['key_serialisation'].keys() else None
        val_serialiser_cfg = producer_msg_serialisation_cfg['val_serialisation']['val_serialiser']
        val_deserialiser_cfg = producer_msg_serialisation_cfg['val_serialisation']['val_deserialiser']
        val_serialisation_cfg = producer_msg_serialisation_cfg['val_serialisation']['val_serialisation'] if 'val_serialisation' in producer_msg_serialisation_cfg['val_serialisation'].keys() else None
        val_deserialisation_cfg = producer_msg_serialisation_cfg['val_serialisation']['val_deserialisation'] if 'val_deserialisation' in producer_msg_serialisation_cfg['val_serialisation'].keys() else None

        serialisation_cfg_manager = SerialisationCfgManager(key_serialisation_factory_cfg=key_serialisation_factory_cfg,
                                                            val_serialisation_factory_cfg=val_serialisation_factory_cfg,
                                                            key_serialiser_cfg=key_serialiser_cfg,
                                                            key_deserialiser_cfg=key_deserialiser_cfg,
                                                            key_serialisation_cfg=key_serialisation_cfg,
                                                            key_deserialisation_cfg=key_deserialisation_cfg,
                                                            val_serialiser_cfg=val_serialiser_cfg,
                                                            val_deserialiser_cfg=val_deserialiser_cfg,
                                                            val_serialisation_cfg=val_serialisation_cfg,
                                                            val_deserialisation_cfg=val_deserialisation_cfg)

        serialisation_handler = SerialisationHandler(schema_registry_client=schema_registry_client,
                                                     serialisation_factory=serialisation_factory,
                                                     serialisation_cfg_manager=serialisation_cfg_manager)
        serialisation_handler.setup_serialisation()

        producer_setup_cfg = {
            'topic': producer_topic_cfg['topic_name'],
            'producer_props': producer_props_cfg,
            'serialisation_handler': serialisation_handler,
            'serialisation_cfg_manager': serialisation_cfg_manager
        }

        return producer_setup_cfg

    def _create_producer(self,
                         topic: str,
                         producer_props: Dict[str, Union[str, int]],
                         serialisation_handler: SerialisationHandler,
                         serialisation_cfg_manager: SerialisationCfgManager) -> KafkaMsgProducer:
        
        kafka_msg_producer = KafkaMsgProducer(topic=topic,
                                              producer_props=producer_props,
                                              serialisation_handler=serialisation_handler,
                                              serialisation_cfg_manager=serialisation_cfg_manager)
        return kafka_msg_producer

    def create(self, producer_cfg_manager: KafkaProducerCfgManager) -> KafkaMsgProducer:

        producer_topic_cfg = producer_cfg_manager.get_producer_topic_cfg()
        producer_props_cfg = producer_cfg_manager.get_producer_props_cfg()
        producer_schema_registry_conn_cfg = producer_cfg_manager.get_producer_schema_registry_conn_cfg()
        producer_msg_serialisation_cfg = producer_cfg_manager.get_producer_msg_serialisation_cfg()
    
        producer_setup_cfg = self._setup_cfg(producer_topic_cfg=producer_topic_cfg,
                                             producer_props_cfg=producer_props_cfg,
                                             producer_schema_registry_conn_cfg=producer_schema_registry_conn_cfg,
                                             producer_msg_serialisation_cfg=producer_msg_serialisation_cfg)
        
        kafka_msg_producer = self._create_producer(**producer_setup_cfg)
        
        return kafka_msg_producer
    
class KafkaTransactionalMsgProducerFactory(KafkaMsgProducerFactory):
    def __init__(self):
        pass

    def create(self):
        pass

def produce_message():

    producer_cfg_reader = KafkaProducerPropsCfgReader()
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
    # produce_message()

    producer_props_cfg_reader = KafkaProducerPropsCfgReader()
    producer_props_cfg = producer_props_cfg_reader.read_producer_props_cfg()
    print(f'Producer properties: {producer_props_cfg}')

    producer_topic_cfg_reader = KafkaProducerTopicCfgReader()
    producer_topic_cfg = producer_topic_cfg_reader.read_producer_topic_cfg()
    print(f'Producer topic properties: {producer_topic_cfg}')

    producer_schema_registry_conn_cfg_reader = KafkaProducerSchemaRegistryConnCfgReader()
    producer_schema_registry_conn_cfg = producer_schema_registry_conn_cfg_reader.read_producer_schema_registry_conn_cfg()
    print(f'Producer schema registry connector properties: {producer_schema_registry_conn_cfg}')

    producer_msg_serialisation_cfg_reader = KafkaProducerMsgSerialisationCfgReader()
    unrendered_producer_msg_serialisation_cfg = producer_msg_serialisation_cfg_reader.read_unrendered_msg_serialisation_cfg()
    print(f'Unrendered producer msg serialisation properties: {unrendered_producer_msg_serialisation_cfg}')

    producer_msg_serialisation_cfg_handler = KafkaProducerMsgSerialisationCfgHandler()
    processed_producer_msg_serialisation_cfg = producer_msg_serialisation_cfg_handler.process_cfg(producer_topic_cfg=producer_topic_cfg,
                                                                                                  producer_schema_registry_conn_cfg=producer_schema_registry_conn_cfg,
                                                                                                  unrendered_producer_msg_serialisation_cfg=unrendered_producer_msg_serialisation_cfg)
    print(f'Processed producer msg serialisation properties: {processed_producer_msg_serialisation_cfg}')

    kafka_producer_cfg_manager = KafkaProducerCfgManager(producer_topic_cfg=producer_topic_cfg,
                                                         producer_props_cfg=producer_props_cfg,
                                                         producer_schema_registry_conn_cfg=producer_schema_registry_conn_cfg,
                                                         producer_msg_serialisation_cfg=processed_producer_msg_serialisation_cfg)

    kafka_msg_producer_factory = KafkaMsgProducerFactory()
    kafka_msg_producer = kafka_msg_producer_factory.create(producer_cfg_manager=kafka_producer_cfg_manager)
    print(f'Created Kafka msg producer')