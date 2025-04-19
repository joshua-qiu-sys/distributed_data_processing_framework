from src.data_producers.random_data_generator.kafka_producer import KafkaTransactionalMsgProducerFactory
from src.data_producers.random_data_generator.kafka_producer_cfg_management import KafkaProducerCfgManager, KafkaProducerTopicCfgReader, KafkaProducerPropsCfgReader, KafkaProducerSchemaRegistryConnCfgReader, KafkaProducerMsgSerialisationCfgReader, KafkaProducerMsgSerialisationCfgHandler
from src.data_producers.random_data_generator.consumer_good_generator import ConsumerGoodGenerator

if __name__ == '__main__':

    consumer_good_generator = ConsumerGoodGenerator()

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

    kafka_transactional_msg_producer_factory = KafkaTransactionalMsgProducerFactory()
    kafka_transactional_msg_producer = kafka_transactional_msg_producer_factory.create(producer_cfg_manager=kafka_producer_cfg_manager)
    print(f'Created Kafka transactional msg producer')

    while True:
        consumer_good_key, consumer_good = consumer_good_generator.generate_random_object_tuple()
        kafka_transactional_msg_producer.produce(msg_key=consumer_good_key, msg_val=consumer_good)