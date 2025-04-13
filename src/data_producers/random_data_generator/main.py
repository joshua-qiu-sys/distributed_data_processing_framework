from kafka_producer import KafkaMsgProducerFactory
from consumer_good_generator import ConsumerGoodGenerator

if __name__ == '__main__':

    consumer_good_generator = ConsumerGoodGenerator()
    
    kafka_msg_producer_factory = KafkaMsgProducerFactory()
    kafka_msg_producer = kafka_msg_producer_factory.create()

    while True:
        consumer_good_key, consumer_good = consumer_good_generator.generate_random_object_tuple()
        kafka_msg_producer.produce(msg_key=consumer_good_key, msg_val=consumer_good)