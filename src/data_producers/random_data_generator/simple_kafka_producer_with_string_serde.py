from confluent_kafka import Producer, Message, KafkaError, KafkaException
from confluent_kafka.serialization import StringSerializer, StringDeserializer
from typing import Dict
from random import choice
import time
import datetime as dt
from decimal import Decimal
from src.data_producers.random_data_generator.kafka_producer_cfg_management import KafkaProducerPropsCfgReader

def produce_message():

    producer_cfg_reader = KafkaProducerPropsCfgReader()
    producer_props_cfg = producer_cfg_reader.read_producer_props_cfg()
    print(f'Producer properties: {producer_props_cfg}')

    producer = Producer(producer_props_cfg)
    producer.init_transactions()
    print(f'Initiated producer transactions')

    string_serialiser = StringSerializer('utf8')

    def delivery_callback(err: KafkaError, msg: Message):
        if err:
            print(f'ERROR: Message delivery failed: {err}')
        else:
            topic = msg.topic()
            
            string_deserialiser = StringDeserializer('utf8')
            key = string_deserialiser(msg.key())
            value = string_deserialiser(msg.value())
            print(f'SUCCESS: Message delivery succeeded: {{"topic": {topic}, "key": {key}, "value": {value}}}')

    topic = 'uncatg_topic'
    products = ['computer', 'television', 'smartphone', 'book', 'clothing', 'alarm clock', 'batteries', 'headphones',
                'toothpaste', 'shampoo', 'laundry detergent', 'paper towels', 'charger', 'sunscreen', 'instant noodles',
                'packaged snacks', 'light bulb', 'bottled water', 'air freshener', 'cooking oil', 'canned soup']
    retailers = ['Woolworths', 'Coles', 'Aldi', 'IGA', 'Amazon', 'Costco']
    prices = [Decimal('5.00'), Decimal('10.00'), Decimal('4.50'), Decimal('9.99'), Decimal('9.83'), Decimal('25.60'),
              Decimal('35.40'), Decimal('23.87'), Decimal('15.00'), Decimal('12.30'), Decimal('2.01'), Decimal('7.45'),
              Decimal('5.07'), Decimal('3.88'), Decimal('75.35'), Decimal('11.00'), Decimal('3.00'), Decimal('1.00'),
              Decimal('9.90'), Decimal('78.39'), Decimal('2.00')]
    txn_delay = [0, 1, 2, 3, 5, 7, 9, 10, 15]
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
            consumer_good_txn_timestamp = dt.datetime.strftime(dt.datetime.now() - dt.timedelta(seconds=choice(txn_delay)), format='%Y-%m-%d %H:%M:%S')
            consumer_good = f'{consumer_good_item}|{consumer_good_retailer}|{str(consumer_good_price)}|{consumer_good_txn_timestamp}'

            producer.produce(topic=topic,
                             key=string_serialiser(key),
                             value=string_serialiser(consumer_good),
                             callback=delivery_callback)
            print(f'Sent data to buffer: {{"topic": {topic}, "key": {key}, "value": {consumer_good}}}')
            
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
            last_poll_time = time.time()
        # if curr_time >= last_flush_time + flush_interval:
        #     print('Producer is flushing records to brokers. Blocking current thread until completion...')
        #     producer.flush()
        #     last_flush_time = curr_time

if __name__ == '__main__':
    produce_message()