from confluent_kafka import Producer
from random import choice
import time

def produce_message():

    config = {
        'bootstrap.servers': 'localhost:9093,localhost:8093,localhost:7093',
        'acks': 'all'
    }

    producer = Producer(config)

    def delivery_callback(err, msg):
        if err:
            print(f'ERROR: Message delivery failed: {err}')
        else:
            topic = msg.topic()
            key = msg.key().decode('utf-8')
            value = msg.value().decode('utf-8')
            print(f'SUCCESS: Message delivery succeeded: {{"topic": {topic}, "key": {key}, "value": {value}}}')

    topic = 'uncatg_landing_zone'
    products = ['computer', 'television', 'smartphone', 'book', 'clothing', 'alarm clock', 'batteries', 'headphones',
                'toothpaste', 'shampoo', 'laundry detergent', 'paper towels', 'charger', 'sunscreen', 'instant noodles',
                'packaged snacks', 'light bulb', 'bottled water', 'air freshener', 'cooking oil', 'canned soup']

    poll_interval = 3
    flush_interval = 15
    curr_time = time.time()
    last_poll_time = curr_time
    last_flush_time = curr_time

    count = 0
    while True:
        try:
            key = str(products.index(choice(products)))
            value = choice(products)
            producer.produce(topic, value, key, callback=delivery_callback)
            print(f'Sent data to buffer: {{"topic": {topic}, "key": {key}, "value": {value}}}')
            print(f'Count: {count}')
            count += 1
        except BufferError as e:
            print(f'Buffer is full. Pausing for 2 seconds before resuming.')
            time.sleep(2)

        curr_time = time.time()
        if curr_time >= last_poll_time + poll_interval:
            print('Producer is polling. Sending existing records in buffer and handling responses from brokers...')
            producer.poll(poll_interval)
            last_poll_time = curr_time
        if curr_time >= last_flush_time + flush_interval:
            print('Producer is flushing records to brokers. Blocking current thread until completion...')
            producer.flush()
            last_flush_time = curr_time

        time.sleep(1)

if __name__ == '__main__':
    produce_message()