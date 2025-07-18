from confluent_kafka import Consumer
import threading
from read_kafka_consumer_cfg import KafkaConsumerCfgReader

def consume_message():

    def commit_callback(err, partitions):
        if err:
            print(f'ERROR: Commit failed: {err}')
        else:
            print(f'SUCCESS: Commit succeeded for partitions: {partitions}')

    consumer_cfg_reader = KafkaConsumerCfgReader()
    consumer_props_cfg = consumer_cfg_reader.read_consumer_props_cfg()
    consumer_props_cfg.update({
        'on_commit': commit_callback
    })

    print(f'Consumer properties: {consumer_props_cfg}')

    consumer = Consumer(consumer_props_cfg)

    is_rebalancing = threading.Event()
    is_rebalancing.set()

    def on_assign_callback(consumer, partitions):
        print(f'Consumer group rebalance - partitions have been assigned to consumer: {partitions}')
        is_rebalancing.clear()

    def on_revoke_callback(consumer, partitions):
        print(f'Consumer group rebalance - partitions have been revoked from consumer: {partitions}')
        is_rebalancing.set()

    def on_lost_callback(consumer, partitions):
        print(f'Consumer group rebalance - partitions have been lost from consumer: {partitions}')
        is_rebalancing.set()

    topic = 'uncatg_landing_zone'
    consumer.subscribe([topic], on_assign=on_assign_callback, on_revoke=on_revoke_callback, on_lost=on_lost_callback)

    poll_interval = 1
    commit_count = 5

    count = 0
    try:
        while True:
            msg = consumer.poll(poll_interval)
            if msg is None:
                if is_rebalancing.is_set():
                    print('Waiting for consumer group rebalancing to complete...')
                else:
                    print('Waiting for messages to arrive...')
            elif msg.error():
                print(f'ERROR: {msg.error}')
            else:
                topic=msg.topic()
                key=msg.key().decode('utf-8')
                value=msg.value().decode('utf-8')
                print(f'Received event: {{"topic": {topic}, "key": {key}, "value": {value}}}')
                print(f'Count: {count}')

                if count % commit_count == 0:
                    consumer.commit(asynchronous=True)
                    print(f'Committed offsets asynchronously')
                    assigned_partitions = consumer.assignment()
                    curr_partition_offsets = consumer.position(assigned_partitions)
                    for tp in curr_partition_offsets:
                        print(f'Current offset position for topic {tp.topic} partition {tp.partition}: {tp.offset}')
                count += 1
    finally:
        consumer.close()

if __name__ == '__main__':
    consume_message()