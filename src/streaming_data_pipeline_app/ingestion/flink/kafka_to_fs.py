from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.common.serialization import Encoder, SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, Duration, TimestampAssigner
from pyflink.common.typeinfo import Types
import datetime as dt

STATE_FAST_CLEANUP_INTERVAL = 5_000
STATE_REG_CLEANUP_INTERVAL = 30_000

class DropLateRecordsKeyedProcessFunction(KeyedProcessFunction):
    def open(self, runtime_context):
        list_state_descriptor = ListStateDescriptor(name='dropped_late_records_state',
                                                    elem_type_info=Types.STRING())
        value_state_descriptor = ValueStateDescriptor(name='dropped_late_records_timer_state',
                                                      value_type_info=Types.LONG())
        self.late_records_list_state = runtime_context.get_list_state(list_state_descriptor)
        self.late_records_timer_state = runtime_context.get_state(value_state_descriptor)

    def process_element(self, value, ctx):
        
        curr_watermark = ctx.timer_service().current_watermark()
        event_ts = int(dt.datetime.timestamp(dt.datetime.strptime(value.split('|')[3], "%Y-%m-%d %H:%M:%S")) * 1000)
        print(f'curr_ts: {ctx.timestamp()}')
        print(f'wm: {curr_watermark}')
        if event_ts < curr_watermark:
            updated_late_records_state_expiry_ts = ctx.timer_service().current_processing_time() + STATE_FAST_CLEANUP_INTERVAL
            prev_timer = self.late_records_timer_state.value()
            if prev_timer is not None:
                if prev_timer - ctx.timer_service().current_processing_time() > STATE_FAST_CLEANUP_INTERVAL:
                    ctx.timer_service().delete_processing_time_timer(prev_timer)
                    ctx.timer_service().register_processing_time_timer(updated_late_records_state_expiry_ts)
                    self.late_records_timer_state.update(updated_late_records_state_expiry_ts)
                    print(f'Registered new timer for current key {ctx.get_current_key()}: {self.late_records_timer_state.value()}')
            self.late_records_list_state.add(value)
            print(f'Dropped late records for current key {ctx.get_current_key()}: {list(self.late_records_list_state.get())}')
            return None
        else:
            prev_timer = self.late_records_timer_state.value()
            if prev_timer is None:
                late_records_state_expiry_ts = ctx.timer_service().current_processing_time() + STATE_REG_CLEANUP_INTERVAL
                ctx.timer_service().register_processing_time_timer(late_records_state_expiry_ts)
                self.late_records_timer_state.update(late_records_state_expiry_ts)
                print(f'Registered new timer for current key {ctx.get_current_key()}: {self.late_records_timer_state.value()}')
            yield value
    
    def on_timer(self, timestamp, ctx):
        print(f'Cleaning up late record state for key {ctx.get_current_key()}...')
        self.late_records_list_state.clear()
        self.late_records_timer_state.clear()
        yield from ()

class RecordTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        ts_millis = int(dt.datetime.strptime(value.split('|')[3], "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        return ts_millis
    
class RecordTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        ts_millis = int(value.split('|')[3])
        print(f'timestamp_assigner_millis: {ts_millis}')
        return []

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.get_config().set_auto_watermark_interval(200)
env.set_parallelism(1)

kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers('localhost:9093,localhost:8093,localhost:7093') \
    .set_topics("uncatg_topic") \
    .set_group_id("flink_kafka_consumer") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

watermark_strategy = WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_seconds(3)) \
    .with_idleness(Duration.of_seconds(5)) \
    .with_timestamp_assigner(RecordTimestampAssigner())

ds = env.from_source(source=kafka_source,
                     watermark_strategy=watermark_strategy,
                     source_name='input_kafka_topic',
                     type_info=Types.STRING())

ds = ds.key_by(lambda event: event.split('|')[0]) \
    .process(DropLateRecordsKeyedProcessFunction(),
             Types.STRING())

output_path = 'data/processed/dataset0'
ds.sink_to(sink=FileSink \
               .for_row_format(base_path=output_path, encoder=Encoder.simple_string_encoder()) \
               .build())

env.execute(job_name='Kafka to FS Flink Application')