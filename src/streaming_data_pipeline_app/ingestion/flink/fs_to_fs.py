from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSource, FileSink, StreamFormat
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor
from pyflink.common.serialization import Encoder
from pyflink.common.watermark_strategy import WatermarkStrategy, Duration, TimestampAssigner
from pyflink.common.typeinfo import Types
import datetime as dt

STATE_CLEANUP_INTERVAL = 30_000

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
        print(f'wm: {curr_watermark}')
        print(event_ts)
        if event_ts < curr_watermark:
            updated_late_records_state_expiry_ts = ctx.timer_service().current_processing_time() + STATE_CLEANUP_INTERVAL
            prev_timer = self.late_records_timer_state.value()
            if prev_timer is not None:
                ctx.timer_service().delete_processing_time_timer(prev_timer)
            self.late_records_list_state.add(value)
            print(f'Dropped watermarks for current key {ctx.get_current_key()}: {self.late_records_list_state.get()}')
            
            ctx.timer_service().register_processing_time_timer(updated_late_records_state_expiry_ts)
            self.late_records_timer_state.update(updated_late_records_state_expiry_ts)
            print(f'Registered new timer for current key {ctx.get_current_key()}: {self.late_records_timer_state.value()}')
            return None
        else:
            prev_timer = self.late_records_timer_state.value()
            if prev_timer is None:
                late_records_state_expiry_ts = ctx.timer_service().current_processing_time() + STATE_CLEANUP_INTERVAL
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
        ts_millis = int(dt.datetime.strptime(str(value).split('|')[3], "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        return ts_millis

env = StreamExecutionEnvironment.get_execution_environment()

input_path = 'data/raw/dataset0'
ds = env.from_source(source=FileSource \
                        .for_record_stream_format(StreamFormat.text_line_format(), input_path) \
                        .monitor_continuously(Duration.of_millis(250)) \
                        .build(),
                     watermark_strategy=WatermarkStrategy \
                        .for_bounded_out_of_orderness(Duration.of_seconds(2)) \
                        .with_timestamp_assigner(RecordTimestampAssigner()),
                     source_name='input_fs',
                     type_info=Types.STRING())

ds = ds.key_by(lambda event: event.split('|')[0]) \
    .process(DropLateRecordsKeyedProcessFunction(),
             Types.STRING())

output_path = 'data/processed/dataset0'
ds.sink_to(sink=FileSink \
               .for_row_format(base_path=output_path, encoder=Encoder.simple_string_encoder()) \
               .build())

env.execute(job_name='FS to FS Flink Application')