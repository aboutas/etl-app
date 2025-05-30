from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic, Time
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
import json
from transformations import LowerCase, UpperCase, Trimming, YearExtraction
from mongo_sink import MongoSink
from utils import assign_timestamps, unique_user_count
from window_aggregations import UniqueUsers , UserStats
def main(config_path):
    with open(config_path) as f:
        config = json.load(f)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    kafka_props = {
        'bootstrap.servers': config["kafka_broker"],
        'group.id': config.get("group_id", "flink-etl"),
        'auto.offset.reset': 'earliest'
    }
    consumer = FlinkKafkaConsumer(
        topics=config["topic"],
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    ds = env.add_source(consumer)
    ds = ds.map(lambda v: json.loads(v))

    # --- Assign Timestamps & Watermarks ---
    ds = assign_timestamps(ds)

    # --- MAP/TRANSFORMATIONS ---
    ds = ds.map(LowerCase()).map(UpperCase()).map(Trimming()).map(YearExtraction())

    # --- Sink: Save Transformed Data ---
    ds.add_sink(MongoSink(collection_name='messages_transformed', config=config))

    # --- WINDOWED AGGREGATION: per user, 1-min tumbling window ---    
    user_stats = (
        ds.key_by(lambda v: v['user_id'])
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .apply(UserStats(), output_type=None)
    )
    user_stats.add_sink(MongoSink(collection_name='user_stats', config=config))

    country_stats = (
        ds.key_by(lambda v: v['country'])
        .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
        .apply(UniqueUsers(), output_type=None)
    )
    country_stats.add_sink(MongoSink(collection_name='country_stats', config=config))

    env.execute("Streaming Flink ETL")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: flink run -py src/streaming_flink_etl.py config/etl_config.json")
        exit(1)
    main(sys.argv[1])
