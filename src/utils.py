from pyflink.datastream.functions import TimestampAssigner, WatermarkStrategy

def assign_timestamps(ds):
    class TSAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return int(value['timestamp']) * 1000  # ms
    return ds.assign_timestamps_and_watermarks(
        WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(TSAssigner())
    )
