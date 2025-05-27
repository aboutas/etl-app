from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import json
from transformer import Transformer
from schema_handler import schema_registry

def main(config_path):
    # Load config+rules (from one json for now)
    with open(config_path) as f:
        job = json.load(f)
        config = job["config"]
        rules_plan = job["rules_plan"]

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Tune as needed

    kafka_props = {
        'bootstrap.servers': config.get("kafka_broker", "kafka:9092"),  # fallback to default
        'group.id': f'flink-etl-{config["topic"]}'
    }

    # Add data source (Kafka topic with your data)
    consumer = FlinkKafkaConsumer(
        topics=config["topic"],
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    ds = env.add_source(consumer)

    def flink_transform(value):
        transformer = Transformer(
            schema_manager=schema_registry,
            selected_rules=rules_plan,
            config=config
        )
        return transformer.map(value)  # Returns JSON string

    transformed = ds.map(flink_transform)

    # Write to MongoDB inside map (calls your existing insert_into_mongo)
    from mongodb import insert_into_mongo
    def write_to_mongo(json_str):
        doc = json.loads(json_str)
        insert_into_mongo(doc, config["app_data_collection"], config)
        return ""  # Flink needs a return

    transformed.map(write_to_mongo)

    env.execute(f"Flink ETL for {config['topic']}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python flink_etl_job.py /opt/flink/etl_app/config/config_and_rules.json")
        exit(1)
    main(sys.argv[1])
