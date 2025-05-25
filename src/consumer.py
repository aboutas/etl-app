from kafka import KafkaConsumer
import json, threading
from transformer import Transformer
from schema_handler import schema_registry

CONFIG_TOPIC = "config_topic"
KAFKA_BROKER = "kafka:9092"

def process_job(config, rules_plan):
    topic = config["topic"]
    verbosity = config.get("verbosity", 0)
    # Pass config directly to Transformer!
    transformer = Transformer(schema_manager=schema_registry,selected_rules=rules_plan, config=config,verbose=verbosity)

    consumer = KafkaConsumer(topic, bootstrap_servers=KAFKA_BROKER,auto_offset_reset='earliest', value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id=f"etl_consumer_{topic}")
    print(f"Consumer started for topic: {topic}")
    for message in consumer:
        raw_item = message.value
        # No need to dump to JSON and re-parse—just pass the dict
        transformed_json = transformer.map(json.dumps(raw_item))
        print("Transformed:", transformed_json)

if __name__ == "__main__":
    consumer = KafkaConsumer(CONFIG_TOPIC, bootstrap_servers=KAFKA_BROKER, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="etl_consumer_manager")
    print("Consumer manager listening for new configs...")
    for msg in consumer:
        job = msg.value
        config = job["config"]
        rules_plan = job["rules_plan"]
        # Always pass config and rules_plan to each thread
        threading.Thread(target=process_job, args=(config, rules_plan), daemon=True).start()
    
