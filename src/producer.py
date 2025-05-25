from kafka import KafkaConsumer, KafkaProducer
import json, logging, threading
from api_client import fetch_data_from_api
from schema_handler import schema_registry

CONFIG_TOPIC = "config_topic"
KAFKA_BROKER = "kafka:9092"

def process_job(config, rules_plan):
    url = config["url"]
    api_key = config["api_key"]
    topic = config["topic"]

    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    data = fetch_data_from_api(url, api_key)

    if not data:
        logging.info("API returned empty input data.")
        return
    logging.info(f"Reading from API for topic {topic}")

    schema_version = schema_registry.handle_schema_from_input(data=data, source=topic, config=config)
    
    for item in data:
        producer.send(topic, item)
        print(f"Sent to {topic}: {item.get('id')}")
    producer.flush()

if __name__ == "__main__":
    consumer = KafkaConsumer(CONFIG_TOPIC, bootstrap_servers=KAFKA_BROKER, value_deserializer=lambda m: json.loads(m.decode("utf-8")), group_id="etl_producer_manager")
    print("Producer listening for new configs...")
    for msg in consumer:
        job = msg.value
        config = job["config"]
        rules_plan = job["rules_plan"]
        # Each config handled in a new thread
        threading.Thread(target=process_job, args=(config, rules_plan), daemon=True).start()
