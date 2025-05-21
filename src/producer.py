from kafka import KafkaProducer
import json, logging
from helpers import load_config
from api_client import fetch_data_from_api
from schema_handler import schema_registry

if __name__ == "__main__":
    config = load_config()
    url = config["url"]
    api_key = config["api_key"]

    producer = KafkaProducer(bootstrap_servers="kafka:9092", value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    data = fetch_data_from_api(url, api_key)

    if not data:
            logging.info("API returned empty input data.")
            exit(0)
    logging.info("Reading from API")
    schema_version = schema_registry.handle_schema_from_input(data=data, source="open_aq_data")

    for item in data:
        producer.send("openaq-data", item)
        print(f"Sent: {item.get('id')}")
    producer.flush()

            
