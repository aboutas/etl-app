from kafka import KafkaConsumer
import json
from transformer import Transformer
from helpers import load_config
from schema_handler import schema_registry


if __name__ == "__main__":
    config = load_config()
    rules_plan = config["rules_plan"]
    verbosity = config.get("verbosity", 0)
    with open(rules_plan, 'r') as f:
        rules_plan = json.load(f)
    
    transformer = Transformer(schema_manager=schema_registry, selected_rules=rules_plan, verbose=verbosity)
    consumer = KafkaConsumer("openaq-data", bootstrap_servers="kafka:9092", value_deserializer=lambda m: json.loads(m.decode("utf-8")))

    for message in consumer:
        raw_item = message.value
        transformed_json = transformer.map(json.dumps(raw_item))
        print("Transformed:", transformed_json)
       
