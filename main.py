from pyflink.datastream import StreamExecutionEnvironment
from transformer import Transformer
import schema_handler
import logging
import json
from helpers import load_config

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    try:
        # Load configuration
        config = load_config()
        verbosity = config.get("verbosity", 0)
        input_data_path = config["input_data_path"]
        applied_rules_path = config["applied_rules_path"]

        # Set up logging
        logging.basicConfig(level=logging.INFO if verbosity else logging.WARNING,format='%(asctime)s - %(levelname)s - %(message)s')

        # Load input data and selected rules
        with open(input_data_path, 'r') as f:
            json_inputs = json.load(f)

        with open(applied_rules_path, 'r') as f:
            selected_rules = json.load(f)

        # Initialize Flink streaming environment
        env = StreamExecutionEnvironment.get_execution_environment()

        # Create data stream from input JSONs
        data_stream = env.from_collection([json.dumps(item) for item in json_inputs])

        # Apply transformation logic
        rule_manager = Transformer(schema_manager=schema_handler.schema_registry,selected_rules=selected_rules,verbose=verbosity)
        transformed_stream = data_stream.map(rule_manager)

        # Execute the Flink job
        env.execute("ETL Pipeline: Transform JSON and Store in MongoDB")
        logging.info("Transformations completed successfully and data stored in MongoDB.")

    except Exception as e:
        logging.error(f"Failed to execute pipeline: {e}")