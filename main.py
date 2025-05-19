from pyflink.datastream import StreamExecutionEnvironment
from transformer import Transformer
from schema_handler import schema_registry, handle_schema_from_input
from api_client import fetch_data_from_api 
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
        applied_rules_path = config["applied_rules_path"]
        url = config.get("url")
        api_key = config.get("api_key")
        # Set up logging
        logging.basicConfig(level=logging.INFO if verbosity else logging.WARNING,format='%(asctime)s - %(levelname)s - %(message)s')

        # Load input data and selected rules
        json_inputs = fetch_data_from_api(url, api_key)
        if not json_inputs:
            logging.info("API returned empty input data.")
        else :
            logging.info("Reading from API")

        # Register schema dynamically
        schema_version = handle_schema_from_input(json_inputs, source="open_aq_data")

        with open(applied_rules_path, 'r') as f:
            selected_rules = json.load(f)

        # Initialize Flink streaming environment
        env = StreamExecutionEnvironment.get_execution_environment()

        # Create data stream from input JSONs
        data_stream = env.from_collection([json.dumps(item) for item in json_inputs])

        # Apply transformation logic
        rule_manager = Transformer(schema_manager=schema_registry,selected_rules=selected_rules,verbose=verbosity,schema_version=schema_version)       
        transformed_stream = data_stream.map(rule_manager)

        # Execute the Flink job
        env.execute("ETL Pipeline: Transform JSON and Store in MongoDB")
        logging.info("Transformations completed successfully and data stored in MongoDB.")

    except Exception as e:
        logging.error(f"Failed to execute pipeline: {e}")