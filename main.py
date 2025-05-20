from pyflink.datastream import StreamExecutionEnvironment
from transformer import Transformer
from api_client import fetch_data_from_api 
import logging, json

from helpers import load_config
from schema_handler import schema_registry

# Used for test reason. Have to declare in dockerFile
if __name__ == "__main__":
    try:
        config = load_config()
        verbosity = config.get("verbosity", 0)
        rules_plan = config["rules_plan"]
        url = config.get("url")
        api_key = config.get("api_key")

        logging.basicConfig(level=logging.INFO if verbosity else logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

        json_inputs = fetch_data_from_api(url, api_key)
        if not json_inputs:
            logging.info("API returned empty input data.")
            exit(0)

        logging.info("Reading from API")
        schema_version = schema_registry.handle_schema_from_input(json_inputs, source="open_aq_data")

        with open(rules_plan, 'r') as f:
            rules_plan = json.load(f)

        env = StreamExecutionEnvironment.get_execution_environment()
        data_stream = env.from_collection([json.dumps(item) for item in json_inputs])

        rule_manager = Transformer(schema_manager=schema_registry,selected_rules=rules_plan,verbose=verbosity,schema_version=schema_version)

        transformed_stream = data_stream.map(rule_manager)
        env.execute("ETL Pipeline: Transform JSON and Store in MongoDB")

        logging.info("Transformations completed successfully and data stored in MongoDB.")

    except Exception as e:
        logging.error(f"Failed to execute pipeline: {e}")
