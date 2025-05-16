from pyflink.datastream import StreamExecutionEnvironment
from rule_manager import RuleManagerTransform
import schema_handler
import logging
import os
import json
from file_helpers import ensure_directory_exists, load_config, save_to_file
from pymongo import MongoClient
from pymongo_get_database import get_database

def get_database(): 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = "mongodb+srv://user:pass@cluster.mongodb.net/myFirstDatabase"
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
   # Create the database for our example (we will use the same database throughout the tutorial
   return client['straming_etl']

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == "__main__":
    try:
        config = load_config()
        # Extract the verbosity, log file path, and save path from the config
        verbosity = config.get("verbosity", 0)
        input_data_path = config["input_data_path"]
        save_path = config["save_path"]
        applied_rules_path = config["applied_rules_path"]

        # Get the database
        dbname = get_database()
        
        # Ensure the directory where parsed data will be saved exists
        ensure_directory_exists(os.path.dirname(save_path))
        # Load the configuration settings from a file
        env = StreamExecutionEnvironment.get_execution_environment()

        with open(input_data_path, 'r') as file:
            json_inputs = json.load(file)
            
        with open(applied_rules_path, 'r') as file:
            selected_rules = json.load(file)

        rule_manager_transform = RuleManagerTransform(schema_handler.schema_registry, selected_rules , verbosity)

        data_stream = env.from_collection([json.dumps(item) for item in json_inputs])
        transformed_data = data_stream.map(rule_manager_transform)

        transformed_data.map(lambda data: save_to_file(data, save_path))
        transformed_data = dbname["transformed_data"]    

        env.execute("Dynamic JSON to File with Flink")
        logging.info("Transforamtions completed saccesfully! The resulst has store in a json file.")
    except Exception as e:
        # Log any errors that occur during the script execution
        logging.error(f"Failed to execute script: {e}")