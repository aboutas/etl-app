import json
import os
from typing import Any, Callable
from pymongo import MongoClient
import json, os
from typing import Any
import time

def initialize_rules() -> dict[str, dict[str, Callable]]:
    from transformations import Transformations  # local import to avoid circular issues
    return {
        "data_cleaning": {
            "standardize_format": Transformations.standardize_format
        },
        "data_aggregation": {
            "summarization": Transformations.summarization
        },
        "data_standardization": {
            "renaming_columns": Transformations.renaming_columns,
            "capitalization_rules": Transformations.capitalization_rules
        },
        "data_validation": {
            "range_checks": Transformations.range_checks
        },
        "data_transformation": {
            "type_conversion": Transformations.type_conversion,
            "normalization": Transformations.normalization,
            "denormalization": Transformations.denormalization
        },
        "text_manipulation": {
            "trimming": Transformations.trimming,
            "regex_operations": Transformations.regex_operations
        },
        "time_transformations": {
            "date_extraction": Transformations.date_extraction
        },
        "anonymization": {
            "data_masking": Transformations.data_masking
        }
    }

def log_message(verbose: int, message: str) -> None:
    if verbose == 1:
        print(message)

def extract_id(input_data: dict) -> tuple[str, Any]:
    id_keys = [key for key in input_data.keys() if "id" in key.lower()]
    selected_key = min(id_keys, key=len) if id_keys else None
    if selected_key and selected_key in input_data:
        return selected_key, input_data[selected_key]
    return "id", hash(json.dumps(input_data, sort_keys=True))

def log_applied_rules(input_id: Any, applied_rules: list[str], transformation_times: list[str]) -> dict:
    return {
        "input_id": input_id,
        "applied_rules": applied_rules or ["None"],
        "transformation_times": transformation_times or ["N/A"],
        "logged_at": time.strftime("%Y-%m-%d %H:%M:%S")
    }

def insert_into_mongo(data: dict, collection_name: str, database_name: str = "transformed_data") -> Union[None, Exception]:
    try:
        client = MongoClient("mongodb://root:password@mongo:27017", serverSelectionTimeoutMS=5000)
        client.admin.command("ping")

        database = client[database_name]

        if collection_name not in database.list_collection_names():
            database.create_collection(collection_name)

        collection = database[collection_name]
        collection.insert_one(data)

        print(f"Inserted into MongoDB collection: {collection_name}")
    except Exception as e:
        print(f"MongoDB insert failed: {e}")
        return e

def load_config(config_path: str = "config.json") -> dict:
    """
    Loads configuration settings from a JSON file.
    
    Parameters:
        config_path (str): The file path to the configuration JSON file. Defaults to 'config.json'.
    
    Returns:
        dict: The configuration settings loaded from the JSON file.
    """
    with open(config_path, "r") as config_file:
        return json.load(config_file)

def ensure_directory_exists(path: str) -> None:
    """
    Ensures that the specified directory exists, creating it if necessary.
    
    Parameters:
        path (str): The path to the directory to check and potentially create.
    """
    os.makedirs(path, exist_ok=True)

def save_to_file(data, save_path: str) -> None:
            """Writes the transformed JSON data to an output file."""
            data = json.loads(data) 
            with open(save_path, 'a') as file:
                json.dump(data, file, indent=4)
                file.write('\n')

def log_message(verbose, message):
    """
    Prints a message if verbosity is enabled.
    """
    if verbose > 0:
        print(message)