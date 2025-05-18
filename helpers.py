import json
import os
from typing import Any, Callable
from pymongo import MongoClient
import json, os

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

def log_applied_rules(log_file: str, input_id: Any, applied_rules: list[str]) -> None:
    try:
        with open(log_file, "a") as log_file_handle:
            log_entry = f"Id: {input_id} | Applied Rules: {', '.join(applied_rules)}\n"
            log_file_handle.write(log_entry)
    except Exception as e:
        print(f"Error writing to log file: {e}")

def insert_into_mongo(data: dict) -> None:
    try:
        client = MongoClient("mongodb://root:password@mongo:27017")
        client.admin.command("ping")
        database= client["transformed_data"]
        database.create_collection("example_collection")
        collection = database["example_collection"]
        collection.insert_one(data)
        
        for x in collection.find():
            print(x)

        print("Inserted into MongoDB")
    except Exception as e:
        print(f"MongoDB insert failed: {e}")

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