import json
import uuid 
from pyflink.datastream.functions import MapFunction
from schema_handler import SchemaHandler  
from tranformations import *
import time


class RuleManagerTransform(MapFunction):
    """
    A MapFunction that dynamically transforms input data based on rules and schema versions.

    Attributes:
        schema_manager (SchemaManager): An instance of SchemaManager to retrieve schemas.
        rules_registry (dict): Dictionary of transformation rules categorized by type.
    """

    LOG_FILE = "/opt/flink/output/log.txt"  
    
    def __init__(self, schema_manager, selected_rules):
        self.schema_manager = schema_manager
        self.rules_registry = self.initalize_rules()  
        self.selected_rules = selected_rules  
        
    def load_selected_rules(self):
        """Loads selected transformation rules from a JSON file."""
        try:
            with open("/opt/flink/app/selected_rules.json", 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading rules: {e}")
            return {}
       
    def initalize_rules(self):
        # Define transformation rules by category
        return {
            "data_cleaning": {
                "standardize_format": standardize_format
            },
            "data_filtering": {
                "row_filtering": row_filtering,
                "column_filtering": column_filtering
            },
            "data_transformation": {
                "type_conversion": type_conversion
            },
            "anonymization": {
                "tokenization": tokenization
            }
        }
   
    def log_applied_rules(self, input_id, applied_rules):
        """
        Logs the applied rules into a file.

        Args:
            input_id (str): Unique identifier from input data.
            applied_rules (list): List of applied transformation rules.
        """
        try:
            with open(self.LOG_FILE, "a") as log_file:
                log_entry = f"ID: {input_id} | Applied Rules: {', '.join(applied_rules)}\n"
                log_file.write(log_entry)
        except Exception as e:
            print(f"Error writing to log file: {e}")

    def extract_id(self, input_data):
        id_keys = [key for key in input_data.keys() if "id" in key.lower()]
        selected_key = min(id_keys, key=len) if id_keys else None

        # If we find an ID key, return it and its value
        if selected_key and selected_key in input_data:
            return selected_key, input_data[selected_key]

        # If no ID key is found, return a generic key with a hash as the value
        return "id", hash(json.dumps(input_data, sort_keys=True))

    def map(self, value):
        try:
            start_time = time.time()
            transformation_times = []

            input_data = json.loads(value)
            output_data = input_data.copy()
            applied_rules = []

            result = self.extract_id(input_data)
            print(f"Extract ID result: {result}")
            if not isinstance(result, tuple) or len(result) != 2:
                raise ValueError(f"Unexpected return from extract_id: {result}") 
            id_key, input_id = result

            for category, transformations in self.selected_rules.items():
                if category in self.rules_registry:
                    for rule_name, fields in transformations.items():
                        if rule_name in self.rules_registry[category]:
                            transformation_func = self.rules_registry[category][rule_name]

                            valid_fields = [field for field in fields if field in output_data]
                            if valid_fields:
                                t_start = time.time()
                                transformed_data, _ = transformation_func(output_data, valid_fields)
                                t_end = time.time()

                                time_taken = t_end - t_start
                                transformation_times.append(f"{rule_name}: {time_taken:.4f} sec")  # Store execution time

                                output_data.update(transformed_data)
                                applied_rules.append(f"{category}.{rule_name} ({', '.join(valid_fields)})")
                                
            # Apply filtering LAST
            # for category, transformations in self.selected_rules.items():
            #     if category == "data_filtering":
            #         for rule_name, fields in transformations.items():
            #             if rule_name in self.rules_registry[category]:
            #                 filtering_func = self.rules_registry[category][rule_name]
            #                 _, rule_filtered_out = filtering_func(output_data, fields)

            #                 if rule_filtered_out:
            #                     applied_rules.append(f"{category}.{rule_name} - FILTERED OUT")
            #                     self.log_applied_rules(input_id, applied_rules)  
            #                     return json.dumps({"customer_id": input_id, "filtered_out": True})
            
            self.log_applied_rules(input_id, applied_rules or ["None"])  
            end_time = time.time()
            print(f"Total map() execution time: {end_time - start_time:.4f} sec") 
             
            return json.dumps(output_data)

        except Exception as e:
            error_message = f"Error processing record: {e}"
            self.log_applied_rules("ERROR", [error_message])  

            return json.dumps({"error": str(e), id_key if id_key else "id": "UNKNOWN"})
