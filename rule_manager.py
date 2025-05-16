from pyflink.datastream.functions import MapFunction
from transformations import Transformations
import json, time, os
from file_helpers import ensure_directory_exists, load_config
from pyflink.datastream.functions import MapFunction
class RuleManagerTransform(MapFunction):
    """
    A MapFunction that dynamically transforms input data based on user-defined rules and schema versions.
    """
    
    def __init__(self, schema_manager, selected_rules, verbose: int = 0):
        self.schema_manager = schema_manager
        self.rules_registry = self.initialize_rules()  
        self.selected_rules = selected_rules  
        self.verbose = verbose

        config = load_config()
        self.log_file = config["log_file"]
        ensure_directory_exists(os.path.dirname(self.log_file))
    
    def log(self, message: str) -> None:
        """
        Logs a message to the console, depending on the verbosity level.
        
        Args:
            message (str): The message to log.
        """
        if self.verbose == 1:
            print(message)

    def initialize_rules(self):
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
    
    def logging_rules(self, input_id : int, applied_rules):
        """
        Logs the applied rules into a file.

        """
        if self.verbose == 1:
            try:
                with open(self.log_file, "a") as log_file:
                    log_entry = f"Id: {input_id} | Applied Rules: {', '.join(applied_rules)}\n"
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
                                            
            self.logging_rules(input_id, applied_rules or ["None"])  
                                
            
            
            end_time = time.time()
            print(f"Total map() execution time: {end_time - start_time:.4f} sec") 
             
            return json.dumps(output_data)

        except Exception as e:
            error_message = f"Error processing record: {e}"
            self.logging_rules("ERROR", [error_message])  

            return json.dumps({"error": str(e), id_key if id_key else "id": "UNKNOWN"})

