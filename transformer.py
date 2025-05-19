from pyflink.datastream.functions import MapFunction
import json, time
from helpers import (initialize_rules, log_message, extract_id, log_applied_rules, insert_into_mongo, load_config, ensure_directory_exists)
import os 

class Transformer(MapFunction):
    def __init__(self, schema_manager, selected_rules, verbose: int = 0):
        self.schema_manager = schema_manager
        self.rules_registry = initialize_rules()
        self.selected_rules = selected_rules
        self.verbose = verbose

        config = load_config()
        self.log_file = config["log_file"]
        ensure_directory_exists(os.path.dirname(self.log_file))

    def map(self, value: str) -> str:
        try:
            start_time = time.time()
            transformation_times = []

            input_data = json.loads(value)
            output_data = input_data.copy()
            applied_rules = []

            id_key, input_id = extract_id(input_data)
            log_message(self.verbose, f"Extract ID result: {id_key} = {input_id}")

            for category, transformations in self.selected_rules.items():
                if category in self.rules_registry:
                    for rule_name, fields in transformations.items():
                        if rule_name in self.rules_registry[category]:
                            func = self.rules_registry[category][rule_name]
                            valid_fields = [f for f in fields if f in output_data]
                            if valid_fields:
                                start_time = time.time()
                                transformed, _ = func(output_data, valid_fields)
                                output_data.update(transformed)
                                end_time = time.time()
                                time_taken = end_time - start_time
                                applied_rules.append(f"{category}.{rule_name} ({', '.join(valid_fields)})")
                                transformation_times.append(f"{rule_name}: {time_taken:.4f} sec")
            log_message(self.verbose, f"Total map() execution: {time.time() - start_time:.4f} sec")

            log_data = log_applied_rules(input_id, applied_rules, transformation_times)
           
            insert_into_mongo(log_data, collection_name="transformation_logs")
            insert_into_mongo(output_data, collection_name="transformed_data")

            return json.dumps(output_data)

        except Exception as e:
            error_msg = f"Error processing record: {e}"
            log_applied_rules(self.log_file, "ERROR", [error_msg])
            return json.dumps({"error": str(e), id_key if 'id_key' in locals() else "id": "UNKNOWN"})
