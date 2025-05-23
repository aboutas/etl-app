from pyflink.datastream.functions import MapFunction
import json, time, os
from helpers import initialize_rules, log_message, extract_id, log_applied_rules, flatten_dict, load_config
from mongodb import insert_into_mongo, load_schema_from_mongo

class Transformer(MapFunction):
    def __init__(self, schema_manager, selected_rules, verbose: int = 0, schema_version: int = None):
        self.schema_manager = schema_manager
        self.rules_registry = initialize_rules()
        self.selected_rules = selected_rules
        self.verbose = verbose
        self.schema_version = schema_version
        
    def map(self, value: str) -> str:
        try:
            config_path = os.environ.get("CONFIG_PATH")
            config = load_config(config_path)
            topic = config.get("topic")

            start_time = time.time()
            transformation_times = []

            input_data = json.loads(value)
            input_data = flatten_dict(input_data)
            output_data = input_data.copy()
            applied_rules = []

            
            schema = load_schema_from_mongo(topic)
            expected_fields = schema.get("fields", [])

            id_key, input_id = extract_id(input_data)
            log_message(self.verbose, f"Extract ID result: {id_key} = {input_id}")

            for category, transformations in self.selected_rules.items():
                if category in self.rules_registry:
                    for rule_name, fields in transformations.items():
                        if rule_name in self.rules_registry[category]:
                            func = self.rules_registry[category][rule_name]
                            valid_fields = []
                            for f in fields:
                                if f in output_data and f in expected_fields:
                                    valid_fields.append(f)

                            if valid_fields:
                                t_start = time.time()
                                transformed, _ = func(output_data, valid_fields)
                                output_data.update(transformed)
                                t_end = time.time()
                                applied_rules.append(f"{category}.{rule_name} ({', '.join(valid_fields)})")
                                transformation_times.append(f"{rule_name}: {t_end - t_start:.4f} sec")
                            print("Fields to transform:", fields)
                            print("Expected fields from schema:", expected_fields)
                            print("Valid fields selected:", valid_fields)

            log_message(self.verbose, f"Total map() execution: {time.time() - start_time:.4f} sec")

            log_data = log_applied_rules(input_id, applied_rules, transformation_times)
            insert_into_mongo(flatten_dict(log_data), "transformation_logs")
            insert_into_mongo(output_data, "transformed_data")

            return json.dumps(output_data)

        except Exception as e:
            error_msg = f"Error processing record: {e}"
            log_message(1, error_msg)
            error_log = log_applied_rules("ERROR", [error_msg], {})
            insert_into_mongo(error_log, "transformation_logs")
            return json.dumps({"error": str(e), "id": input_data.get("id", "UNKNOWN")})

