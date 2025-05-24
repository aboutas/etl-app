from pyflink.datastream.functions import MapFunction
import json, time, os
from helpers import initialize_rules, log_message, extract_id, log_applied_rules, flatten_dict, load_config
from mongodb import insert_into_mongo, load_schema_from_mongo
import rule_execution_order

class Transformer(MapFunction):
    def __init__(self, schema_manager, selected_rules, verbose: int = 0, schema_version: int = None):
        self.schema_manager = schema_manager
        self.rules_registry = initialize_rules()
        self.selected_rules = selected_rules
        self.verbose = verbose
        self.schema_version = schema_version
        
    def map(self, value: str) -> str:
        try:
            import os
            config_path = os.environ.get("CONFIG_PATH")
            config = load_config(config_path)
            topic = config.get("topic")
            app_data_collection = config.get("app_data_collection")
            log_data_collection = config.get("log_data_collection")

            start_time = time.time()
            transformation_times = []

            input_data = json.loads(value)
            input_data = flatten_dict(input_data)
            output_data = input_data.copy()
            applied_rules = []

            execution_order = rule_execution_order.rule_exectution_order()
            schema = load_schema_from_mongo(topic)
            expected_fields = schema.get("fields", [])

            id_key, input_id = extract_id(input_data)
            log_message(self.verbose, f"Extract ID result: {id_key} = {input_id}")

            for rule in execution_order:
                found = False
                for category, transformations in self.selected_rules.items():
                    if rule in transformations and rule in self.rules_registry.get(category, {}):
                        func = self.rules_registry[category][rule]
                        if rule == "renaming_columns":
                            params = transformations[rule]
                            fields = params["fields"]
                            rename_map = params["rename_map"]
                            valid_fields = [f for f in fields if f in output_data and f in expected_fields]
                            if valid_fields and rename_map:
                                found = True
                                t_start = time.time()
                                transformed, _ = func(output_data, valid_fields, rename_map)
                                output_data = transformed
                                t_end = time.time()
                                applied_rules.append(f"{category}.{rule} ({', '.join(valid_fields)})")
                                transformation_times.append(f"{rule}: {t_end - t_start:.4f} sec")
                                print("Rule:", rule, "| Fields:", fields, "| Valid fields:", valid_fields)
                            continue

                        fields = transformations[rule]
                        valid_fields = [f for f in fields if f in output_data and f in expected_fields]
                        if valid_fields:
                            found = True
                            t_start = time.time()
                            if rule == "concatination":
                                transformed, _ = func(output_data, valid_fields, " ")
                            else:
                                transformed, _ = func(output_data, valid_fields)

                            if rule == "range_checks":
                                if not all(f in transformed for f in valid_fields):
                                    log_message(1, f"Range check failed: {valid_fields}, skipping record.")
                                    log_data = log_applied_rules(input_id, applied_rules, transformation_times)
                                    insert_into_mongo(flatten_dict(log_data), log_data_collection)
                                    return None

                            elif rule == "summarization" or rule == "concatination":
                                for k, v in transformed.items():
                                    if k not in output_data:
                                        output_data[k] = v
                            else:
                                output_data.update(transformed)

                            t_end = time.time()
                            applied_rules.append(f"{category}.{rule} ({', '.join(valid_fields)})")
                            transformation_times.append(f"{rule}: {t_end - t_start:.4f} sec")
                            print("Rule:", rule, "| Fields:", fields, "| Valid fields:", valid_fields)
                if not found:
                    pass

            log_message(self.verbose, f"Total map() execution: {time.time() - start_time:.4f} sec")

            log_data = log_applied_rules(input_id, applied_rules, transformation_times)
            insert_into_mongo(flatten_dict(log_data), log_data_collection)
            insert_into_mongo(output_data, app_data_collection)
            return json.dumps(output_data)

        except Exception as e:
            error_msg = f"Error processing record: {e}"
            log_message(1, error_msg)
            error_log = log_applied_rules("ERROR", [error_msg], {})
            insert_into_mongo(error_log, log_data_collection)
            return json.dumps({"error": str(e), "id": input_data.get("id", "UNKNOWN")})



