import json
import re
from pyflink.datastream.functions import MapFunction
from schema_manager import SchemaManager  # Import SchemaManager correctly


class RuleManagerTransform(MapFunction):
    """
    A MapFunction that dynamically transforms input data based on rules and schema versions.

    Attributes:
        schema_manager (SchemaManager): An instance of SchemaManager to retrieve schemas.
        rules_registry (dict): Dictionary of transformation rules categorized by type.
    """

    LOG_FILE = "/opt/flink/output/log.txt"  # Define log file location
    

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
                "standardize_format": lambda data: {k.lower().strip(): v for k, v in data.items()}
            },
            "data_aggregation": {
                "summarization": lambda data: {"total_sum": sum(v for v in data.values() if isinstance(v, (int, float)))}
            },
            "data_filtering": {
                "row_filtering": lambda data: data if any(isinstance(v, (int, float)) and v > 100 for v in data.values()) else None,
                "column_filtering": lambda data: {k: v for k, v in data.items() if isinstance(v, (int, str))}
            },
            "data_standardization": {
                "renaming_columns": lambda data, rename_map={}: {rename_map.get(k, k): v for k, v in data.items()},
                "capitalization_rules": lambda data: {k: v.upper() if isinstance(v, str) else v for k, v in data.items()}
            },
            "data_validation": {
                "range_checks": lambda data: {k: v for k, v in data.items() if isinstance(v, (int, float)) and 0 <= v <= 10000}
            },
            "data_transformation": {
                "type_conversion": lambda data: {
                    k: float(v) if isinstance(v, str) and v.replace('.', '', 1).isdigit() else v
                    for k, v in data.items()
                },
                "normalization": lambda data: {
                    k: v / max(1, max(data.values())) if isinstance(v, (int, float)) else v  # Avoid division by zero
                    for k, v in data.items()
                },
                "denormalization": lambda data: {
                    **data,
                    "full_address": ", ".join(filter(None, [data.get("street", ""), data.get("city", "")]))
                }
            },
            "text_manipulation": {
                "trimming": lambda data: {k: v.strip() if isinstance(v, str) else v for k, v in data.items()},
                "regex_operations": lambda data: {
                    f"{k}_digits": re.findall(r'\d+', v) if isinstance(v, str) else v for k, v in data.items()
                }
            },
            "time_transformations": {
                "date_extraction": lambda data: {
                    f"{k}_year": v[:4] for k, v in data.items()
                    if isinstance(v, str) and re.match(r"\d{4}-\d{2}-\d{2}", v)
                }
            },
            "anonymization": {
                "data_masking": lambda data: {
                    k: f"XXXX-{str(v)[-4:]}" if "id" in k.lower() and isinstance(v, (str, int)) else v
                    for k, v in data.items()
                },
                "tokenization": lambda data: {
                    k: hash(v) if isinstance(v, (str, int)) else v for k, v in data.items()
                }
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

    def map(self, value):
        """
        Transforms input JSON data based on dynamic rules and schema.

        Args:
            value (str): Input JSON data as a string.

        Returns:
            str: Transformed JSON data as a string.
        """
        try:
            input_data = json.loads(value)
            output_data = input_data.copy()  # Preserve original structure
            applied_rules = []
            
            # Generate a generic identifier (hash of the input data)
            input_id = hash(json.dumps(input_data, sort_keys=True))

            for category, transformations in self.selected_rules.items():
                if category in self.rules_registry:
                    for rule_name, columns in transformations.items() if isinstance(transformations, dict) else [(rule, None) for rule in transformations]:
                        if rule_name in self.rules_registry[category]:
                            transformation_func = self.rules_registry[category][rule_name]

                            # Apply transformation based on column specification
                            if isinstance(columns, list):  # Apply to specific columns
                                for col in columns:
                                    if col in output_data:
                                        transformed = transformation_func({col: output_data[col]})
                                        if isinstance(transformed, dict):  # Ensure we update safely
                                            output_data.update(transformed)
                                            applied_rules.append(f"{category}.{rule_name} ({col})")
                            else:  # Apply transformation to the full dataset
                                transformed_data = transformation_func(output_data.copy())  # Work on a copy to avoid mutation issues
                                if isinstance(transformed_data, dict):
                                    output_data.update(transformed_data)
                                    applied_rules.append(f"{category}.{rule_name}")

            if applied_rules:
                self.log_applied_rules(input_id, applied_rules)

            return json.dumps(output_data)

        except Exception as e:
            error_message = f"Error processing record: {e}"
            self.log_applied_rules("ERROR", [error_message])
            return json.dumps({"error": str(e)})