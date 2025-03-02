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
        
    def load_selected_rules(self):
        """Loads selected transformation rules from a JSON file."""
        try:
            with open("/opt/flink/app/selected_rules.json", 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading rules: {e}")
            return {}
       
    def initalize_rules(self):
        # Exchange rates dictionary
        self.exchange_rates = {
            'USD': 0.85,
            'GBP': 1.17
        }
        
        def get_exchange_rate(currency):
            """
            Returns the exchange rate for a given currency. Defaults to 1 if currency is not found.
            """
            return self.exchange_rates.get(currency, 1)

        # Define transformation rules by category
        return {
            "data_cleaning": {
                "standardize_format": lambda data: {k.lower().strip(): v for k, v in data.items()},
            },
            "data_aggregation": {
                "summarization": lambda data: {"total_sum": sum(v for v in data.values() if isinstance(v, (int, float)))},
            },
            "data_filtering": {
                "row_filtering": lambda data: data if any(isinstance(v, (int, float)) and v > 100 for v in data.values()) else None,
                "column_filtering": lambda data: {k: v for k, v in data.items() if isinstance(v, (int, str))},  
            },
            "data_standardization": {
                "renaming_columns": lambda data, rename_map={"old_name": "new_name"}: {rename_map.get(k, k): v for k, v in data.items()},
                "capitalization_rules": lambda data: {k: v.upper() if isinstance(v, str) else v for k, v in data.items()},
            },
            "data_validation": {
                "range_checks": lambda data: {k: v for k, v in data.items() if isinstance(v, (int, float)) and 0 <= v <= 10000},
            },
            "data_transformation": {
                "type_conversion": lambda data: {k: (float(v) if isinstance(v, str) and v.replace('.', '', 1).isdigit() else v) for k, v in data.items()},
                "normalization": lambda data: {k: v / max(data.values()) if isinstance(v, (int, float)) else v for k, v in data.items()},
                "denormalization": lambda data: {**data, "full_address": ", ".join(filter(None, [data.get(k, "") for k in ["street", "city"]]))},
            },
            "text_manipulation": {
                "trimming": lambda data: {k: v.strip() if isinstance(v, str) else v for k, v in data.items()},
                "regex_operations": lambda data: {k: re.findall(r'\d+', str(v)) if isinstance(v, str) else v for k, v in data.items()},
            },
            "time_transformations": {
                "date_extraction": lambda data: {k + "_year": v[:4] for k, v in data.items() if isinstance(v, str) and re.match(r"\d{4}-\d{2}-\d{2}", v)},
            },
            "anonymization": {
                "data_masking": lambda data: {k: f"XXXX-{str(v)[-4:]}" if isinstance(v, str) and len(v) > 4 else v for k, v in data.items()},
                "tokenization": lambda data: {k: hash(v) if isinstance(v, (str, int)) else v for k, v in data.items()},
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
            output_data = {}
            applied_rules = []
            input_id = input_data.get("customer_id", "unknown")

            for category, rules in self.selected_rules.items():
                for rule_name in rules:
                    if category in self.rules_registry and rule_name in self.rules_registry[category]:
                        transformed = self.rules_registry[category][rule_name](input_data)
                        if transformed is not None:
                            output_data.update(transformed)
                            applied_rules.append(f"{category}.{rule_name}")
            if applied_rules:
                self.log_applied_rules(input_id, applied_rules)

            return json.dumps(output_data)
        except Exception as e:
            error_message = f"Error processing record: {e}"
            self.log_applied_rules("ERROR", [error_message])
            return json.dumps({"error": str(e)})