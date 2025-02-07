from pyflink.datastream.functions import MapFunction
import json, re


from schema_manager import SchemaManager  # Import SchemaManager correctly


class RuleManagerTransform(MapFunction):
    """
    A MapFunction that dynamically transforms input data based on rules and schema versions.

    Attributes:
        schema_manager (SchemaManager): An instance of SchemaManager to retrieve schemas.
        rules_registry (dict): Dictionary of transformation rules categorized by type.
    """

    def __init__(self, schema_manager, rules_registry=None):
        self.schema_manager = schema_manager

        # Define transformation rules by category
        self.rules_registry = rules_registry or {
            "data_cleaning": {
                "standardize_format": lambda data: {k.lower().strip(): v for k, v in data.items()},  # Standardize key names
            },
            "data_aggregation": {
                "summarization": lambda data: {"total_cost": sum(data.get("costs", []))},
            },
            "data_filtering": {
                "row_filtering": lambda data: data if data.get("cost", 0) > 100 else None,  # Example threshold filter
                "column_filtering": lambda data: {k: v for k, v in data.items() if k in ["customer_id", "cost", "consume"]},
            },
            "data_standardization": {
                "renaming_columns": lambda data: {"customerID": data.get("customer_id", ""), "totalCost": data.get("cost", 0)},
                "standardizing_units": lambda data: {"cost_in_dollars": round(data.get("cost", 0) * 1.1, 2)},
                "capitalization_rules": lambda data: {k: (v.upper() if isinstance(v, str) else v) for k, v in data.items()},
            },
            "data_validation": {
                "range_checks": lambda data: data if 0 <= data.get("cost", 0) <= 10000 else None,
            },
            "data_transformation": {
                "type_conversion": lambda data: {k: float(v) if isinstance(v, str) and v.replace('.', '', 1).isdigit() else v for k, v in data.items()},
                "normalization": lambda data: {"normalized_cost": data.get("cost", 0) / 1000},
                "denormalization": lambda data: {**data, "full_address": f"{data.get('street', '')}, {data.get('city', '')}"},
            },
            "text_manipulation": {
                "trimming": lambda data: {k: v.strip() if isinstance(v, str) else v for k, v in data.items()},
                "regex_operations": lambda data: {"extracted_digits": re.findall(r'\d+', data.get("comment", ""))},
            },
            "time_transformations": {
                "date_extraction": lambda data: {"year": data.get("timestamp", "")[:4]} if "timestamp" in data else {},
            },
            "anonymization": {
                "data_masking": lambda data: {"masked_id": f"XXXX-{str(data.get('customer_id', ''))[-4:]}"},  # Masking customer ID
                "tokenization": lambda data: {"token": hash(data.get("customer_id", ""))},
            }
        }

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

            for category, rules in self.rules_registry.items():
                for rule_name, rule_function in rules.items():
                    transformed = rule_function(input_data)
                    if transformed is not None:
                        output_data[rule_name] = transformed

            output_data["applied_rules"] = list(output_data.keys())  # Store applied rules

            return json.dumps(output_data)
        except Exception as e:
            return json.dumps({"error": str(e)})