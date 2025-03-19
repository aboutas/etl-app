import json
import uuid 
from pyflink.datastream.functions import MapFunction
from schema_handler import SchemaHandler  
from tranformations import *




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
                "standardize_format": standardize_format
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
            missing_columns = []

            # Generate a stable ID based on customer_id (or fallback to UUID)
            input_id = str(input_data.get("customer_id", uuid.uuid4().int))  # Ensure unique ID per row

            for category, transformations in self.selected_rules.items():
                if category in self.rules_registry:
                    for rule_name, columns in transformations.items() if isinstance(transformations, dict) else [(rule, None) for rule in transformations]:
                        if rule_name in self.rules_registry[category]:
                            transformation_func = self.rules_registry[category][rule_name]

                            if isinstance(columns, list):  # Apply to specific columns
                                valid_columns = [col for col in columns if col in output_data]
                                missing_cols = [col for col in columns if col not in output_data]

                                if valid_columns:  # Only apply if relevant columns exist
                                    transformed = transformation_func(output_data, valid_columns)
                                    if isinstance(transformed, dict):
                                        output_data.update(transformed)
                                        applied_rules.append(f"{category}.{rule_name} ({', '.join(valid_columns)})")

                                if missing_cols:  # Store missing columns for later logging
                                    missing_columns.append(f"{category}.{rule_name} - Missing: {', '.join(missing_cols)}")

                            else:  # Apply transformation to the full dataset
                                transformed_data = transformation_func(output_data.copy(), [])
                                if isinstance(transformed_data, dict):
                                    output_data.update(transformed_data)
                                    applied_rules.append(f"{category}.{rule_name}")

            # Ensure "None" is logged when no transformations were applied
            applied_rules_str = ", ".join(applied_rules) if applied_rules else "None"
            missing_columns_str = " | ".join(missing_columns) if missing_columns else ""

            # Log applied rules + missing columns together
            log_entry = f"ID: {input_id} | Applied Rules: {applied_rules_str}"
            if missing_columns_str:
                log_entry += f" | {missing_columns_str}"

            self.log_applied_rules(input_id, [log_entry])

            return json.dumps(output_data)

        except Exception as e:
            error_message = f"Error processing record: {e}"
            self.log_applied_rules("ERROR", [error_message])
            return json.dumps({"error": str(e)})
