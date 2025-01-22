from pyflink.datastream.functions import MapFunction
import json
from schema_manager import SchemaManager  # Import SchemaManager correctly


class RuleManagerTransform(MapFunction):
    """
    A MapFunction that dynamically transforms input data based on rules and schema versions.

    Attributes:
        schema_manager (SchemaManager): An instance of SchemaManager to retrieve schemas.
        rules_registry
    """
    def __init__(self, schema_manager=SchemaManager(), rules_registry=None):
        
        self.schema_manager = schema_manager
         # Define transformation rules
        self.rules_registry = rules_registry or {
            "rule1": {
                "description": "Concatenate name and last name",
                "function": lambda data: f"{data.get('name', '')} {data.get('last_name', '')}"
            },
            "rule2": {
                "description": "Concatenate last name, name, and cost in euros",
                "function": lambda data: f"{data.get('customer_id', '')} {data.get('name', '')} {data.get('last_name', '')}, Cost: euro {data.get('cost', 0)}"
            },
            "rule3": {
                "description": "Calculate cost per unit",
                "function": lambda data: f"{data.get('customer_id', '')}, Cost/Unit: {round(data.get('cost', 0) / data.get('consume', 1), 2)}"
                if data.get('consume') else "N/A"
            }
        }

    def map(self, value):
        """
        Transforms input JSON data based on dynamic rules and schema.

        Args:
            value (str): Input JSON data as a string.

        Returns:
            str: Transformed JSON data as a string.
                        schema = self.schema_manager.get_latest_schema('input_json')  # Use SchemaManager's method

        """ 
        # Parse input data
        input_data = json.loads(value)
        output_data = {}
        applied_rules = []

        for rule_name, rule in self.rules_registry.items():
            if "function" in rule:
                # Execute the rule and store the output
                output_data[rule_name] = rule["function"](input_data)
                applied_rules.append(rule_name)

        output_data["applied_rules"] = applied_rules  
        # Add metadata about applied rules
        return json.dumps(output_data)