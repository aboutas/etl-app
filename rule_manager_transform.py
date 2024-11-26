from pyflink.datastream.functions import MapFunction
import json
from schema_manager import SchemaManager  # Import SchemaManager correctly


class RuleManagerTransform(MapFunction):
    """
    A MapFunction that dynamically transforms input data based on rules and schema versions.

    Attributes:
        schema_manager (SchemaManager): An instance of SchemaManager to retrieve schemas.
    """
    def __init__(self, schema_manager=SchemaManager()):
    # Define transformation rules
        self.schema_manager = schema_manager
        
        self.rules_registry = {
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
        applied_rules = []

        # # Retrieve the latest schema
        # try:
        #     schema = self.schema_manager.get_latest_schema('input_json')
        # except ValueError as e:
        #     # Return an error message if no schema is found
        #     return json.dumps({"error": str(e)})

        # Extract fields from the schema
        # schema_fields = schema.get('fields', [])

        for rule_name, rule in self.rules_registry.items():
            if "function" in rule:
                # Execute the rule and store the output
                input_data[f"output_{rule_name}"] = rule["function"](input_data)
                applied_rules.append(rule_name)

                # Add metadata about applied rules
        input_data["applied_rules"] = applied_rules  
                # Add metadata about applied rules
        return json.dumps(input_data)