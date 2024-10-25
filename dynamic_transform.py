from pyflink.datastream.functions import MapFunction
import json

class DynamicTransform(MapFunction):
    """
    A custom MapFunction that transforms input JSON data based on dynamic schema definitions
    from a schema registry. The transformation involves currency conversion and normalization.

    This class takes a schema registry to dynamically retrieve the latest schema for
    incoming data and applies specific transformations like:

    - Converting 'heating_cost' to euros if both 'heating_cost' and 'currency' fields are present.
    - Normalizing the 'temperature' field, if available, by dividing it by 100.
    
    Attributes:
        schema_registry (SchemaRegistry): A schema registry object that stores and retrieves 
                                          schema versions dynamically.
    
    Methods:
        map(value):
            Transforms the input JSON data based on the schema. It performs:
            - Currency conversion of heating costs to euros using exchange rates.
            - Normalization of temperature values.
            - Sets 'heating_cost_euro' to None if data is invalid or the necessary fields are missing.
        
        get_exchange_rate(currency):
            Returns the exchange rate for a given currency. Defaults to 1 if the currency is not found.
    
    Example:
        schema_registry = SchemaRegistry()
        schema_registry.register_schema('input_json', 1, {'fields': ['temperature', 'humidity']})
        transform = DynamicTransform(schema_registry)
        transformed_data = transform.map('{"temperature": "20.5", "currency": "USD"}')
    """

    def __init__(self, schema_registry):
        """
        Initializes the DynamicTransform class with a schema registry.

        Args:
            schema_registry (SchemaRegistry): An object that handles dynamic schema retrieval.
        """
        self.schema_registry = schema_registry

    def map(self, value):
        """
        Transforms input JSON data based on the latest schema.

        Args:
            value (str): The JSON string input to be transformed.

        Returns:
            str: A JSON string with transformed values like heating_cost_euro and temperature_normalized.

        Raises:
            TypeError: If 'heating_cost' is None or cannot be converted to float.
        """
        data = json.loads(value)
        schema = self.schema_registry.get_latest_schema('input_json')

   
        if 'heating_cost' in schema.get('fields') and 'currency' in schema.get('fields'):
            heating_cost = data.get('heating_cost')
            exchange_rate = get_exchange_rate(data.get('currency'))
            if heating_cost is not None:
                data['heating_cost_euro'] = round(float(heating_cost) * float(exchange_rate),2)
            else:
                data['heating_cost_euro'] = None  
        else:
            data['heating_cost_euro'] = None

        if 'temperature' in schema.get('fields'):
            data['temperature_normalized'] = round(float(data['temperature']) / 1) 

        return json.dumps(data)

def get_exchange_rate(currency):
    """
    Returns the exchange rate for a given currency. Defaults to 1 if currency is not found.

    Args:
        currency (str): The currency code (e.g., 'USD', 'GBP').

    Returns:
        float: The exchange rate for the given currency.
    """
    rates = {
        'USD': 0.85,  
        'GBP': 1.17
    }
    return rates.get(currency, 1)  
