class SchemaManager:
    """
    A class to manage and retrieve schemas dynamically for different data sources.

    The `SchemaRegistry` allows for registering schemas with versioning and provides 
    methods to retrieve the latest schema for a specific source. This is useful in 
    scenarios where data format evolves over time, and the transformation logic needs 
    to adapt dynamically.

    Attributes:
        schemas (dict): A dictionary that stores schema versions. The key is a tuple 
                        (source, schema_version), and the value is the schema itself.

    Methods:
        register_schema(source, schema_version, schema):
            Registers a new schema for a specific source and schema version.
        
        get_latest_schema(source):
            Retrieves the latest schema version for a given source based on the 
            highest schema_version registered.
    
    Example:
        schema_registry = SchemaRegistry()
        schema_registry.register_schema('input_json', 1, {'fields': ['temperature', 'humidity']})
        latest_schema = schema_registry.get_latest_schema('input_json')
    """

    def __init__(self):
        """
        Initializes the SchemaRegistry with an empty dictionary to store schemas.
        """
        self.schemas = {}

    def register_schema(self, source, schema_version, schema):
        """
        Registers a schema for a specific source and schema version.

        Args:
            source (str): The name of the data source (e.g., 'input_json').
            schema_version (int): The version number of the schema.
            schema (dict): A dictionary representing the schema (e.g., the fields it contains).

        Example:
            schema_registry.register_schema('input_json', 1, {'fields': ['temperature', 'humidity']})
        """
        self.schemas[(source, schema_version)] = schema

    def get_latest_schema(self, source):
        """
        Retrieves the latest schema for a given source based on the highest schema version.

        Args:
            source (str): The name of the data source (e.g., 'input_json').

        Returns:
            dict: The latest schema for the specified source.

        Raises:
            ValueError: If no schemas are found for the given source.

        Example:
            latest_schema = schema_registry.get_latest_schema('input_json')
        """
        schemas_for_source = {}
        for k, v in self.schemas.items():
            if k[0] == source:
                schemas_for_source[k] = v

        if not schemas_for_source:
            raise ValueError(f"No schemas found for source: {source}")

        # Find the maximum schema version key without lambda
        latest_version = None
        for key in schemas_for_source.keys():
            if latest_version is None or key[1] > latest_version[1]:
                latest_version = key

        return self.schemas[latest_version]

#Mock schema registry and register dynamic schemas 
schema_registry = SchemaManager()
schema_registry.register_schema('input_json', 1, {'fields': ['customer id', 'cost']})
schema_registry.register_schema('input_json', 2, {'fields': ['customer id', 'cost', 'consumption', 'name']})
schema_registry.register_schema('input_json', 3, {'fields': ['customer id', 'cost', 'consumption', 'last_name', 'extra_column']})
schema_registry.register_schema('input_json', 4, {'fields': ['customer id', 'cost', 'consumption', 'last_name', 'street', 'city']})


