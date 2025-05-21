from typing import Any, Dict
from helpers import flatten_dict
from mongodb import save_schema_to_mongo

class SchemaHandler:
    def __init__(self):
        self.schemas = {}

    def register_schema(self, source: str, schema_version: int, schema: Dict[str, Any]):
        self.schemas[(source, schema_version)] = schema    

    def get_next_version(self, source: str) -> int:
        versions = [k[1] for k in self.schemas if k[0] == source]
        return max(versions) + 1 if versions else 1

    def handle_schema_from_input(self,data: list[dict], source: str) -> int:
        flattened_sample = flatten_dict(data[0])
        schema = {'fields': list(flattened_sample.keys())}
        version = self.get_next_version(source)
        self.register_schema(source, version, schema)
        save_schema_to_mongo(source, version, schema)
        return version

schema_registry = SchemaHandler()

