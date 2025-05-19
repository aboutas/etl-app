from typing import Any, Dict
from collections import OrderedDict

class SchemaHandler:
    def __init__(self):
        self.schemas = {}

    def register_schema(self, source: str, schema_version: int, schema: Dict[str, Any]):
        self.schemas[(source, schema_version)] = schema

    def get_latest_schema(self, source: str) -> Dict[str, Any]:
        versions = [(k[1], v) for k, v in self.schemas.items() if k[0] == source]
        if not versions:
            raise ValueError(f"No schemas found for source: {source}")
        latest_version = max(versions, key=lambda x: x[0])[0]
        return self.schemas[(source, latest_version)]

    def get_next_version(self, source: str) -> int:
        versions = [k[1] for k in self.schemas if k[0] == source]
        return max(versions) + 1 if versions else 1

# Utility: flatten nested dicts (e.g., OpenAQ format)
def flatten_dict(d: dict[str, Any], parent_key: str = '', sep: str = '.') -> dict[str, Any]:
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):
                # Flatten only first dict of list for schema inference
                items.extend(flatten_dict(v[0], new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        else:
            items.append((new_key, v))
    return OrderedDict(items)

# Main handler function to infer schema and register
def handle_schema_from_input(data: list[dict[str, Any]], source: str) -> int:
    from .schema_handler import schema_registry  # local import to avoid circular issues

    # Flatten one sample input to infer schema fields
    flattened_sample = flatten_dict(data[0])
    schema = {'fields': list(flattened_sample.keys())}

    # Register schema
    version = schema_registry.get_next_version(source)
    schema_registry.register_schema(source, version, schema)
    return version


# Global registry instance
schema_registry = SchemaHandler()
