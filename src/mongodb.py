from pymongo import MongoClient
from typing import Dict, Any, Optional, Union
from datetime import datetime
from helpers import load_config
import os

config_path = os.environ.get("CONFIG_PATH", "/opt/flink/etl_app/config/config.json")
mongo_uri = os.environ.get("MONGO_URI", "mongodb://root:password@mongo:27017")
config = load_config(config_path)
database = config["database"] 
schema_collection = config["schema_collection"]
app_data_collection = config["app_data_collection"]

def save_schema_to_mongo(source: str, version: int, schema: Dict[str, Any]) -> None:
    client = MongoClient(mongo_uri)
    db = client[database]
    collection = db[schema_collection]
    collection.update_one({"source": source, "version": version}, {"$set": {"schema": schema}}, upsert=True)
    client.close()

def load_schema_from_mongo(source: str, version: Optional[int] = None) -> Dict[str, Any]:
    client = MongoClient(mongo_uri)
    db = client[database]
    collection = db[schema_collection]
    if version is None:
        doc = collection.find({"source": source}).sort("version", -1).limit(1)
    else:
        doc = collection.find({"source": source, "version": version})
    schema_doc = next(doc, None)
    client.close()
    if schema_doc is None:
        raise ValueError(f"No schema found for source={source} version={version}")
    return schema_doc["schema"]

def insert_into_mongo(data: dict, collection_name: str, database_name: str = schema_collection ) -> Union[None, Exception]:
    try:
        client = MongoClient(mongo_uri)
        client.admin.command("ping")
        db = client[app_data_collection]
        collection = db[collection_name]
        data["ingested_at"] = datetime.now().isoformat()

        collection.insert_one(data)
        print(f"Appended new document to '{collection_name}' at {data['ingested_at']}")

    except Exception as e:
        print(f"MongoDB insert failed: {e}")
        return e

