from pymongo import MongoClient
from typing import Dict, Any, Optional, Union
from datetime import datetime

MONGO_URI = "mongodb://root:password@mongo:27017"
SCHEMA_DB = "etl_registry"
SCHEMA_COLLECTION = "schema_registry"
APP_DATA_COLLECTION = "etl_result"

def save_schema_to_mongo(source: str, version: int, schema: Dict[str, Any]) -> None:
    client = MongoClient(MONGO_URI)
    db = client[SCHEMA_DB]
    collection = db[SCHEMA_COLLECTION]
    collection.update_one({"source": source, "version": version}, {"$set": {"schema": schema}}, upsert=True)
    client.close()

def load_schema_from_mongo(source: str, version: Optional[int] = None) -> Dict[str, Any]:
    client = MongoClient(MONGO_URI)
    db = client[SCHEMA_DB]
    collection = db[SCHEMA_COLLECTION]
    if version is None:
        doc = collection.find({"source": source}).sort("version", -1).limit(1)
    else:
        doc = collection.find({"source": source, "version": version})
    schema_doc = next(doc, None)
    client.close()
    if schema_doc is None:
        raise ValueError(f"No schema found for source={source} version={version}")
    return schema_doc["schema"]

def insert_into_mongo(data: dict, collection_name: str, database_name: str = APP_DATA_COLLECTION ) -> Union[None, Exception]:
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[database_name]
        collection = db[collection_name]
        data["ingested_at"] = datetime.now().isoformat()

        collection.insert_one(data)
        print(f"Appended new document to '{collection_name}' at {data['ingested_at']}")

    except Exception as e:
        print(f"MongoDB insert failed: {e}")
        return e


