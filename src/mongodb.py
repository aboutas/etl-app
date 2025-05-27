# mongodb.py
from pymongo import MongoClient
from typing import Dict, Any, Optional, Union
from datetime import datetime
#pythhonApp/Thread logic
def get_mongo_client(mongo_uri: str) -> MongoClient:
    return MongoClient(mongo_uri)

def save_schema_to_mongo(source: str, version: int, schema: Dict[str, Any], config: Dict) -> None:
    client = get_mongo_client(config["mongo_uri"])
    db = client[config["database"]]
    collection = db[config["schema_collection"]]
    collection.update_one({"source": source, "version": version}, {"$set": {"schema": schema}}, upsert=True)
    client.close()

def load_schema_from_mongo(source: str, config: Dict, version: Optional[int] = None) -> Dict[str, Any]:
    client = get_mongo_client(config["mongo_uri"])
    db = client[config["database"]]
    collection = db[config["schema_collection"]]
    if version is None:
        doc = collection.find({"source": source}).sort("version", -1).limit(1)
    else:
        doc = collection.find({"source": source, "version": version})
    schema_doc = next(doc, None)
    client.close()
    if schema_doc is None:
        raise ValueError(f"No schema found for source={source} version={version}")
    return schema_doc["schema"]

def insert_into_mongo(data: dict, collection_name: str, config: Dict) -> Union[None, Exception]:
    try:
        client = get_mongo_client(config["mongo_uri"])
        client.admin.command("ping")
        db = client[config["database"]]
        collection = db[collection_name]
        data["ingested_at"] = datetime.now().isoformat()
        collection.insert_one(data)
        print(f"Appended new document to '{collection_name}' at {data['ingested_at']}")
    except Exception as e:
        print(f"MongoDB insert failed: {e}")
        return e
