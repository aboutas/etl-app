from pymongo import MongoClient

class MongoSink:
    def __init__(self, collection_name, config):
        self.collection_name = collection_name
        self.config = config

    def invoke(self, value, context):
        client = MongoClient(self.config["mongo_uri"])
        db = client[self.config["database"]]
        col = db[self.collection_name]
        col.insert_one(value)
        client.close()
