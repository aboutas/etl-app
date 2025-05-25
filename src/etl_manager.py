from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)

KAFKA_BROKER = "kafka:9092"
CONFIG_TOPIC = "config_topic"

def get_kafka_producer():
    return KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode("utf-8"))

@app.route("/submit_config", methods=["POST"])
def submit_config():
    try:
        req_json = request.get_json()
        if not req_json:
            return jsonify({"error": "Missing JSON"}), 400
        if "config" not in req_json or "rules_plan" not in req_json:
            return jsonify({"error": "Both 'config' and 'rules_plan' are required!"}), 400
        producer = get_kafka_producer()
        producer.send(CONFIG_TOPIC, req_json)
        producer.flush()
        return jsonify({"status": "Config submitted to Kafka"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
