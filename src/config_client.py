import os
import json
from flask import Flask, request, jsonify
from datetime import datetime

app = Flask(__name__)

CONFIG_SAVE_DIR = "/opt/flink/etl_app/configs"  # Make sure this exists and is mounted in Docker!

if not os.path.exists(CONFIG_SAVE_DIR):
    os.makedirs(CONFIG_SAVE_DIR, exist_ok=True)

@app.route("/submit_config", methods=["POST"])
def submit_config():
    try:
        req_json = request.get_json()
        if not req_json:
            return jsonify({"error": "Missing JSON"}), 400
        if "config" not in req_json or "rules_plan" not in req_json:
            return jsonify({"error": "Both 'config' and 'rules_plan' are required!"}), 400

        # Unique filename for each config
        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
        topic = req_json["config"].get("topic", "unknown_topic")
        save_path = os.path.join(CONFIG_SAVE_DIR, f"{topic}_{timestamp}.json")
        with open(save_path, "w") as f:
            json.dump(req_json, f, indent=2)

        # Here: Optionally, trigger Flink job with this config (see next step)
        # (For now, just print)
        print(f"Saved config to {save_path}")
        return jsonify({"status": "Config saved", "config_path": save_path}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
