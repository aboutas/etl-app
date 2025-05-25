# ETL Streaming Pipeline: Open Data → Kafka → PyFlink → MongoDB

## Overview
This project implements a flexible, real-time ETL (Extract, Transform, Load) pipeline in Python.
It ingests data from public REST APIs, streams it through Kafka, applies dynamic, user-defined transformation rules (with full nested JSON support), and stores both results and logs in MongoDB.

Config management is handled via a Flask API: simply POST your config and rules, and the system spins up streaming jobs for each new data source—no code or service restarts needed!

---

## Features

- **REST API ingestion**: Pulls data from any REST endpoint (API, configurable)
- **Dynamic rules**: Users upload configs/rule plans via an HTTP endpoint (no file editing needed)
- **Multi-user / multi-job**: The pipeline can process multiple independent configs and rules in parallel
- **Kafka streaming**: Modern, robust data transport
- **Flexible transformation engine**: 
  - Rules defined per field (including nested fields, dot notation: `owner.name`)
  - Easily add new transformations in `transformations.py`
- **MongoDB persistence**:
  - Stores results and a log/history of applied transformations per record
- **Schema tracking**:
  - Auto-infers JSON schema from data, versions tracked in MongoDB
- **Dockerized**: Spin up all dependencies
---

## Setup & Running

### 1. Clone the repo
``` bash
git clone https://github.com/aboutas/etl-app.git
cd etl-app
```

### 2 Configure
* (Optional) Adjust default config.json in /config for reference.

* You will submit configs and rules via the API endpoint after startup—no need to edit files for new jobs!


### 3. Build & Start All Services
``` bash
docker-compose up --build
```
This starts:
* MongoDB

* Kafka & Zookeeper

* Producer, Consumer, and Manager (Flask API) containers

### 4. Submit Your Configs and Rules
* POST a JSON payload to the Manager API to launch an ETL pipeline for each new data source.
* Example (config_and_rules.json)
```json 
{
  "config": {
    "url": "https://api.openaq.org/v3/locations",
    "api_key": "YOUR_API_KEY",
    "topic": "openaq-data-locations",
    "database": "registry_locations",
    "schema_collection": "locations_schema_registry",
    "app_data_collection": "locations_data",
    "log_data_collection": "locations_logs",
    "mongo_uri": "mongodb://root:password@mongo:27017",
    "verbosity": 0
  },
  "rules_plan": {
    "data_cleaning": {
      "lower_case": ["name", "owner.name"]
    },
    "data_standardization": {
      "renaming_columns": {"fields": ["timezone"], "rename_map": {"timezone": "zoniwras"}}
    },
    "data_aggregation": {
      "summarization": ["sensors.id", "instruments.id"]
    },
    "time_transformations": {
      "trimming": ["datetimeFirst.utc", "datetimeFirst.local"]
    }
  }
}
```
* Post with curl (Windows example):
``` bash
curl.exe -X POST http://localhost:8080/submit_config ^
  -H "Content-Type: application/json" ^
  -d "@C:\path\to\config_and_rules.json"
```

### 5. Browse the data

* Results and logs are available in your configured MongoDB collections.

* Transformation Rule Plan Example
``` json
{
  "data_cleaning": {
    "lower_case": ["name", "country.name", "owner.name"]
  },
  "anonymization": {
    "data_masking": ["id", "owner.id", "country.id"]
  },
  "data_aggregation": {
    "summarization": ["sensors.id", "instruments.id"]
  },
  "time_transformations": {
    "trimming": ["datetimeFirst.utc", "datetimeFirst.local"]
  }
}
```

* Keys are transformation categories and method names (must match those in transformations.py)

* Values are the fields (dot notation supports nested JSON)

## Extending: Add a New Transformation
* Implement your function in transformations.py

* Register it in the transformation registry

* Add it to your rules plan and POST via the API

## Next Steps
* Web UI: The system is ready to add a simple web interface for uploading configs/rules, monitoring jobs, etc.
