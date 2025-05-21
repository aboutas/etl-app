<!-- # Dynamic Mapping Solution for ETL Source and Destination
This project offers a proposed solution to the problem of dynamic mapping in both the source and destination stages of ETL processes.

The solution accepts a JSON input file, simulating various schemas that may be loaded into the ETL pipeline. A custom schema registry handles schema management, allowing flexible adjustments based on the data structure dynamically (source dynamic mapping).

The data processed includes metrics like temperature. When fields for "heating cost" and "currency" are present, an additional column, "heating cost in euros," is calculated based on currency exchange rates (destination mapping). This feature allows for seamless integration and transformation of varying data schemas within the ETL process.

# PROJECT STRUCTURE:

* flink_code.py: Main script that sets up the Flink streaming environment, loads input data, applies transformations, and writes output to a JSON file.

* schema_manager.py: Manages schema registration and retrieval to enable dynamic transformations based on different schema versions.

* transform_rules_manager.py: Contains the transformation logic using lamda functions for processing input data based on the current schema.

* input.json: Input file containing JSON records to be processed by the Flink job.

* Dockerfile: The Dockerfile to build the environment, install dependencies, and execute the Flink job.

# GETTING STARTED
## Prerequisites
* Docker: Ensure Docker is installed on your system.

* Python 3.10: The Python version used in this project is 3.10. Docker will handle the installation of dependencies and environment setup.

* Apache PyFlink: PyFlink is installed via pip inside the Docker container.


## Build the Docker image: In the project directory, run:
```
build -t flink-job-image . 
```

## Run the Docker container: 
After building the image, run the following command to execute the Flink job:
```
docker run --name flink-job-container -v C:/path_to_your_local_output_directory:/opt/flink/output flink-job-image
```
Note! : 
Make sure to replace C:/path_to_your_local_output_directory with the absolute path of your local output directory. -->
# ETL Streaming Pipeline: Open Data → Kafka → PyFlink → MongoDB

## Overview
This project implements a flexible, streaming ETL (Extract, Transform, Load) pipeline in Python.  
It ingests data from an open API, streams it through Kafka, applies dynamic transformation rules (supporting nested JSON), and stores both results and logs in MongoDB.

The pipeline is modular, allowing users to adapt the rule plan, support new APIs, or extend transformation logic with ease.

---

## Features

- **Data ingestion** from any REST API (configured via `config.json`)
- **Kafka streaming** (dockerized, ready for real-time or batch)
- **Dynamic transformation** based on a rule plan (`rules_plan.json`)
- **Nested JSON support** with dotted notation (e.g., `country.name`)
- **MongoDB persistence**: both results and a log/history of applied rules
- **Schema handling**: auto-inferred from the API input, tracked per source/version
- **Dockerized**: Easily spin up MongoDB, Kafka, Zookeeper, and your app(s) with Docker Compose

---

## Setup & Running

### 1. Clone the repo
``` bash
https://github.com/aboutas/etl-app.git
cd etl-app
```

### 2 Configure
* Edit config.json with your API endpoint, keys, Mongo details, etc.

* Edit rules_plan.json to define your transformation rules (see below).


### 3. Build & Start All Services
''' bash
docker-compose up --build
'''
*Starts MongoDB, Kafka, Zookeeper, producer, and consumer containers.

### 4. See the Data
Browse results in MongoDB

## Rule Plan Example (rules_plan.json)
''' json
{
  "data_cleaning": {
    "standardize_format": ["name", "country.name", "owner.name"],
    "data_masking": ["id", "owner.id"]
  },
  "text_manipulation": {
    "trimming": ["datetimeFirst.utc", "datetimeFirst.local"]
  }
}

'''

* Keys are transformation categories and methods

* Values are fields to apply them on (dot notation for nested fields)

## Adding a New Transformation
* Add your logic in transformations.py
* Register it in the transformation registry 
* Add to your rule plan!

