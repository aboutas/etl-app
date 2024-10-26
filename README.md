# Dynamic Mapping Solution for ETL Source and Destination
This project offers a proposed solution to the problem of dynamic mapping in both the source and destination stages of ETL processes.

The solution accepts a JSON input file, simulating various schemas that may be loaded into the ETL pipeline. A custom schema registry handles schema management, allowing flexible adjustments based on the data structure dynamically (source dynamic mapping).

The data processed includes metrics like temperature. When fields for "heating cost" and "currency" are present, an additional column, "heating cost in euros," is calculated based on currency exchange rates (destination mapping). This feature allows for seamless integration and transformation of varying data schemas within the ETL process.

# PROJECT STRUCTURE:

* flink_code.py: Main script that sets up the Flink streaming environment, loads input data, applies transformations, and writes output to a JSON file.

* schema_registry.py: Manages schema registration and retrieval to enable dynamic transformations based on different schema versions.

* dynamic_transform.py: Contains the transformation logic for processing input data based on the current schema.

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
Make sure to replace C:/path_to_your_local_output_directory with the absolute path of your local output directory.
