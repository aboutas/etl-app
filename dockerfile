# Use a slim Python base image to keep it lightweight
FROM python:3.10-slim-bullseye

# Install system dependencies and Java (required for Flink)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set up environment variables for Java (required for PyFlink)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Apache Flink Python API (PyFlink) and necessary dependencies
RUN pip install apache-flink==1.17.0

# Set the working directory inside the container
WORKDIR /opt/flink/app

COPY main.py .
COPY schema_manager.py .
COPY dynamic_transform.py .
COPY rule_manager_transform.py .

COPY input.json /opt/flink/app/input.json

RUN mkdir -p /opt/flink/output

CMD ["python", "/opt/flink/app/main.py"]
