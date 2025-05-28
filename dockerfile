# Start from the official PyFlink image
FROM apache/flink:1.17.0-python

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install pymongo requests kafka-python python-dateutil flask

# Set up working directory
WORKDIR /opt/flink/etl_app

# Copy source code and configs
COPY src/ ./src/
COPY config/ ./config/

ENV PYTHONPATH="/opt/flink/etl_app/src:$PYTHONPATH"
