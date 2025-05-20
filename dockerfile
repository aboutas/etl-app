# Use a slim Python base image
FROM python:3.10-slim-bullseye

# Install system dependencies and Java for PyFlink
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Java env vars
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install PyFlink + Mongo + requests
RUN pip install apache-flink==1.17.0 "pymongo[srv]" requests

WORKDIR /opt/flink/etl_app

COPY config.json .
COPY helpers.py .
COPY main.py .
COPY schema_handler.py .
COPY transformations.py .
COPY transformer.py .
COPY api_client.py .

COPY rules_plan.json rules_plan.json
# Ensure module can run with -m etl_app.main
RUN touch etl_app/__init__.py || mkdir -p etl_app && touch etl_app/__init__.py
