# Use a slim Python base image
FROM python:3.10-slim-bullseye

# Install system dependencies and Java for PyFlink
RUN pip install --no-cache-dir \
    apache-flink==1.17.0 \
    "pymongo[srv]" \
    requests \
    kafka-python \
    confluent-kafka \
    python-dateutil \
    flask

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /opt/flink/etl_app

COPY config/ ./config/
COPY src/ ./src/

