FROM flink:1.17.0-scala_2.12

USER root

# Install python and pip (minimal)
RUN apt-get update && apt-get install -y python3 python3-pip python3-setuptools

# Upgrade pip and install PyFlink
RUN pip3 install --upgrade pip
RUN pip3 install apache-flink==1.17.0 pymongo requests python-dateutil

# If you want extra dependencies:
# RUN pip3 install kafka-python confluent-kafka flask

USER flink
