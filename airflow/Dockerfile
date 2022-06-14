# Install Airflow
FROM apache/airflow:latest-python3.9
ENV AIRFLOW_HOME=/opt/spark-learning/airflow
ENV PYTHON_BASE_IMAGE=3.9.1-buster
RUN pip install --upgrade pip
COPY poetry.lock pyproject.toml /opt/spark-learning/
COPY . .
WORKDIR /opt/spark-learning/airflow

# Install java to make spark working
USER root
RUN apt-get update
RUN apt-get install -y software-properties-common
RUN apt-get install -y default-jre
RUN export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/bin/java

# Create a new user (airflow doesn't want to be installed with root user)
RUN useradd -m docker && echo "airflow:airflow" | chpasswd && adduser docker sudo
USER airflow