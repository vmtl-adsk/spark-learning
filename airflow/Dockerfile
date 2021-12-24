# Install Airflow
FROM apache/airflow:latest-python3.9
ENV AIRFLOW_HOME=/opt/spark-learning/airflow
ENV PYTHON_BASE_IMAGE=3.9.1-buster
RUN pip install --upgrade pip
COPY poetry.lock pyproject.toml /opt/spark-learning/
COPY . .
WORKDIR /opt/spark-learning/airflow
#USER root
#RUN apt-get update \
#    && apt-get install -y --no-install-recommends \
#    vim \
#    && apt-get autoremove -yqq --purge \
#    && apt-get clean \
#    && rm -rf /var/lib/apt/lists/*
#USER airflow