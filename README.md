# Spark-Kafka jobs orchestraed with Airflow
The following instruments are used in the project:
  - **Poetry**: as a project dependency management tool
  - **FastAPI**: used to immitate a data stream (a request sent and processed in a Faust-Kafka task)
  - **Faust**: Kafka-Python library
  - **SQLalchemy**: to persist data from FastAPI to a **Postgresql** database
  - **Spark**: to process a Kafka stream and persist the Data Stream to a **Cassandra** database
  - **Airflow**: to trigger tasks execution
  - **Docker**: to run everything in containers

## Installation

- Install Docker
- Start Docker Kafka, Spark, Cassandra containers `spark-learning: docker-compose up`
- (optional) `docker-compose up --scale spark-worker=3` to spin up the Spark additional spark workers
- Install all dependencies `spark-learning: poetry install`

### Airflow
- (Optional) Change Airflow home location (if running Airflow locally) `export AIRFLOW_HOME=~spark-learning/airflow`
- Create a custom Airflow docker image containing the project and its dependencie `spark-learning: docker  build . --file airflow/Dockerfile --tag my_airflow_image`
- Start Airflow containers `spark-learning: dc -f airflow/docker-compose.yaml up`
- Run following commands on the webserver and worker containers (install project deps and java8):
  - `sudo apt-get update`
  - `sudo apt install software-properties-common`
  - `sudo add-apt-repository ppa:openjdk-r/ppa`
  - `sudo apt-get install openjdk-8-jre`
  - `export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java`
  - if any of the commands above return an error, then install default java `apt install default-jre` and java home will be different `export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/bin/java`
  - `pip install poetry`
  - `poetry config virtualenvs.create false && poetry install`

### FastAPI
- Create a FastAPI-Uvicorn image `spark-learning/fastapi: docker  build . --tag my_fastapi_image`
- Start FastAPI container `docker-compose up`


## RUN
- Start faust-kafka worker (Required if **without** Airflow) `python data_processing/faust_kafka_stream.py worker -l info`
- Start spark (Required if **without** Airflow) `python fastapi/spark.py`
### Airflow
- Test run a task in a DAG (Example) `airflow tasks test airflow_dag_1 print_date 2015-06-01`
- Acecess webserver container `docker exec -it airflow_airflow-webserver_1 bash` and install poetry and project dependencies `poetry config virtualenvs.create false && poetry install`
- Start Airflow UI web server `airflow webserver --debug &`


## Helpers

### Postgres
- Access postgres container `docker exec -it airflow_postgres_1 bash`
- Access a DB in the container `psql --host=airflow_postgres_1 --dbname=vmtl --username=docker`
### Cassandra
- Access cassandra container `docker exec -it spark-cassandra cqlsh`
### Kafka
- Add more kafka brokers `docker-compose scale kafka=3`
- Access a container as a root user `docker exec -it -u=0 airflow_airflow-webserver_1 bash`