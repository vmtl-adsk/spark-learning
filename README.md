# Spark-Kafka jobs orchestrated with Airflow
The following instruments are used in the project:
  - **Poetry**: as a project dependency management tool
  - **FastAPI**: used to immitate a data stream (a request sent and processed in a Faust-Kafka task)
  - **Faust**: Kafka-Python library
  - **SQLalchemy**: to persist data from FastAPI to a **Postgresql** database
  - **Spark**: to process a Kafka stream and persist the Data Stream to a **Cassandra** database
  - **Airflow**: to trigger tasks execution
  - **Docker**: to run everything in containers
  - **Greate Expectations**: to validate data on all the steps of the pipeline

## Installation
- Install Docker
- Install all project dependencies `spark-learning: poetry install`
### Airflow
- (Optional) Change Airflow home location (if running Airflow locally) `export AIRFLOW_HOME=~spark-learning/airflow`
- Create a custom Airflow docker image containing the project and its dependencies `spark-learning: docker  build . --file airflow/Dockerfile --tag my_airflow_image`
- Start Airflow containers `spark-learning: dc -f airflow/docker-compose.yaml up`
- Install project dependencies on webserver and worker containers:
  - `pip install poetry`
  - `poetry config virtualenvs.create false && poetry install`
### FastAPI
- Create a FastAPI-Uvicorn image `spark-learning/fastapi: docker  build . --tag my_fastapi_image`
- Start FastAPI container `spark-learning/fastapi: docker-compose up`

## RUN
- Start Docker Kafka, Spark, Cassandra containers `spark-learning: docker-compose up`
- (optional) `docker-compose up --scale spark-worker=3` to spin up the Spark additional spark workers
- Start faust-kafka worker (Required if **without** Airflow) `python data_processing/faust_kafka_stream.py worker -l info`
- Start spark (Required if **without** Airflow) `python fastapi/spark.py`
- Test run a task in a DAG (Example) `airflow tasks test airflow_dag_1 print_date 2015-06-01`

pip install poetry && poetry config virtualenvs.create false && poetry install
## Helpers

### Postgres
- Access postgres container `docker exec -it airflow_postgres_1 bash`
- Access a DB in the container `psql --host=airflow_postgres_1 --dbname=vmtl --username=docker`
### Cassandra
- Access cassandra container `docker exec -it spark-cassandra cqlsh`
### Kafka
- Add more kafka brokers `docker-compose scale kafka=3`
- Access a container as a root user `docker exec -it -u=0 airflow_airflow-webserver_1 bash`
### Airflow
- Acecess webserver container `docker exec -it airflow_airflow-webserver_1 bash`
- Manually start Airflow UI web server `airflow webserver --debug &`
### Great Expectations
- Change default port for Jypiter notebooks in cas of ports conflict `export GE_JUPYTER_CMD='jupyter notebook --port=8187'`
- Create a datasource `great_expectations datasource new`
- Create an expectations suite `great_expectations suite new`
- Create a checkpoint `great_expectations checkpoint new`