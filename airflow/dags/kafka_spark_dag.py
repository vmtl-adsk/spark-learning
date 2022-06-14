from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import sys
import socket
import config_vars as vars


def prnt():
    print(sys.executable)
    print(sys.path)
    print(socket.gethostname())


with DAG(
    dag_id='kafka_spark_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email': [vars.email],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'dag': dag,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description='postgres-kafka-spark-cassandra pipeline',
    start_date=datetime(2022, 6, 8),
    default_view='graph',
    schedule_interval='@daily',
    catchup=False,
    tags=['spark'],
) as dag:

    # Defining tasks
    task_1 = PythonOperator(
        task_id='print_date',
        python_callable=prnt,
    )

    task_2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    task_1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """
    )

    task_3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
    )

    task_4 = BashOperator(
        task_id='start_spark_stream',
        depends_on_past=False,
        bash_command='python /opt/spark-learning/data_processing/spark.py',
        retries=0
    )

    task_5 = BashOperator(
        task_id='start_kafka_broker',
        depends_on_past=False,
        bash_command='python /opt/spark-learning/data_processing/faust_kafka_stream.py worker -l info',
        retries=0
    )

    task_6 = BashOperator(
        task_id='ge_data_validation_postgre',
        depends_on_past=False,
        bash_command='cd /opt/spark-learning/great_expectations && great_expectations checkpoint run checkpoint_postgre',
        retries=0
    )
