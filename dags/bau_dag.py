import datetime as dt

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator

dag = DAG (
    dag_id = 'bau_dag',
    start_date = dt.datetime(year=2022, month=1, day=28),
    schedule_interval = None
)

start_dag = DummyOperator(
    task_id = 'start',
    dag = dag
)

extract_load_fpl = DockerOperator(
    task_id = 'extract_load_fpl',
    image = 'analytics-fc:dev',
    container_name = 'extract_load_fpl',
    auto_remove = True,
    command = 'elt tap-fpl target-postgres',
    network_mode = 'analytics-fc_default',
    dag = dag
)

start_dag >> extract_load_fpl

