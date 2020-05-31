from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


# Define default_args that will be passed on to each operator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}

# Define a DAG and use the default_args
dag = DAG(
    'covid19_pipeline_dag',
    default_args=default_args,
    max_active_runs=1,
    description='Load and transform data into S3 Parquet with Airflow',
    schedule_interval='@once',
    is_paused_upon_creation=False
)

# Set the DAG begin execution
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Set the DAG the end execution
end_operator = DummyOperator(task_id='End_execution',  dag=dag)

# Set the correct dependecies
start_operator >> end_operator
