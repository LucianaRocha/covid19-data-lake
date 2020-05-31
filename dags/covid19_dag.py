from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

from covid19_helpers import check_csv_data_exists, \
                            check_wildcard_data_exists, \
                            emr_settings, \
                            covid19_pipeline


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


# Verify weather world data file exists
verify_world_data_file_task = PythonOperator(
    task_id='verify_world_data_file',
    python_callable=check_csv_data_exists,
    op_kwargs={'bucket': 'covid19-lake',
               'prefix': 'archived/tableau-jhu/csv',
               'file': 'COVID-19-Cases.csv'},
    dag=dag
)


# Verify weather Brazil data file exists
verify_brazil_data_file_task = PythonOperator(
    task_id='verify_brazil_data_file',
    python_callable=check_csv_data_exists,
    op_kwargs={'bucket': 'covid19-input',
               'prefix': 'raw-data',
               'file': 'COVID-19-Brazil.csv'},
    dag=dag
)


# Verify weather US data file exists
verify_usa_data_file_task = PythonOperator(
    task_id='verify_usa_data_file',
    python_callable=check_wildcard_data_exists,
    op_kwargs={'bucket': 'covid19-lake',
               'prefix': 'archived/enigma-jhu/json'},
    dag=dag
)


# Create an EMR JobFlow
spin_up_emr_cluster_task = EmrCreateJobFlowOperator(
    task_id='spin_up_emr_cluster',
    job_flow_overrides=emr_settings,
    dag=dag
)


# Add steps to an existing EMR JobFlow
add_pipeline_to_emr_cluster_task = EmrAddStepsOperator(
    task_id='add_pipeline_to_emr_cluster',
    job_flow_id="{{task_instance.xcom_pull('spin_up_emr_cluster', " \
               +"  key='return_value')}}",
    steps=covid19_pipeline,
    dag=dag
)


# Wait step to be completed
watch_pipeline_step_task = EmrStepSensor(
    task_id='watch_pipeline_step',
    job_flow_id="{{task_instance.xcom_pull(" \
                "      'spin_up_emr_cluster'," \
                "      key='return_value')}}",
    step_id="{{task_instance.xcom_pull(" \
            "  'add_pipeline_to_emr_cluster'," \
            "  key='return_value')[0]}}",
    dag=dag)


# Terminate EMR JobFlows
spin_down_emr_cluster_task = EmrTerminateJobFlowOperator(
    task_id='spin_down_emr_cluster',
    job_flow_id="{{task_instance.xcom_pull('spin_up_emr_cluster', " \
               +"  key='return_value')}}",
    trigger_rule="all_done",
    dag=dag
)


# Set the DAG the end execution
end_operator = DummyOperator(task_id='End_execution',  dag=dag)


# Set the correct dependecies
start_operator >> [verify_world_data_file_task,
                   verify_brazil_data_file_task, 
                   verify_usa_data_file_task] \
>> spin_up_emr_cluster_task \
>> add_pipeline_to_emr_cluster_task >> watch_pipeline_step_task \
>> spin_down_emr_cluster_task >> end_operator
