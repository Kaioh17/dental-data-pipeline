from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from src.copy_file import perform_copy_data
from src import minimize_data

from datetime import datetime, timedelta
import json

# Load json config settings 
def load_config(path = "ddp_airflow/config/dbm_config.json" ):
    try:
        with open(path)as f:
            return json.load(f)
    except (FileNotFoundError, FileExistsError) as f:
        raise ValueError(f"Error finding file: {f}") 

config = load_config()
dest = config["environments"]


default_args = {
    'owner': 'airflow',
    'retries' : 3
}

with DAG(
    dag_id = 'data_minimization_DAG',
    default_args = default_args,
    description = "A DAG data minimization before delvering to the lower environment",
    start_date = datetime(2025, 4, 29, 2),
    schedule_interval = '@daily'

) as dag:
    # this task waits for the first copy_to_mini task to be done
    wait_for_copy_to_mini = ExternalTaskSensor(
        task_id = 'wait_for_copy_to_mini_task',
        external_dag_id = 'bank_elt_DAG',
        external_task_id= 'copy_to_mini',
        execution_delta= timedelta(minutes = 0),
        timeout = 600,
        mode = 'poke',
        poke_interval = 30
    )
    filter_data =PythonOperator(
        task_id = 'filter_data',
        description = "Loads only data needed",
        python_callable= minimize_data.filter_data,

    )

    #a task to copy's the minimized data into the lower env
    copy_to_dev = PythonOperator(
        task_id = 'copy_to_dev',
        description = "Loads clean and minimized data to db",
        python_callable = perform_copy_data,
        op_kwargs=
        {
            "dest_table": dest[2]["destination_table"],
            "source_db" : dest[1]["name"], 
            "dest_db_name": dest[2]["name"],
            "source" : dest[2]["source"],
            "batch_size" : dest[2]["batch_size"],
            "filter_criteria" : dest[2]["filter_criteria"]
        },
        retry_delay = timedelta(minutes = dest[2]["retry_delay"]) 
    )


    # set task dependencies (wait_copy_to_mini >> data-minimization process >> copy_to_dev)