from airflow import DAG
from airflow.operators.python import PythonOperator

from src.copy_file import perform_copy_data


from datetime import datetime, timedelta

import psycopg2
import json



#Load json config settings 
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
    dag_id = 'copy_db',
    default_args = default_args,
    description = "This copies task from prod db to destinations",
    start_date = datetime(2025, 4, 29, 2),
    schedule_interval = None

) as dag:
    copy_to_sanitized = PythonOperator(
        task_id = 'copy_to_sanitized',
        python_callable = perform_copy_data,
        op_kwargs=
        {
            "dest_table": dest[0]["destination_table"],
            "dest_db_name": dest[0]["name"],
            "source" : dest[0]["source"],
            "batch_size" : dest[0]["batch_size"]
             
        },
        retry_delay = timedelta(minutes = dest[0]["retry_delay"]) 
    )

    copy_to_obfuscated = PythonOperator(
        task_id = 'copy_to_obfuscated',
        python_callable = perform_copy_data,
        op_kwargs=
        {
            "dest_table": dest[1]["destination_table"],
            "dest_db_name": dest[1]["name"],
            "source" : dest[1]["source"],
            "batch_size" : dest[1]["batch_size"]
             
        },
        retry_delay = timedelta(minutes = dest[1]["retry_delay"]) 
    )

    copy_to_dev = PythonOperator(
        task_id = 'copy_to_dev',
        python_callable = perform_copy_data,
        op_kwargs=
        {
            "dest_table": dest[2]["destination_table"],
            "dest_db_name": dest[2]["name"],
            "source" : dest[2]["source"],
            "batch_size" : dest[2]["batch_size"],
            "filter_criteria" : dest[2]["filter_criteria"]
        },
        retry_delay = timedelta(minutes = dest[2]["retry_delay"]) 
    )
    #sset task dependencies
    copy_to_sanitized >> copy_to_obfuscated >> copy_to_dev
