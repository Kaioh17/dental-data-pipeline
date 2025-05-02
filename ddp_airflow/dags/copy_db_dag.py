from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from src.copy_file import perform_copy_data
from src.clean import sanitize_and_obfuscate



from datetime import datetime, timedelta


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
    dag_id = 'bank_elt',
    default_args = default_args,
    description = "Performs daily ELT: copies bank data from prod to clean DB, sanitizes it, and loads it into a dev database.",
    start_date = datetime(2025, 4, 29, 2),
    schedule_interval = '@daily'

) as dag:
    #extract from production to cleandb
    copy_to_clean = PythonOperator(
        task_id = 'copy_to_clean',
        description = "Loads raw data from production to clean database",
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
    
    # sanitize and obfuscate data
    transform_data = PythonOperator(
        task_id = 'sanitize_and_obfuscate',
        description = "Sanitizes and obfuscates sensitive fields in the clean bank data (e.g., name, age, contact).",
        python_callable= sanitize_and_obfuscate,
        retry_delay = timedelta(minutes = dest[0]["retry_delay"])
    )
    

    #copy to mini db
    copy_to_dev = PythonOperator(
        task_id = 'copy_to_dev',
        description = "Loads clean and minimized data to db",
        python_callable = perform_copy_data,
        op_kwargs=
        {
            "dest_table": dest[1]["destination_table"],
            "source_db" : dest[0]["name"], #calls the copy_file(i.e transformed)
            "dest_db_name": dest[1]["name"],
            "source" : dest[1]["source"],
            "batch_size" : dest[1]["batch_size"],
            "filter_criteria" : dest[1]["filter_criteria"]
        },
        retry_delay = timedelta(minutes = dest[1]["retry_delay"]) 
    )



    #set task dependencies (ELT)

    copy_to_clean >> transform_data >> copy_to_dev
