from airflow import DAG
from airflow.operators.python import PythonOperator 

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from ddp_airflow.src.copy_file import perform_copy_data
from ddp_airflow.src.clean import sanitize_and_obfuscate


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
    dag_id = 'bank_elt_DAG',
    default_args = default_args,
    description = "Performs daily ELT: copies bank data from prod to clean DB, sanitizes it, and loads it into a dev database.",
    start_date = datetime(2025, 4, 29, 2),
    schedule = '@daily'

) as dag:
    #extract from production to cleandb
    copy_to_clean = PythonOperator(
        task_id = 'copy_to_clean',
        python_callable = perform_copy_data,
        op_kwargs=
        {
            "dest_table": dest[0]["destination_table"],
            "dest_db_name": dest[0]["name"],
            "source" : dest[0]["source"],
            "batch_size" : dest[0]["batch_size"]
             
        },
        retry_delay = timedelta(minutes = int(dest[0]["retry_delay"])), 
        doc_md = """### Task: copy_to_clean  
                    Loads raw data from production to the clean database.
                    """
    )
    
    # sanitize and obfuscate data
    transform_data = PythonOperator(
        task_id = 'sanitize_and_obfuscate',
        python_callable= sanitize_and_obfuscate,
        retry_delay = timedelta(minutes = int(dest[0]["retry_delay"])),
        doc_md = """### Task: sanitize_and_obfuscate  
                    Sanitizes and obfuscates sensitive fields in the clean bank data (e.g., name, age, contact).
                    """
         
    )
    

    #copy to mini db
    copy_to_mini = PythonOperator(
        task_id = 'Copy-clean-data-to-mini-db',
        python_callable = perform_copy_data,
        op_kwargs=
        {
            "dest_table": dest[1]["destination_table"],
            "source_db" : dest[0]["name"], 
            "dest_db_name" : dest[1]["name"],
            "batch_size" : dest[1]["batch_size"],
            "source" : dest[1]["source"],
            "filter_criteria" : dest[2]["filter_criteria"]

        },
        retry_delay = timedelta(minutes = int(dest[1]["retry_delay"])), 
        doc_md = """### Task: Copy-clean-data-to-mini-db
                    This task copies already transformed data to the mini database
                    """
    )
   


    #set task dependencies (ELT)

    copy_to_clean >> transform_data >> copy_to_mini
