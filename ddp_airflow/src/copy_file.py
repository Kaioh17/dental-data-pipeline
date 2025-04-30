import json
import psycopg2
import os
from dotenv import load_dotenv
#This loads the config file (converting it to a python dictionary for easy access)
def load_config(path = "ddp_airflow/config/dbm_config.json" ):
    try:
        with open(path)as f:
            return json.load(f)
    except (FileNotFoundError, FileExistsError) as f:
        raise ValueError(f"Error finding file: {f}")
#connect to any db
load_dotenv()
def connect_db( db_info):
    try:
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")

        if not db_user or not db_password:
            raise KeyError

        return psycopg2.connect(
                host = db_info["host"],
                port = db_info["port"],
                database = db_info["database"],
                user = db_user,
                password =db_password
            )
    except psycopg2.Error as  p:
        raise ConnectionError(f"Error connecting to db: {p}")
#method to connect to db
def connect_to_prod_db():
    config = load_config()
    prod_info = config["connections"]["source_db"]
    conn = connect_db(prod_info)
    return conn

#create a temp_db connection
def connect_to_temp_db(name):#name will be provided in the dag for each tasks
    config = load_config()
    test_info = config["connections"]["destinations"][name]
    conn = connect_db(test_info)
    return conn

#a close db method
def close_db(conn,cur):
    if cur:
        cur.close()
    if conn and not conn.closed:
        conn.close()

#functions to copy data from the production DB to the destination DB
def copy_data(source_cur, dest_conn, dest_cur, dest_table, **kwargs):
    source_type = kwargs.get("source", "clean")
    batch_size = kwargs.get ("batch_size", 3000)
    

    if  source_type == 'mini':
        source_cur.execute("SELECT * FROM bank_data LIMIT 5000")
    elif source_type == 'clean':
        source_cur.execute("SELECT * FROM bank_data ")

    rows = source_cur.fetchmany(batch_size) # retrieves chunk ofrows at a time
    if not rows:
        return "No data to copy."

    placeholders =','.join(['%s']* len(rows[0]))
    insert_query =f"INSERT INTO {dest_table} VALUES ({placeholders})"

    #Prepare an SQL insert statement with placeholders
    while rows:
        try:
            #insert each row into the destination table
            for row in rows:
                dest_cur.execute(insert_query, row)
            dest_conn.commit()
        except Exception as e:
            dest_conn.rollback()   
            return f"Error: {e}"
        rows = source_cur.fetchmany(batch_size)

           

    return f"Copied {len(rows)} rows to {dest_table}" #succes message

# Function to handle the process of copying data from production DB to dest DB
def perform_copy_data ( dest_table,dest_db_name, source, **kwargs):
    source_conn = connect_to_prod_db(source) #connect to source db
    source_cur = source_conn.cursor() #create cursor for production db

    dest_conn = connect_to_temp_db(dest_db_name)  # connect to destination db
    dest_cur = dest_conn.cursor() #create cursor for destination db

    try:
        copy_data(source_cur,dest_conn,dest_cur,dest_table,**kwargs)
    except (Exception, psycopg2.DatabaseError) as e:
        return f"Error: {e}"
    finally:
        # ensure all dbs connections are properly closed
        close_db(dest_conn,dest_cur)
        close_db(source_conn,source_cur)


#testing if path is right
if __name__ == "__main__":
    config = load_config()
    connect_to_prod_db()
    print(config)