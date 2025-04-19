import json
import psycopg2

#This loads the config file (converting it to a python dictionary for easy access)
def load_config(path = "../config/dbm_config.json" ):
    try:
        with open(path)as f:
            return json.load(f)
    except (FileNotFoundError, FileExistsError) as f:
        return f"Error finding file: {f}"

#connect to any db
def connect_db(db_info):
    return psycopg2.connect(
        host = db_info["host"],
        port = db_info["port"],
        database = db_info["database"],
        user = db_info["user"],
        password = db_info["password"]
    )

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
def copy_data(source_query, source_cur, dest_conn, dest_cur, dest_table):
    source_cur.execute(source_query)
    rows = source_cur.fetchall() # retrieves all rows in production db

    if not rows:
        return "No data to copy."

    #Prepare an SQL insert statement with placeholders
    placeholders =','.join(['%s']* len(rows[0]))
    insert_query =f"INSERT INTO {dest_table} VALUES ({placeholders})"

    #insert each row into the destination table
    for row in rows:
        dest_cur.execute(insert_query, row)
    dest_conn.commit()

    return f"Copied {len(rows)} rows to {dest_table}" #succes message

# Function to handle the process of copying data from production DB to dest DB
def perform_copy_data (source_query,dest_table,dest_db_name):
    source_conn = connect_to_prod_db() #connect to source db
    source_cur = source_conn.cursor() #create cursor for production db

    dest_conn = connect_to_temp_db(dest_db_name)  # connect to destination db
    dest_cur = dest_conn.cursor() #create cursor for destination db

    try:
        copy_data(source_query,source_cur,dest_conn,dest_cur,dest_table)
    except (Exception, psycopg2.DatabaseError) as e:
        return f"Error: {e}"
    finally:
        # ensure all dbs connections are properly closed
        close_db(dest_conn,dest_cur)
        close_db(source_conn,source_cur)


#testing if path is right
if __name__ == "__main__":
    config = load_config()
    print(config)