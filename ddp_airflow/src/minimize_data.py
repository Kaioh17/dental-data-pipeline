import psycopg2 
from . import copy_file


###after minimization process the data and structture of each table will be sent to the sandox dbs

def connect_db(conn, cur):
    conn = copy_file.connect_to_temp_db("mini_bank_db")
    cur = conn.cursor()
    return conn, cur

def close_db(conn, cur):
    conn.close()
    cur.close()

###filter
def filter_data(month: str):
    conn, cur = connect_db()
    cur.execute ("""SELECT * FROM bank_data
                    WHERE month = %s;
                 """, (month,))
    conn.commit()
    close_db(conn, cur) 
    return 

##by selecting a specific amount of duratiom e.g duration  > 600
def sample_data():
    pass
     
def data_aggregation():
    pass

def suppress_fields():
    pass


#a function built to copy minimized data into the different lower env
# def copy_to_sandboxes(dest_table, **kwargs):
#    dest_db = ["sandbox1","sandbox2","sandbox3" ]
#    for db in dest_db:
#         copy_file.perform_copy_data(dest_table, dest_db_name = db, **kwargs)    
    
# copy_to_sandboxes("bank_data")
    