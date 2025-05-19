import psycopg2 
import copy_file


def connect_db(conn, cur):
    conn = copy_file.connect_to_temp_db("mini_bank_db")
    cur = conn.cursor()
    return conn, cur


def filter_data():
    conn, cur = connect_db()
    cur.execute ("""SELECT age, job, marital, education, balance, deposit FROM bank_data""")
    rows = cur.fetchall()   
    ###we wan to select a specific amount of columns 

def data_aggregation():
    pass

    


    