import psycopg2
import copy_file



def anonymize_data(data):
    
    ##AGE JOB MARITAL EDUCATION DEFAULT BALANCE HOUSing LOAN CONTACT 
    data['Age'] = None
    data['Job'] = 'unknown'
    data['marital'] = 'unknown'
    data['education']  =  'unknown' 
    data['balanced']  =  0
    data['loan']  =  'unknown' 
    data['contact']  =  'redacted'  
    
    return data
def redact_data(data):
    pass

def mask_data(data):
    pass

def tokenize_data(data):
    pass

def aggregate_age(data):
    pass



#python operation sanotzong an dobfuscattion
def sanitize_and_obfuscate():
    conn= copy_file.connect_to_temp_db("clean_bank_db")
    cur = conn.cursor()

    #anonymize data
    column = {}
    data = anonymize_data(column)

    set_clause  = ",".join([f"{k} = {v}" for k,v in data.items()])
    cur.execute(f"UPDATE bank_table SET {set_clause};")
    # print(f"UPDATE bank_table SET {set_clause};")


    conn.commit()
    copy_file.close_db(conn,cur)



sanitize_and_obfuscate()


    
