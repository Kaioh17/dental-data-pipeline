import psycopg2
import copy_file



def anonymize_data(data):
    
    ##AGE JOB MARITAL EDUCATION DEFAULT BALANCE HOUSing LOAN CONTACT 
    data['Job'] = 'unknown'
    data['marital'] = 'unknown'
    data['loan']  =  'unknown'  
    data['education'] = 'unkown'
    return data
    # data['contact']  =  'redacted'  
    
    return data
def redact_data(data):
    data['contact'] = '(xxx)-xxx-xxxx'
    return data

def mask_data(data):
    data['balance'] = -1
    return data

def tokenize_data(data):
    pass

def aggregate_age(data):
    age = data.get('age')

    if age is not None: 
        if age < 20:
            data['age'] = 'under 20'
        elif 20 <= age < 30:
            data['age'] = '20-29'
        else:
            data['age'] = '30+'
    else:
        data['age'] = -1
    return data
def field_removal():
    keys_to_remove = ['default', 'pdays','previous','poutcome'] 
    return keys_to_remove

def _execute_removal_query(conn, cur, columns):
    clause = ",".join([f'DROP COLUMN IF EXISTS "{col}"' for col in columns])
    cur.execute(f"ALTER TABLE bank_data  {clause};")
    print(f"ALTER TABLE bank_data {clause};")
    #conn.commit()

#helper function execute query and reduce redundancy
def _execute_query(conn, cur, columns):
    
    clause  = ",".join([f"{k} = '{v}'" for k,v in columns.items()])
    cur.execute(f"UPDATE bank_data SET {clause};")
    print(f"UPDATE bank_table SET {clause};")
    # conn.commit()


#python operation sanotzong an dobfuscattion
def sanitize_and_obfuscate():
    conn= copy_file.connect_to_temp_db("clean_bank_db")
    cur = conn.cursor()

    #anonymize data
    column_anonymized = {}
    anonymized_data = anonymize_data(column_anonymized)
    #query
    _execute_query(conn, cur, anonymized_data)

   

    #redact data
    column2 = {}
    redacted_data = redact_data(column2)
    #query
    _execute_query(conn,cur, redacted_data)

    # mask data
    column_masked = {}
    masked_data = mask_data(column_masked)
    #query
    _execute_query(conn,cur, masked_data)

    #Aggregate data
    column_agg = {}
    agg_data = aggregate_age(column_agg)
    #query
    _execute_query(conn,cur, agg_data)

    #field remval
    remove_data = field_removal()
    print(remove_data)
    #query
    _execute_removal_query(conn, cur, remove_data)    
    
    
    
    copy_file.close_db(conn,cur)






sanitize_and_obfuscate()


    
