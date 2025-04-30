import psycopg2
import copy_file

copy_file.connect_to_prod_db()

## we have an operator that will connect to the production db 
## connecttodb >> sanitize >> obfuscate

def obfuscation():
    ### connect to db
    # - write code for obfuscation 
    copy_file.connect_to_temp_db("obfuscated_bank_db")
    pass

def sanitazation():
    copy_file.connect_to_temp_db("sanitize_bank_db")
    pass