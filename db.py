#Importing the required modules
import oracledb
import pandas as pd
# Specifying required parameters for connecting to oracle db
un = 'g23konda'
cs = '54.224.209.13:1521/xe'
pw = 'g23konda123'
d1 = r"C:\Users\saispoorthy.konda\Downloads\instantclient-basic-windows.x64-21.12.0.0.0dbru\instantclient_21_12"
# connecting to oracle DB and creating object
oracledb.init_oracle_client(lib_dir=d1)
connection = oracledb.connect(user=un, password=pw, dsn=cs) 
cursor = connection.cursor() 

# specifying schema_name and password for DB Link
schema_name = 'cm_20050609'
identified =  'cm_20050609123'

# executing query to create DB Link
cursor.execute(f'Drop public database link konda_dblink_classicmodels')
query= f"CREATE PUBLIC database link konda_dblink_classicmodels CONNECT TO {schema_name} IDENTIFIED BY {identified} USING 'XE'"
cursor.execute(query)

    
