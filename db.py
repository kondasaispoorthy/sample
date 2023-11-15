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
identified = 'cm_20050609123'

# executing query to create DB Link
cursor.execute(f'Drop public database link konda_dblink_classicmodels')
query= f"CREATE PUBLIC database link konda_dblink_classicmodels CONNECT TO {schema_name} IDENTIFIED BY {identified} USING 'XE'"
cursor.execute(query)

# selecting the values from batch_control table
cursor.execute(f"select * FROM g23konda.batch_control")

 # Convert the results of the SQL query into a pandas DataFrame.
df = pd.DataFrame(cursor.fetchall(), columns=list(map(lambda col: col[0],cursor.description)))

# etl_batch_no & etl_batch_date
etl_batch_no = df.ETL_BATCH_NO[0]
etl_batch_date = df.ETL_BATCH_DATE[0]
print(f'etl batch no and etl batch date are {etl_batch_no} and {etl_batch_date} respectively')
