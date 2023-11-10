import oracledb
un = 'g23konda'
cs = '54.224.209.13:1521/xe'
pw = 'g23konda123'
d1 = r"C:\Users\saispoorthy.konda\Downloads\instantclient-basic-windows.x64-21.12.0.0.0dbru\instantclient_21_12"
oracledb.init_oracle_client(lib_dir=d1)
connection = oracledb.connect(user=un, password=pw, dsn=cs) 
cursor = connection.cursor() 
schema_name = 'cm_20050609'
identified = 'cm_20050609123'
etl_batch_date = '2001-01-01'
cursor.execute(f'Drop public database link konda_dblink_classicmodels')
query= f"CREATE PUBLIC database link konda_dblink_classicmodels CONNECT TO {schema_name} IDENTIFIED BY {identified} USING 'XE'"
cursor.execute(query)
cursor.execute(f'''
    SELECT * FROM Offices@konda_dblink_classicmodels
    WHERE to_char(update_timestamp, 'yyyy-mm-dd') >= '{etl_batch_date}'
''')
connection.close