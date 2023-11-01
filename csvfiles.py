import oracledb
import csv
table_names=["Offices", "Productlines", "Employees", "Customers", "Payments","Orders","Products", "Orderdetails","Calendar"]
un = 'g23konda'
cs = '54.224.209.13:1521/xe'
pw = 'g23konda123'
d1 = r"C:\Users\saispoorthy.konda\Downloads\instantclient-basic-windows.x64-21.12.0.0.0dbru\instantclient_21_12"
oracledb.init_oracle_client(lib_dir=d1)
with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
    with connection.cursor() as cursor:
       for table_name in table_names:
            cursor.execute(f"select * FROM CM_20050609.{table_name}")
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            with open(f'{table_name}.csv','w', newline="") as file:
                csv_writer = csv.writer(file)
                csv_writer.writerow(column_names)
                csv_writer.writerows(rows)