import oracledb
import platform
import csv
un = 'g23konda'
cs = '54.224.209.13:1521/xe'
pw = 'g23konda123'
if platform.system() == "Windows":
  d1 = r"C:\Users\saispoorthy.konda\Downloads\instantclient-basic-windows.x64-21.12.0.0.0dbru\instantclient_21_12"
oracledb.init_oracle_client(lib_dir=d1)
with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
    with connection.cursor() as cursor:
       cursor.execute("select * from customers")
       results = cursor.fetchall()
       csv_file = r"C:\Users\saispoorthy.konda\Downloads\sample.csv"
       with open(csv_file, "w", newline="") as file:
          csv_writer = csv.writer(file)
          column_names = [description[0] for description in cursor.description]
          csv_writer.writerow(column_names)
          csv_writer.writerows(results)
          