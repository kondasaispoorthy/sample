# Importing thr required Modules
import boto3
import oracledb
import csv
import os
from datetime import datetime
from botocore.exceptions import ClientError
import logging
import sys
sys.path.append('C:/Users/saispoorthy.konda/Downloads/Pratice/sample')
import db
import redshift_conn
# Storing the table_name, bucket_name, etl_batch_date in variables
table_name ="Orders"
bucket_name = "spoorthyetl"
etl_batch_date = redshift_conn.etl_batch_date;

# Getting data from Oracle DB and storing in CSV File
def get_csvdata():
    # Specifying Oracle Credentials & Oracle Client
    un = 'g23konda'
    cs = '54.224.209.13:1521/xe'
    pw = 'g23konda123'
    d1 = r"C:\Users\saispoorthy.konda\Downloads\instantclient-basic-windows.x64-21.12.0.0.0dbru\instantclient_21_12"
    oracledb.init_oracle_client(lib_dir=d1)

    # creating a oracle connection
    with oracledb.connect(user=un, password=pw, dsn=cs) as connection:
        # creating a cursor object
        with connection.cursor() as cursor:
            # executing query to get data from oracle db
            cursor.execute(f'''
            SELECT
            ordernumber,
            orderdate,
            requireddate,
            shippeddate,
            status,
            customernumber,
            create_timestamp,
            update_timestamp,
            cancelleddate              
            FROM {table_name}@konda_dblink_classicmodels
            WHERE to_char(update_timestamp, 'yyyy-mm-dd') >= '{etl_batch_date}'
            ''')
            # extracting the data from cursor and storing in CSV files
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            with open(f'{table_name}.csv','w', newline="") as file:
                csv_writer = csv.writer(file)
                csv_writer.writerow(column_names)
                csv_writer.writerows(rows)
# creating folders in bucket
def create_bucketfolders():
    s3 = boto3.client('s3')
    x = db.schema_name.replace("cm_", "")
    folder_name = f'{table_name}/{x}'
    s3.put_object(Bucket=bucket_name, Key=(folder_name+'/'))
    return folder_name
# uploading CSV Files into bucket
def upload_file(filename,folder,object_name=None):
    s3_client = boto3.client('s3')
    if object_name is None:
        object_name = os.path.basename(filename)
    try:
        s3_key = folder + object_name
        response = s3_client.upload_file(filename, bucket_name,s3_key)
    except ClientError as e:
        logging.error(e)
        return False
    return True
get_csvdata()
folder = create_bucketfolders()
folder = folder + '/'
print(folder)
upload_file(f'C:\\Users\\saispoorthy.konda\\Downloads\\Pratice\\sample\\{table_name}.csv',folder)
