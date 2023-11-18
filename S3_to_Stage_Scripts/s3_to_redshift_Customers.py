# Importing the Required Modules
import psycopg2 as pg
import boto3
import sys
import pandas as pd
sys.path.append('C:/Users/saispoorthy.konda/Downloads/Pratice/sample')
import db

# Redshift connection parameters
host = 'spoorthy-workgroup.854668443937.us-east-1.redshift-serverless.amazonaws.com'
database = 'dev'
user = 'admin'
password = 'Spoorthy123' # Leave this empty if using AWS CLI for authentication
port = '5439'  # Adjust the port as needed
s3 = boto3.client('s3')

# Specifying the file_path,bucket_name and table_name parameters
bucket_name = "spoorthyetl"
table_name = "customers"
table_nm = table_name.capitalize()
#schema = db.schema_name.replace("cm_","")

#file_path = f"{table_nm}/{schema}/{table_nm}.csv"
#print(f"filepath is :{file_path}")
try:
    # Connect to Redshift
    conn = pg.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port,
    )
    cursor = conn.cursor()

    # selecting values from batch_control table
    cursor.execute(f"select * FROM etl_metadata.batch_control")
    #print("Query executed successfully")

    # Convert the results of the SQL query into a pandas DataFrame.
    df = pd.DataFrame(cursor.fetchall(), columns=list(map(lambda col: col[0],cursor.description)))

    # storing etl_batch_no and etl_batch_date in variables
    etl_batch_no = df.etl_batch_no[0]
    etl_batch_date = df.etl_batch_date[0]

    #schema = db.schema_name.replace("cm_","")
    file_path = f"{table_nm}/{etl_batch_date}/{table_nm}.csv"
    #print(f"filepath is :{file_path}")
    # SQL COPY command to load data from S3 to Redshift
    copy_sql = f"""
    COPY dev.dev_stage.{table_name}(
    customernumber,
    customername,
    contactlastname,
    contactfirstname,
    phone,
    addressline1,
    addressline2,
    city,
    state,
    postalcode,
    country,
    salesrepemployeenumber,
    creditlimit,
    create_timestamp,
    update_timestamp
    )
    FROM 's3://{bucket_name}/{file_path}' IAM_ROLE 'arn:aws:iam::854668443937:role/service-role/AmazonRedshift-CommandsAccessRole-20231102T150508'  
    ACCEPTINVCHARS 
    FORMAT AS CSV DELIMITER
    ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'eu-north-1';
    """
    # Truncate the table in stage(if exists)
    cursor.execute(f"TRUNCATE TABLE dev_stage.{table_name}")

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print(f"{table_name} Data loaded successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()