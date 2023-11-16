import psycopg2 as pg
import boto3
import sys
sys.path.append('C:/Users/saispoorthy.konda/Downloads/Pratice/sample')
import db

# Redshift connection parameters
host = 'spoorthy-workgroup.854668443937.us-east-1.redshift-serverless.amazonaws.com'
database = 'dev'
user = 'admin'
password = 'Spoorthy123' # Leave this empty if using AWS CLI for authentication
port = '5439'  # Adjust the port as needed
s3 = boto3.client('s3') 

# specifying bucket_name,table_name,schema respectively
bucket_name = "spoorthyetl"
table_name = "orderdetails"
table_nm = table_name.capitalize()
schema = db.schema_name.replace("cm_","")
file_path = f"{table_nm}/{schema}/{table_nm}.csv"
print(f"filepath is :{file_path}")

# SQL COPY command to load data from S3 to Redshift
copy_sql = f"""
COPY dev.stage.{table_name}(
ordernumber,
productcode,
quantityordered,
priceeach,
orderlinenumber,
create_timestamp,
update_timestamp
)
FROM 's3://{bucket_name}/{file_path}' IAM_ROLE 'arn:aws:iam::854668443937:role/service-role/AmazonRedshift-CommandsAccessRole-20231102T150508'  
ACCEPTINVCHARS 
FORMAT AS CSV DELIMITER
 ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'eu-north-1';
"""
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

    # Truncate the table(if exists)
    cursor.execute(f"TRUNCATE TABLE stage.{table_name}")

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print("Data loaded successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()