import psycopg2 as pg
import boto3

# Redshift connection parameters
host = 'default-workgroup.854668443937.eu-north-1.redshift-serverless.amazonaws.com'
database = 'dev'
user = 'admin'
password = 'Spoorthy123' # Leave this empty if using AWS CLI for authentication
port = '5439'  # Adjust the port as needed
s3 = boto3.client('s3') 
bucket_name = "spoorthyetl"
table_name = "Products"
# SQL COPY command to load data from S3 to Redshift
copy_sql = f"""
COPY dev.stage.products(
productcode,
productname,
productline,
productscale,
productvendor,
quantityinstock,
buyprice,
msrp,
create_timestamp,
update_timestamp
)
FROM 's3://{bucket_name}/{table_name}/20050609/{table_name}.csv' IAM_ROLE 'arn:aws:iam::854668443937:role/service-role/AmazonRedshift-CommandsAccessRole-20231102T150508'  
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

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print("Data loaded successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()