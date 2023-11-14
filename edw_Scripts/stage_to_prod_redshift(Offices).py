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
# SQL COPY command to load data from S3 to Redshift
copy_sql = f"""
INSERT INTO prod.offices(
officeCode,
city,
phone,
addressLine1,
addressLine2,
state,
country,
postalCode,
territory,
src_create_timestamp,
src_update_timestamp
)
select
officeCode,
city,
phone,
addressLine1,
addressLine2,
state,
country,
postalCode,
territory,
create_timestamp,
update_timestamp
from stage.offices;
"""
# Connecting to redshift table
try:
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