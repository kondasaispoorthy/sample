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
INSERT INTO prod.payments(
dw_customer_id,
src_customerNumber,
checkNumber,
paymentDate,
amount,
src_create_timestamp,
src_update_timestamp
)
SELECT 
c.dw_customer_id,
a.customerNumber,
a.checkNumber,
a.paymentDate,
a.amount,
a.create_timestamp,
a.update_timestamp
FROM
stage.payments a 
JOIN prod.customers c ON 
a.customerNumber = c.src_customerNumber;
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