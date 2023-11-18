import psycopg2 as pg
import boto3
import pandas as pd

# Redshift connection parameters
host = 'spoorthy-workgroup.854668443937.us-east-1.redshift-serverless.amazonaws.com'
database = 'dev'
user = 'admin'
password = 'Spoorthy123' # Leave this empty if using AWS CLI for authentication
port = '5439'  # Adjust the port as needed
s3 = boto3.client('s3') 
bucket_name = "spoorthyetl"

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
     # selecting values from batch_control table
    cursor.execute(f"select * FROM etl_metadata.batch_control")

    # Convert the results of the SQL query into a pandas DataFrame.
    df = pd.DataFrame(cursor.fetchall(), columns=list(map(lambda col: col[0],cursor.description)))

    # Extracting etl_batch_no and etl_batch_date from DataFrame
    etl_batch_no = df.etl_batch_no[0]
    etl_batch_date = df.etl_batch_date[0]
    #print(f"etl_batch_no and etl_batch_date are {etl_batch_no} and {etl_batch_date} respectively")
    # SQL COPY command to load data from S3 to Redshift
    copy_sql = f"""
    UPDATE dev_dw.orders a 
SET
src_orderNumber = b.orderNumber,
orderDate = b.orderDate,
requiredDate = b.requiredDate,
shippedDate = b.shippedDate,
status = b.status,
comments = b.comments,
src_customerNumber = b.customerNumber,
src_update_timestamp = b.update_timestamp,
dw_update_timestamp = current_timestamp,
cancelledDate = b.cancelledDate
FROM dev_stage.orders b
WHERE a.src_orderNumber = b.orderNumber;
INSERT INTO dev_dw.orders(
dw_customer_id,
src_orderNumber,
orderDate,
requiredDate,
shippedDate,
status,
comments,
src_customerNumber,
src_create_timestamp,
src_update_timestamp,
cancelledDate,
etl_batch_no,
etl_batch_date
)
SELECT 
c.dw_customer_id,
a.orderNumber,
a.orderDate,
a.requiredDate,
a.shippedDate,
a.status,
a.comments,
a.customerNumber,
a.create_timestamp,
a.update_timestamp,
a.cancelledDate,
{etl_batch_no},
cast('{etl_batch_date}' as date)
FROM 
dev_stage.orders a 
LEFT JOIN dev_dw.orders b
ON a.orderNumber = b.src_orderNumber
JOIN dev_dw.customers c ON
a.customerNumber = c.src_customerNumber
WHERE b.src_orderNumber IS NULL;
    """

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print("Orders shifted successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()