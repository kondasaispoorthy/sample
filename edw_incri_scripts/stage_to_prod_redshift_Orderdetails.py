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

    # SQL  command to transfer from stage_to_prod in redshift
    copy_sql = f"""
    UPDATE dev_dw.orderdetails a1 
SET
src_productCode = b1.productCode,
quantityOrdered = b1.quantityOrdered,
priceEach = b1.priceEach,
orderLineNumber = b1.orderLineNumber,
src_update_timestamp = b1.update_timestamp,
dw_update_timestamp = current_timestamp
FROM dev_stage.orderdetails b1
WHERE a1.src_orderNumber = b1.orderNumber;
INSERT INTO dev_dw.orderdetails(
dw_order_id,
dw_product_id,
src_orderNumber,
src_productCode,
quantityOrdered,
priceEach,
orderLineNumber,
src_create_timestamp,
src_update_timestamp,
etl_batch_no,
etl_batch_date
)
SELECT
c.dw_order_id,
d.dw_product_id,
a.orderNumber,
a.productCode,
a.quantityOrdered,
a.priceEach,
a.orderLineNumber,
a.create_timestamp,
a.update_timestamp,
{etl_batch_no},
cast('{etl_batch_date}' as date)
FROM
dev_stage.orderdetails a LEFT JOIN dev_dw.orderdetails b
ON a.orderNumber = b.src_orderNumber
JOIN dev_dw.orders c 
ON a.orderNumber = c.src_orderNumber
JOIN dev_dw.products d ON
a.productCode = d.src_productCode
WHERE b.src_orderNumber IS NULL;


    """

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print("OrderDetails shifted successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()