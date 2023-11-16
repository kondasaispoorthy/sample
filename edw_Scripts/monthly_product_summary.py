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
    print(f"etl_batch_no and etl_batch_date are {etl_batch_no} and {etl_batch_date} respectively")

    # SQL COPY command to load data from S3 to Redshift
    copy_sql = f"""
    INSERT INTO prod.monthly_product_summary
(
start_of_the_month_date,
dw_product_id,
customer_apd,
customer_apm,
product_order_amount,
product_cost_amount,
product_msrp_amount,
cancelled_product_qty,
cancelled_order_amount,
cancelled_cost_amount,
cancelled_mrp_amount,
cancelled_order_apd,
customer_order_apm,
etl_batch_no,
etl_batch_date
)
SELECT DATE_TRUNC('MONTH',orderDate) start_of_the_month_date,
dw_product_id,
SUM(customer_apd) as customer_apd,
CASE 
WHEN SUM(customer_apd) > 0 
THEN 1 ELSE 0 
END as customer_apm,
SUM(product_order_amount) as product_order_amount,
SUM(product_cost_amount) as product_cost_amount,
SUM(product_MSRP_amount) as product_msrp_amount,
SUM(cancelled_product_qty) as cancelled_product_qty,
SUM(cancelled_order_amount) as cancelled_order_amount,
SUM(cancelled_cost_amount) as cancelled_cost_amount,
SUM(cancelled_msrp_amount) as cancelled_mrp_amount,
SUM(cancelled_order_apd) as cancelled_order_apd,
CASE
WHEN SUM(cancelled_order_apd) > 0
THEN 1 ELSE 0
END as customer_order_apm,
{etl_batch_no} as etl_batch_no,
cast('{etl_batch_date}' as date) as etl_batch_date
FROM prod.daily_product_summary
GROUP BY 1,2
ORDER BY 1,2;
    """

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print("Data loaded successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()