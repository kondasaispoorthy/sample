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

    # SQL command to load data into prod in redshift
    copy_sql = f"""
INSERT INTO dev_dw.monthly_customer_summary(
dw_customer_id,
summarydate,
order_count,
order_apd,
order_apm,
ordered_amt,
order_cost_amount,
order_mrp_amount,
products_ordered_qty,
products_items_qty,
cancelled_order_count,
cancelled_order_amount,
cancelled_order_apd,
cancelled_order_apm,
shipped_order_count,
shipped_order_amount,
shipped_order_apd,
shipped_order_apm,
payment_apd,
payment_apm,
payment_amount,
new_customer_apd,
new_customer_apm,
new_customer_paid_apd,
new_customer_paid_apm,
etl_batch_no,
etl_batch_date
)
select dw_customer_id,
DATE_TRUNC('MONTH', summarydate) summarydate,
SUM(order_count) as order_count,
SUM(order_apd) as order_apd,
CASE 
WHEN SUM(order_apd) > 0 THEN 1 ELSE 0 
END as order_apm,
SUM(ordered_amt) as ordered_amt,
SUM(order_cost_amount) as order_cost_amount,
SUM(order_mrp_amount) as order_mrp_amount,
SUM(products_ordered_qty) as products_ordered_qty,
SUM(products_items_qty) as products_items_qty,
SUM(cancelled_order_count) as cancelled_order_count,
SUM(cancelled_order_amount) as cancelled_order_amount,
SUM(cancelled_order_apd) as cancelled_order_apd,
CASE 
WHEN SUM(cancelled_order_apd) > 0 THEN 1 ELSE 0
END as cancelled_order_apm,
SUM(shipped_order_count) as shipped_order_count,
SUM(shipped_order_amount) as shipped_order_amount,
SUM(shipped_order_apd) as shipped_order_apd,
CASE 
WHEN SUM(shipped_order_apd) > 0 THEN 1 ELSE 0
END as shipped_order_apm,
SUM(payment_apd) as payment_apd,
CASE
WHEN SUM(payment_apd) > 0 THEN 1 ELSE 0
END as payment_apm,
SUM(payment_amount) as payment_amount,
SUM(new_customer_apd) as new_customer_apd,
CASE
WHEN SUM(new_customer_apd) > 0 THEN 1 ELSE 0
END as new_customer_apm,
SUM(new_customer_paid_apd) as new_customer_paid_apd,
CASE
WHEN SUM(new_customer_paid_apd) > 0 THEN 1 ELSE 0
END as new_customer_paid_apm,
{etl_batch_no} as etl_batch_no,
cast('{etl_batch_date}' as date) as etl_batch_date
FROM  dev_dw.daily_customer_summary
GROUP BY 1,2
ORDER BY 1,2;
        """

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print("MCS loaded successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()