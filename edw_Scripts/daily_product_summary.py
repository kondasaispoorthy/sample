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
    INSERT into prod.daily_product_summary
(
orderDate,
dw_product_id,
customer_apd,
product_order_amount,
product_cost_amount,
product_MSRP_amount,
cancelled_product_qty,
cancelled_order_amount,
cancelled_cost_amount,
cancelled_MSRP_amount,
cancelled_order_apd,
etl_batch_no,
etl_batch_date
)
select orderDate as summarydate,dw_product_id,
MAX(customer_apd) as customer_apd,
MAX(product_order_amount) as product_order_amount,
MAX(product_cost_amount) as product_cost_amount,
MAX(product_MSRP_amount) as product_MSRP_amount,
SUM(cancelled_product_qty) as cancelled_product_qty,
MAX(cancelled_order_amount) as cancelled_order_amount,
MAX(cancelled_cost_amount) as cancelled_cost_amount,
MAX(cancelled_MSRP_amount) as cancelled_MSRP_amount,
MAX(cancelled_order_apd) as cancelled_order_apd,
{etl_batch_no} as etl_batch_no,
cast('{etl_batch_date}' as date) as etl_batch_date
FROM (
select o.orderDate,p.dw_product_id,
COUNT(DISTINCT dw_customer_id) as customer_apd,
SUM(od.quantityOrdered * od.priceEach) as product_order_amount,
SUM(od.quantityOrdered * p.buyPrice) as product_cost_amount,
SUM(od.quantityOrdered * p.MSRP) as product_MSRP_amount,
0 as cancelled_product_qty,
0 as cancelled_order_amount,
0 as cancelled_cost_amount,
0 as cancelled_MSRP_amount,
0 as cancelled_order_apd
from prod.orders o INNER JOIN prod.orderdetails od ON 
o.dw_order_id = od.dw_order_id
INNER JOIN prod.products p ON
od.dw_product_id = p.dw_product_id
WHERE o.orderDate >= cast('{etl_batch_date}' as date)
GROUP BY 1,2
UNION ALL
select o.cancelledDate,
p.dw_product_id,
0 as customer_apd,
0 as product_order_amount,
0 as product_cost_amount,
0 as product_MSRP_amount,
COUNT(p.dw_product_id) as cancelled_product_qty,
SUM(od.quantityOrdered * od.priceEach) as cancelled_order_amount,
SUM(od.quantityOrdered * p.buyPrice) as cancelled_cost_amount,
SUM(od.quantityOrdered * p.MSRP) as cancelled_MSRP_amount,
1 as cancelled_order_apd
FROM 
prod.orders o INNER JOIN prod.orderdetails od 
ON o.dw_order_id = od.dw_order_id
INNER JOIN prod.products p ON
od.dw_product_id = p.dw_product_id
WHERE o.cancelledDate >= cast('{etl_batch_date}' as date)
GROUP BY 1,2) x
GROUP BY 1,2;
    """

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print("DPS loaded successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()