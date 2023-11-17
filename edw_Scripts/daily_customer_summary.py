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
    INSERT into prod.daily_customer_summary 
    (
    summarydate,
    dw_customer_id,
    order_count,
    order_apd,
    ordered_amt,
    order_cost_amount,
    order_mrp_amount,
    products_ordered_qty,
    products_items_qty,
    cancelled_order_count,
    cancelled_order_amount,
    cancelled_order_apd,
    shipped_order_count,
    shipped_order_amount,
    shipped_order_apd,
    payment_apd,
    payment_amount,
    new_customer_apd,
    etl_batch_no,
    etl_batch_date
    )
    select 
    a.summarydate,
    a.dw_customer_id,
    MAX(a.order_count) as order_count,
    MAX(a.order_apd) as order_apd,
    MAX(a.ordered_amt) as ordered_amt,
    MAX(a.order_cost_amount) as order_cost_amount,
    MAX(a.order_mrp_amount) as order_mrp_amount,
    MAX(a.products_ordered_qty) as products_ordered_qty,
    MAX(products_items_qty) as products_items_qty,
    MAX(cancelled_order_count) as cancelled_order_count,
    MAX(cancelled_order_amount) as cancelled_order_amount,
    MAX(cancelled_order_apd) as cancelled_order_apd,
    MAX(shipped_order_count) as shipped_order_count,
    MAX(shipped_order_amount) as shipped_order_amount,
    MAX(shipped_order_apd) as shipped_order_apd,
    MAX(payment_apd) as payment_apd,
    MAX(payment_amount) as payment_amount,
    MAX(new_customer_apd) as new_customer_apd,
    {etl_batch_no} as etl_batch_no,
    cast('{etl_batch_date}' as date) as etl_batch_date
    FROM
    (select o.orderDate as summarydate,
    c.dw_customer_id,
    COUNT(DISTINCT od.src_orderNumber) as order_count,
    1 as order_apd,
    SUM(od.quantityOrdered * od.priceEach) as ordered_amt,
    SUM(od.quantityOrdered * p.buyPrice) as order_cost_amount,
    SUM(od.quantityOrdered * p.MSRP) as order_mrp_amount,
    COUNT(od.src_productCode) as products_ordered_qty,
    COUNT(DISTINCT p.ProductLine) as products_items_qty,
    0 as cancelled_order_count,
    0 as cancelled_order_amount,
    0 as cancelled_order_apd,
    0 as shipped_order_count,
    0 as shipped_order_amount,
    0 as shipped_order_apd,
    0 as payment_apd,
    0 as payment_amount,
    0 as new_customer_apd 
    FROM
    prod.customers c INNER JOIN prod.orders o
    ON c.dw_customer_id = o.dw_customer_id
    INNER JOIN prod.orderdetails od 
    ON o.dw_order_id = od.dw_order_id
    INNER JOIN prod.products p 
    ON od.dw_product_id = p.dw_product_id
    WHERE o.orderDate >= cast('{etl_batch_date}' as date)
    GROUP BY 1,2
    UNION ALL
    select o.cancelledDate,
    c.dw_customer_id,
    0 as order_count,
    0 as order_apd,
    0 as order_amt,
    0 as order_cost_amount,
    0 as order_mrp_amount,
    0 as products_ordered_qty,
    0 as products_items_qty ,
    COUNT(DISTINCT o.src_orderNumber) as cancelled_order_count,
    SUM(od.quantityOrdered * od.priceEach) as cancelled_order_amount,
    1 as cancelled_order_apd,
    0 as shipped_order_count,	
    0 as shipped_order_amount,	
    0 as shipped_order_apd,	
    0 as payment_apd,	
    0 as payment_amount,	
    0 as new_customer_apd 
    FROM 
    prod.customers c INNER JOIN prod.orders o 
    ON c.dw_customer_id = o.dw_customer_id
    INNER JOIN prod.orderdetails od ON
    o.dw_order_id = od.dw_order_id
    WHERE o.cancelledDate >= cast('{etl_batch_date}' as date)
    GROUP BY 1,2
    UNION ALL
    select o.shippedDate,o.dw_customer_id,
    0 as order_count,	
    0 as order_apd,	
    0 as order_amt,	
    0 as order_cost_amount,	
    0 as order_mrp_amount,	
    0 as products_ordered_qty,	
    0 as products_items_qty,	
    0 as cancelled_order_count,	
    0 as cancelled_order_amount,	
    0 as cancelled_order_apd,	
    COUNT(DISTINCT o.src_orderNumber) as shipped_order_count,
    SUM(od.quantityOrdered * od.priceEach) as shipped_order_amount,
    1 as shipped_order_apd,
    0 as payment_apd,	
    0 as payment_amount,	
    0 as new_customer_apd
    FROM prod.orders o
    INNER JOIN prod.orderdetails od ON
    o.dw_order_id = od.dw_order_id
    WHERE o.shippedDate >= cast('{etl_batch_date}' as date)
    GROUP BY 1,2
    UNION ALL
    select paymentDate,
    dw_customer_id,
    0 as order_count,	
    0 as order_apd,	
    0 as order_amt,	
    0 as order_cost_amount,	
    0 as order_mrp_amount,	
    0 as products_ordered_qty,	
    0 as products_items_qty,	
    0 as cancelled_order_count,	
    0 as cancelled_order_amount,	
    0 as cancelled_order_apd,	
    0 as shipped_order_count,	
    0 as shipped_order_amount,	
    0 as shipped_order_apd,	
    1 as payment_apd,
    SUM(amount) as payment_amt,
    0 as new_customer_apd 
    FROM prod.payments
    WHERE paymentDate >= cast('{etl_batch_date}' as date)
    GROUP BY 1,2
    UNION ALL
    select DATE(src_create_timestamp),
    dw_customer_id,
    0 as order_count,
    0 as order_apd,
    0 as order_amt,
    0 as order_cost_amount,
    0 as order_mrp_amount,
    0 as products_ordered_qty,
    0 as products_items_qty,
    0 as cancelled_order_count,
    0 as cancelled_order_amount,
    0 as cancelled_order_apd,
    0 as shipped_order_count,
    0 as shipped_order_amount,
    0 as shipped_order_apd,
    0 as payment_apd,
    0 as payment_amount,
    1 as new_customer_apd
    FROM prod.customers
    WHERE DATE(src_create_timestamp)>= cast('{etl_batch_date}' as date)
    GROUP BY 1,2 ) a
    GROUP BY 1,2;
    UPDATE prod.daily_customer_summary dcs1
    set new_customer_paid_apd = 1
    FROM 
    (SELECT t1.dw_customer_id,
    t1.fod
    FROM (SELECT dw_customer_id,
                MIN(summarydate) AS fod
        FROM prod.daily_customer_summary
        WHERE order_apd = 1
        GROUP BY 1) t1
    WHERE t1.fod >= cast('{etl_batch_date}' as date)) dcs2
    WHERE dcs1.dw_customer_id=dcs2.dw_customer_id and dcs1.summarydate=dcs2.fod

        """

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print("DCS loaded successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()