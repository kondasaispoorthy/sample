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
    UPDATE dev_dw.product_history a
SET dw_active_record_ind = 0,
effective_to_date = DATEADD(DAY,-1,cast('{etl_batch_date}' as date)),
dw_update_timestamp = CURRENT_TIMESTAMP,
update_etl_batch_no = {etl_batch_no},
update_etl_batch_date = cast('{etl_batch_date}' as date)
FROM dev_dw.products b 
WHERE a.dw_product_id = b.dw_product_id AND
a.MSRP <> b.MSRP AND 
a.dw_active_record_ind  = 1;

INSERT INTO dev_dw.product_history(
dw_product_id,
MSRP,
effective_from_date,
dw_active_record_ind,
create_etl_batch_no,
create_etl_batch_date
)
select
c.dw_product_id,
c.MSRP,
cast('{etl_batch_date}' as date),
1 dw_active_record_ind,
{etl_batch_no} create_etl_batch_no,
cast('{etl_batch_date}' as date) create_etl_batch_date
FROM 
dev_dw.products c
LEFT JOIN
(select dw_product_id from dev_dw.product_history where 
dw_active_record_ind = 1) d
ON c.dw_product_id = d.dw_product_id
WHERE d.dw_product_id IS NULL;
    """

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print("product history loaded successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()