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
    UPDATE prod.offices o1
SET city = o2.city,
phone = o2.phone,
addressLine1 = o2.addressLine1,
addressLine2 = o2.addressLine2,
state = o2.state,
country = o2.country,
postalCode = o1.postalCode,
territory = o2.territory,
src_update_timestamp = o2.update_timestamp,
dw_update_timestamp = current_timestamp
FROM stage.offices o2
where o1.officeCode = o2.officecode;
insert into prod.offices
(officeCode,
city,
phone,
addressLine1,
addressLine2,
state,
country,
postalCode,
territory,
src_create_timestamp,
src_update_timestamp,
etl_batch_no,
etl_batch_date
)
select 
a.officeCode,
a.city,
a.phone,
a.addressLine1,
a.addressLine2,
a.state,
a.country,
a.postalCode,
a.territory,
a.create_timestamp,
a.update_timestamp,
{etl_batch_no},
cast('{etl_batch_date}' as date)
from 
stage.offices a LEFT JOIN prod.offices b
ON a.officeCode=b.OfficeCode
where b.officeCode is null;
    """
    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print("Data loaded successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()