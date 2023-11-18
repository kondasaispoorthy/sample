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

    # SQL COPY command to load data from Stage to Prod in Redshift
    copy_sql = f"""
UPDATE dev_dw.employees a
SET
lastName = b.lastName,
firstName = b.firstName,
extension = b.extension,
email = b.email,
officeCode = b.officeCode,
reportsTo =  b.reportsto,
jobTitle = b.jobTitle,
src_update_timestamp = b.update_timestamp,
dw_update_timestamp = current_timestamp
FROM dev_stage.employees b 
WHERE a.employeeNumber = b.employeeNumber;
INSERT INTO dev_dw.employees (
employeeNumber,
lastName,
firstName,
extension,
email,
officeCode,
reportsTo,
jobTitle,
dw_office_id,
src_create_timestamp,
src_update_timestamp
)
SELECT
a.employeeNumber,
a.lastName,
a.firstName,
a.extension,
a.email,
a.officeCode,
a.reportsTo,
a.jobTitle,
c.dw_office_id,
a.create_timestamp,
a.update_timestamp
FROM
dev_stage.employees a LEFT JOIN dev_dw.employees b
ON a.employeeNumber = b.employeeNumber
JOIN dev_dw.offices c ON 
a.officeCode = c.officeCode
WHERE b.employeeNumber IS NULL;
    """

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print("Employees shifted successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()