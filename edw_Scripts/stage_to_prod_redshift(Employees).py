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
INSERT INTO prod.employees (
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
stage.employees a 
JOIN prod.offices c ON 
a.officeCode = c.officeCode;

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