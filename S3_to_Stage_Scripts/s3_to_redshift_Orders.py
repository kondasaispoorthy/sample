import boto3
import pandas as pd
import io
import psycopg2 as pg
import sys
sys.path.append('C:/Users/saispoorthy.konda/Downloads/Pratice/sample')
#import db
s3 = boto3.client('s3')
import pandas as pd

# Specify the S3 bucket, table_name,schema, file_key
bucket_name = 'spoorthyetl'
table_name = "orders"
table_nm = table_name.capitalize()
#schema = db.schema_name.replace("cm_","")
#file_key = f"{table_nm}/{schema}/{table_nm}.csv"
#print(f"filepath is :{file_key}")
#redshift_table = file_key.split("/")[0].lower()

# Redshift connection parameters
host = 'spoorthy-workgroup.854668443937.us-east-1.redshift-serverless.amazonaws.com'
database = 'dev'
user = 'admin'
password = 'Spoorthy123' # Leave this empty if using AWS CLI for authentication
port = '5439'  # Adjust the port as needed
s3 = boto3.client('s3') 

# Returns the column names from CSV File
def return_columns(file_key):
   # Read the CSV file from S3
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response['Body'].read().decode('utf-8')
    # Create a Pandas DataFrame
    df = pd.read_csv(io.StringIO(csv_content))
    c_names = df.columns.tolist()
    s = ",".join(c_names)
    s = s.replace("["," ")
    s = s.replace("]"," ")
    return s
try:
    # Connect to Redshift
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
    #print("Query executed successfully")

    # Convert the results of the SQL query into a pandas DataFrame.
    df = pd.DataFrame(cursor.fetchall(), columns=list(map(lambda col: col[0],cursor.description)))

    # storing etl_batch_no and etl_batch_date in variables
    etl_batch_no = df.etl_batch_no[0]
    etl_batch_date = df.etl_batch_date[0]

    file_path = f"{table_nm}/{etl_batch_date}/{table_nm}.csv"
    print(f"filepath is :{file_path}")
    val = return_columns(file_path)

    # Truncate the table(if exists)
    cursor.execute(f"TRUNCATE TABLE dev_stage.{table_name}")

    # SQL COPY command to load data from S3 to Redshift
    copy_sql = f"""
    COPY dev.dev_stage.{table_name}(
    {val}
    )
    FROM 's3://{bucket_name}/{file_path}' IAM_ROLE 'arn:aws:iam::854668443937:role/service-role/AmazonRedshift-CommandsAccessRole-20231102T150508'  
    ACCEPTINVCHARS 
    FORMAT AS CSV DELIMITER
    ',' QUOTE '"' IGNOREHEADER 1 REGION AS 'eu-north-1';
"""

    # Execute the COPY command to load data from S3
    cursor.execute(copy_sql)
    conn.commit()

    print(f"{table_name} Data loaded successfully into Redshift.")

except Exception as e:
    print(f"Error: {str(e)}")
conn.close()
