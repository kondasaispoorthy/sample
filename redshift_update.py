# Importing the Required Modules
import psycopg2 as pg
import boto3
import pandas as pd

# Redshift connection parameters
host = 'spoorthy-workgroup.854668443937.us-east-1.redshift-serverless.amazonaws.com'
database = 'dev'
user = 'admin'
password = 'Spoorthy123' # Leave this empty if using AWS CLI for authentication
port = '5439'  # Adjust the port as needed
def update_shift(type):
    s3 = boto3.client('s3')
    try:
        # Connecting to Redshift
        conn = pg.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
        )
        cursor = conn.cursor()
        if type == "increment":
            # selecting values from batch_control table
            cursor.execute(f''' UPDATE etl_metadata.batch_control
            SET etl_batch_no = etl_batch_no + 1,
        etl_batch_date = DATEADD(day, 1,etl_batch_date)
    ''')
        elif type == "full":
               cursor.execute(f''' UPDATE etl_metadata.batch_control
            SET etl_batch_no = etl_batch_no + 1,
        etl_batch_date = DATE '2005-06-10'
    ''')
        cursor.execute(f"commit")
        cursor.execute(f"select * FROM etl_metadata.batch_control")
        #print("Query executed successfully")
        # Convert the results of the SQL query into a pandas DataFrame.
        df = pd.DataFrame(cursor.fetchall(), columns=list(map(lambda col: col[0],cursor.description)))
        etl_batch_no = df.etl_batch_no[0]
        etl_batch_date = df.etl_batch_date[0]
        print(f"etl_batch_no and etl_batch_date are {etl_batch_no} and {etl_batch_date} respectively")
    except Exception as e:
        print(f"Error: {str(e)}")
    conn.close()
#update_shift("full")