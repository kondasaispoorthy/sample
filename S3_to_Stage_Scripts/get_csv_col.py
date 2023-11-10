import boto3
import pandas as pd
import io
# Initialize the S3 client
s3 = boto3.client('s3')
# Specify the S3 bucket and object key of the CSV file
bucket_name = 'spoorthyetl'
file_key = 'Payments/cm20050609.csv'
response = s3.get_object(Bucket=bucket_name, Key=file_key)
csv_content = response['Body'].read().decode('utf-8')
# Create a Pandas DataFrame
df = pd.read_csv(io.StringIO(csv_content))
print(df.head(5))
