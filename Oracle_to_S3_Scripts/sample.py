import boto3
s3 = boto3.client('s3')
bucket_name = "spoorthyetl"
folder_name = "Offices1/20050609"
s3.put_object(Bucket=bucket_name, Key=(folder_name+'/'))