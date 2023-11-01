import boto3
s3 = boto3.client('s3')
bucket_name = "spoorthyetl"
folders=["Offices", "Productlines", "Employees", "Customers", "Payments","Orders","Products", "Orderdetails","Calendar"]
for folder_name in folders:
    s3.put_object(Bucket=bucket_name, Key=(folder_name+'/'))