import logging
import csv
import boto3
from botocore.exceptions import ClientError
import os
def upload_file(file_name,bucket,folder,object_name=None):

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        s3_key = folder + "cm20050609.csv"
        response = s3_client.upload_file(file_name, bucket,s3_key)
    except ClientError as e:
        logging.error(e)
        return False
    return True
files=["Offices", "Productlines", "Employees", "Customers", "Payments","Orders","Products", "Orderdetails","Calendar"]
folders = ["Offices/", "Productlines/", "Employees/", "Customers/", "Payments/","Orders/","Products/", "Orderdetails/","Calendar/"]
len1 = len(files)
c1 = [item for item in range(1, len1+1)]
for x1,y1 in zip(c1,folders):
    for x2,y2 in zip(c1,files):
        if x1 == x2:
            upload_file(f'C:\\Users\\saispoorthy.konda\\Downloads\\Pratice\\sample\\{y2}.csv',"spoorthyetl",y1,f'{y2}.csv')





