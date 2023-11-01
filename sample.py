import boto3

def create_s3_folder(bucket_name, folder_name):
    s3 = boto3.client('s3')

    
    if not folder_name.endswith('/'):
        folder_name += '/'

    # Create an empty object (simulating a folder) in the S3 bucket
    try:
        s3.put_object(Bucket=bucket_name, Key=folder_name)
        print(f"Folder '{folder_name}' created successfully in S3 bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Error creating folder: {e}")

# Replace 'your_bucket_name' with your actual S3 bucket name
bucket_name = 'madhura-s3bucket'
folders=['customers','offices','employees','orders','orderdetails','payments','products','productlines']
# Replace 'your_folder_name' with your desired folder name
for i in folders:
    folder_name = i
    create_s3_folder(bucket_name, folder_name)
