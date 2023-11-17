#import s3_to_redshift_Offices,s3_to_redshift_employees,s3_to_redshift_Customers,s3_to_redshift_Payments
#import s3_to_redshift_Orders,s3_to_redshift_OrderDetails,s3_to_redshift_Products,s3_to_redshift_Productlines
import subprocess
import concurrent.futures
def run_script(script_name):
    command = f"python  S3_to_Stage_Scripts/{script_name}.py"
    # You can use subprocess.run or os.system to run the command
    subprocess.run(command, shell=True)
    return 1


script_names = [
        's3_to_redshift_Offices',
        's3_to_redshift_employees',
        's3_to_redshift_Customers',
        's3_to_redshift_Payments',
        's3_to_redshift_Orders',
        's3_to_redshift_OrderDetails',
        's3_to_redshift_Productlines',
        's3_to_redshift_Products'
]

with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    # Use executor.map to parallelize the execution of run_script
    results = list(executor.map(run_script, script_names))




   
