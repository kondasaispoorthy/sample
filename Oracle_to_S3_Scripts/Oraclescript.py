#import Oracle_to_s3_Customers,Oracle_to_s3_Employees,Oracle_to_s3_Offices,Oracle_to_s3_OrderDetails
#import Oracle_to_s3_Orders,Oracle_to_s3_Payments,Oracle_to_s3_products,Oracle_to_s3_productline
import concurrent.futures
import subprocess

def run_script(script_name):
    command = f"python Oracle_to_S3_Scripts/{script_name}.py"
    # You can use subprocess.run or os.system to run the command
    subprocess.run(command, shell=True)
    return 1

if __name__ == '__main__':
    script_names = [
        'Oracle_to_s3_Offices',
        'Oracle_to_s3_Employees',
        'Oracle_to_s3_Customers',
        'Oracle_to_s3_Payments',
        'Oracle_to_s3_Orders',
        'Oracle_to_s3_OrderDetails',
        'Oracle_to_s3_productline',
        'Oracle_to_s3_products'
    ]

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        # Use executor.map to parallelize the execution of run_script
        results = list(executor.map(run_script, script_names))




   
