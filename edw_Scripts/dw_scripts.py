import subprocess
def run_scripts(script_name):
    command = f"python edw_Scripts/{script_name}.py"
    process = subprocess.Popen(command, shell=True)
    process.wait()
scripts = ['stage_to_prod_redshift_Offices','stage_to_prod_redshift_Employees','stage_to_prod_redshift_Customers',
           'stage_to_prod_redshift_Payments','stage_to_prod_redshift_Orders','stage_to_prod_redshift_productlines',
             'stage_to_prod_redshift_products','stage_to_prod_redshift_Orderdetails',
             'customerhistory','producthistory','daily_customer_summary','monthly_customer_summary',
             'daily_product_summary','monthly_product_summary']
for script_name in scripts:
    run_scripts(script_name)
