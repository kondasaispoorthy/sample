import multiprocessing,time
import subprocess
def subprocess1(script_name):
    command = f"python edw_Scripts/{script_name}.py"
    process = subprocess.Popen(command, shell=True)
    process.wait()
def mainprocess(l):
    for each in l:
        subprocess1(each)
def sub(path):
    path1 = f"edw_Scripts/{path}.py"
    subprocess.run(["python", path1])
def sub1(list1):
    processes = []
    for i in range(len(list1)):  # Number of processes
        process = multiprocessing.Process(target=sub,args = (list1[i],))
        processes.append(process)
        process.start()
    for process in processes:
        process.join()
if __name__ == '__main__':
    l1 = ['stage_to_prod_redshift_Offices','stage_to_prod_redshift_Employees','stage_to_prod_redshift_Customers','stage_to_prod_redshift_Orders'] 
    l2 = ['stage_to_prod_redshift_productlines','stage_to_prod_redshift_products','producthistory']
    l3 = ['stage_to_prod_redshift_Payments','stage_to_prod_redshift_Orderdetails','customerhistory']
    l4 = ['daily_customer_summary','monthly_customer_summary','daily_product_summary','monthly_product_summary']
    p1 = multiprocessing.Process(target=mainprocess,args = (l1,))
    p2 = multiprocessing.Process(target=mainprocess,args = (l2,))
    p3 = multiprocessing.Process(target=sub1,args = (l3,))
    p4 = multiprocessing.Process(target=mainprocess,args = (l4,))

    p1.start()
    p2.start()
    p1.join()
    p2.join()
    p3.start()
    p3.join()
    p4.start()
    p4.join()
