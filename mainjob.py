import subprocess
import multiprocessing
import db
import redshift_update

def run_script(path):
    subprocess.run(["python", path])
def link(schema,password):
    db.change_dblink(schema,password)
def update():
    redshift_update.update_shift("full")
    print("I ran")

if __name__ == '__main__':
    p1 = multiprocessing.Process(target=link, args=("cm_20050609","cm_20050609123"))
    p2 = multiprocessing.Process(target=run_script, args=("batch_control_log_start.py",))
    p3 = multiprocessing.Process(target=run_script, args=("./Oracle_to_S3_Scripts/Oraclescript.py",))
    p4 = multiprocessing.Process(target=run_script, args=("./S3_to_stage_Scripts/stage.py",))
    #p5 = multiprocessing.Process(target=run_script, args=("./edw_Scripts/dw_scripts.py",))
    p5 = multiprocessing.Process(target=run_script, args=("./edw_Scripts/dw3_scripts.py",))
    p6 = multiprocessing.Process(target=run_script, args=("batch_control_log_end.py",))
    p7 = multiprocessing.Process(target=update)



    p1.start()
    p1.join()
    p2.start()
    p2.join()
    p3.start()
    p3.join()
    p4.start()
    p4.join()
    p5.start()
    p5.join()
    p6.start()
    p6.join()
    p7.start()
    p7.join()
    print("Full load is completed")
