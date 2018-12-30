import os
import getpass
import time
import csv
from math import ceil
from psutil import virtual_memory


JAVA_PROCESS = 'java'
TEMP_LOG = 'temp.log'
USER = getpass.getuser()
MEM_SIZE = virtual_memory().total
KB1 = 1024
MB1 = 1024 * KB1
GB1 = 1024 * MB1
MEM_SIZE = MEM_SIZE / float(GB1)
PER2REAL = 0.01
MIN2SEC = 60


def get_java_processes():
    os.system("ps aux > %s" % TEMP_LOG)
    with open(TEMP_LOG) as f:
        logs = f.read()
    ls = logs.split('\n')
    header = ls[0].split()
    hid = {cn: i for i, cn in enumerate(header)}
    java_processes = []
    for i in range(1, len(ls)):
        try:
            l = ls[i].split()
            if len(l) > len(header):
                process = ' '.join(l[len(header) - 1:])
            else:
                process = l[hid['COMMAND']]
            if l[hid['USER']] == USER and JAVA_PROCESS in process:
                java_processes.append({cn: l[hid[cn]] for cn in header})
        except:
            continue
    assert len(java_processes) <= 1, java_processes
    os.system("rm %s" % TEMP_LOG)
    if java_processes:
        return java_processes[0]
    else:
        return None

def resv_kill_process(termination_limit):
    time.sleep(termination_limit)
    pro = get_java_processes()
    os.system("kill %s" % pro['PID'])

def logging_process(interval, log_fpath):
    with open(log_fpath, 'w') as w_csvfile:
        writer = csv.writer(w_csvfile, lineterminator='\n')
        header = ['time', '%CPU', '%MEM', 'numCore', 'memory']
        writer.writerow(header)
    while True:
        pro = get_java_processes()
        if not pro:
            time.sleep(interval)
            continue
        pCPU, pMEM = map(eval, [pro[cn] for cn in ['%CPU', '%MEM']])
        with open(log_fpath, 'a') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            writer.writerow([time.time(),
                             pCPU, pMEM,
                             ceil(pCPU * PER2REAL), pMEM * MEM_SIZE * PER2REAL])
        time.sleep(interval)


if __name__== '__main__':
    interval, log_fpath = 1, 'temp0.csv'
    logging_process(interval, log_fpath)
    # resv_kill_java_process(1)
    # kill_python_processes()
