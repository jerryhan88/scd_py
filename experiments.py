import os.path as opath
import multiprocessing
import os
import csv
import pandas as pd
from functools import reduce
#
from __path_organizer import exp_dpath
from genAG import gen_agents, NUM_GROUP
from genTK import gen_tasks
from problems import gen_prmt_AGTK, prob_pkl2json, DEFAULT_VOLUME_CAPACITY, DEFAULT_WEIGHT_CAPACITY
from genPI import gen_problemInstance


problem_dpath = opath.join(exp_dpath, 'problem_temp')
csv_dpath = opath.join(problem_dpath, 'csv')
json_dpath = opath.join(problem_dpath, 'json')
for dpath in [problem_dpath, csv_dpath, json_dpath]:
    if not opath.exists(dpath):
        os.mkdir(dpath)


def gen_instances():
    numProcessors = multiprocessing.cpu_count() - 2
    # numProcessors = 1
    worker_arguments = [[] for _ in range(numProcessors)]

    for seedNum in range(20):
        worker_arguments[seedNum % numProcessors].append(seedNum)
    ps = []
    for wid, seedNums in enumerate(worker_arguments):
        p = multiprocessing.Process(target=get_a_instance_givenSeedNums,
                                    args=(wid, seedNums))
        ps.append(p)
        p.start()
    for p in ps:
        p.join()


def get_a_instance_givenSeedNums(wid, seedNums):
    capacity = 5
    numAgents = 10
    for seedNum in seedNums:
        gNum = seedNum % NUM_GROUP
        prefix = 'na%03d-sn%02d' % (numAgents, seedNum)
        agents = gen_agents(seedNum, prefix, gNum, numAgents, dpath=csv_dpath)
        for numTasks in range(20, 101, 20):
            prefix = 'na%03d-nt%03d-ca%03d-sn%02d' % (numAgents, numTasks, capacity, seedNum)
            tasks = gen_tasks(seedNum, prefix, numTasks, agents, dpath=csv_dpath)
            prob = gen_problemInstance(prefix, agents, tasks,
                                       tb_ratio=1.05, volume_capacity=capacity, weight_capacity=capacity)
            prob_pkl2json(prob, dpath=json_dpath)


def gen_a_instance(wid, arguments):
    for numAgents, numTasks, seedNum, gNum, csv_dpath, pkl_dpath, json_dpath in arguments:
        prefix = 'na%03d-nt%03d-ca%03d-sn%02d' % (numAgents, numTasks, DEFAULT_VOLUME_CAPACITY, seedNum)
        agents = gen_agents(seedNum, prefix, gNum, numAgents, dpath=csv_dpath)
        tasks = gen_tasks(seedNum, prefix, numTasks, agents, dpath=csv_dpath)
        prob = gen_problemInstance(prefix, agents, tasks, tb_ratio=1.05)
        prob_pkl2json(prob, dpath=json_dpath)


def summary():
    summary_fpath = opath.join(exp_dpath, 'summary.csv')
    appNames = ['ILP', 'SDA', 'SDAbnb', 'SDAgh']
    with open(summary_fpath, 'w') as w_csvfile:
        writer = csv.writer(w_csvfile, lineterminator='\n')
        header = ['pf', '#A', '#T', 'vc', 'wc', 'sn']
        header += ['%s_objV' % appName for appName in appNames]
        header += ['%s_cpuT' % appName for appName in appNames]
        writer.writerow(header)
        
    json_dapth = reduce(opath.join, [exp_dpath, 'problem', 'json'])
    prefixs = []
    for fn in os.listdir(json_dapth):
        if not fn.endswith('.json'):
            continue
        _, prefix = fn[:-len('.json')].split('_')
        prefixs.append(prefix)
    #
    sol_dpath = opath.join(exp_dpath, 'solution')
    for prefix in prefixs:
        na, nt, vc, wc, sn = [int(s[len('xx'):]) for s in prefix.split('-')]
        new_row = [prefix, na, nt, vc, wc, sn]
        objVs, cpuTs = [], []
        for appName in appNames:
            sol_fpath = opath.join(sol_dpath, 'sol_%s_%s.csv' % (prefix, appName))
            if opath.exists(sol_fpath):
                with open(sol_fpath) as r_csvfile:
                    reader = csv.DictReader(r_csvfile)
                    for row in reader:
                        _objV, _eliCpuTime = [row[cn] for cn in ['objV', 'eliCpuTime']]
                    objVs.append(_objV)
                    cpuTs.append(_eliCpuTime)
            else:
                if appName == 'ILP':
                    log_fpath = opath.join(sol_dpath, 'log_%s_%s.log' % (prefix, appName))
                else:
                    log_fpath = opath.join(sol_dpath, 'log_%s_%s.csv' % (prefix, appName))
                if opath.exists(log_fpath):
                    objVs.append('-')
                    cpuTs.append('-')
                else:
                    objVs.append(None)
                    cpuTs.append(None)
        new_row += objVs
        new_row += cpuTs
        with open(summary_fpath, 'a') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            writer.writerow(new_row)
    #
    df = pd.read_csv(summary_fpath)
    df = df.sort_values(by=['#A', '#T', 'vc', 'wc', 'sn'])
    df.to_csv(summary_fpath, index=False)


if __name__ == '__main__':
    gen_instances()
    # summary()
