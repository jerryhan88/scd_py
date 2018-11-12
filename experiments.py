import os.path as opath
import os
import csv
import pandas as pd
from functools import reduce
#
from __path_organizer import exp_dpath
from genAG import gen_agents, NUM_GROUP
from genTK import gen_tasks
from problems import gen_prmt_AGTK, prmt_pkl2json, DEFAULT_VOLUME_CAPACITY, DEFAULT_WEIGHT_CAPACITY


def gen_instances():
    problem_dpath = opath.join(exp_dpath, '_problem')
    csv_dpath = opath.join(problem_dpath, 'csv')
    pkl_dpath = opath.join(problem_dpath, 'pkl')
    json_dpath = opath.join(problem_dpath, 'json')
    for dpath in [problem_dpath, csv_dpath, pkl_dpath, json_dpath]:
        if not opath.exists(dpath):
            os.mkdir(dpath)
    #
    seedNum = 0
    # gNum = randint(NUM_GROUP)

    numAgents, numTasks = 20, 500
    numAgents = 40
    # for seedNum in range(5):
    seedNum = 4
    gNum = seedNum % NUM_GROUP
    # for numTasks in range(400, 1001, 100):
    numTasks = 400
    prefix = 'na%03d-nt%03d-vc%02d-wc%02d-sn%02d' % (numAgents, numTasks,
                                                     DEFAULT_VOLUME_CAPACITY, DEFAULT_WEIGHT_CAPACITY, seedNum)
    agents = gen_agents(seedNum, prefix, gNum, numAgents, dpath=csv_dpath)
    tasks = gen_tasks(seedNum, prefix, numTasks, agents, dpath=csv_dpath)
    prmt = gen_prmt_AGTK(agents, tasks, prefix, dpath=pkl_dpath)
    prmt_pkl2json(prmt, dpath=json_dpath)


def summary():
    summary_fpath = opath.join(exp_dpath, 'summary.csv')
    appNames = ['ILP', 'SDA', 'SDAbnb']
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
