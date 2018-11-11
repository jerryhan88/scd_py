import os.path as opath
import os
import csv
from random import randint
#
from __path_organizer import exp_dpath
from genAG import gen_agents, NUM_GROUP
from genTK import gen_tasks
from problems import gen_prmt_AGTK, prmt_pkl2json


def gen_instances():
    problem_dpath = opath.join(exp_dpath, 'problem10')
    csv_dpath = opath.join(problem_dpath, 'csv')
    pkl_dpath = opath.join(problem_dpath, 'pkl')
    json_dpath = opath.join(problem_dpath, 'json')
    for dpath in [problem_dpath, csv_dpath, pkl_dpath, json_dpath]:
        if not opath.exists(dpath):
            os.mkdir(dpath)
    #
    seedNum = 0
    # gNum = randint(NUM_GROUP)
    gNum = 0
    numAgents, numTasks = 20, 500

    # for numAgents in range(2, 12, 2):

    # numAgents = 20
    for numTasks in range(100, 181, 20):
        prefix = 'g%d-na%03d-nt%03d-sn%02d' % (gNum, numAgents, numTasks, seedNum)
        agents = gen_agents(seedNum, prefix, gNum, numAgents, dpath=csv_dpath)
        tasks = gen_tasks(seedNum, prefix, numTasks, agents, dpath=csv_dpath)
        prmt = gen_prmt_AGTK(agents, tasks, prefix, dpath=pkl_dpath)
        prmt_pkl2json(prmt, dpath=json_dpath)


def summary():
    summary_fpath = opath.join(exp_dpath, 'summary.csv')
    with open(summary_fpath, 'w') as w_csvfile:
        writer = csv.writer(w_csvfile, lineterminator='\n')
        writer.writerow(['#A', '#T', 'AppName', 'objV', 'Gap', 'eliWallTime'])
        
    sol_dpath = opath.join(exp_dpath, 'solution')
    
    log_fns = [fn for fn in os.listdir(sol_dpath) if fn.startswith('log')]
    for fn in log_fns:
        _, prefix, appName = fn[:-len('.csv')].split('_')
        _, _na, _nt, _ = prefix.split('-')
        na, nt = [int(s[len('xx'):]) for s in [_na, _nt]]
        sol_fpath = opath.join(sol_dpath, 'sol_%s_%s.csv' % (prefix, appName))
        new_row = [na, nt, appName]
        if opath.exists(sol_fpath):
            with open(sol_fpath) as r_csvfile:
                reader = csv.DictReader(r_csvfile)
                for row in reader:
                    new_row += [row[cn] for cn in ['objV', 'Gap', 'eliWallTime']]
                    break
            with open(summary_fpath, 'a') as w_csvfile:
                writer = csv.writer(w_csvfile, lineterminator='\n')
                writer.writerow(new_row)


if __name__ == '__main__':
    gen_instances()
    # summary()
