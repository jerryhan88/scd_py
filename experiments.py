import os.path as opath
import os
from random import randint
#
from __path_organizer import exp_dpath
from genAG import gen_agents, NUM_GROUP
from genTK import gen_tasks
from problems import gen_prmt_AGTK, prmt_pkl2json


def gen_instances():
    problem_dpath = opath.join(exp_dpath, 'problem')
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
    numAgents, numTasks = 10, 10

    for numAgents in range(2, 12, 2):
        for numTasks in range(5, 21, 5):
            prefix = 'g%d-na%03d-nt%03d-sn%02d' % (gNum, numAgents, numTasks, seedNum)
            agents = gen_agents(seedNum, prefix, gNum, numAgents, dpath=csv_dpath)
            tasks = gen_tasks(seedNum, prefix, numTasks, agents, dpath=csv_dpath)
            prmt = gen_prmt_AGTK(agents, tasks, prefix, dpath=pkl_dpath)
            prmt_pkl2json(prmt, dpath=json_dpath)


if __name__ == '__main__':
    gen_instances()
