import os.path as opath
import multiprocessing
import time
import pickle, csv
from gurobipy import *
#
from _util_logging import write_log, res2file

NUM_CORES = multiprocessing.cpu_count()
LOGGING_INTERVAL = 20


def itr2file(fpath, contents=[]):
    if not contents:
        if opath.exists(fpath):
            os.remove(fpath)
        with open(fpath, 'wt') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            header = ['eliCpuTime', 'eliWallTime',
                      'objbst', 'objbnd', 'gap']
            writer.writerow(header)
    else:
        with open(fpath, 'a') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            writer.writerow(contents)


def run(prmt, etc=None):
    startCpuTime, startWallTime = time.clock(), time.time()
    if 'TimeLimit' not in etc:
        etc['TimeLimit'] = 1e400
    etc['startTS'] = startCpuTime
    etc['startCpuTime'] = startCpuTime
    etc['startWallTime'] = startWallTime
    etc['lastLoggingTime'] = startWallTime
    itr2file(etc['itrFileCSV'])
    #
    def callbackF(m, where):
        if where == GRB.Callback.MIP:
            if time.clock() - etc['startTS'] > etc['TimeLimit']:
                logContents = '\n'
                logContents += 'Interrupted by time limit\n'
                write_log(etc['logFile'], logContents)
                m.terminate()
            if time.time() - etc['lastLoggingTime'] > LOGGING_INTERVAL:
                etc['lastLoggingTime'] = time.time()
                eliCpuTimeP, eliWallTimeP = time.clock() - etc['startCpuTime'], time.time() - etc['startWallTime']
                objbst = m.cbGet(GRB.Callback.MIP_OBJBST)
                objbnd = m.cbGet(GRB.Callback.MIP_OBJBND)
                gap = abs(objbst - objbnd) / (0.000001 + abs(objbst))
                itr2file(etc['itrFileCSV'], ['%.2f' % eliCpuTimeP, '%.2f' % eliWallTimeP,
                                             '%.2f' % objbst, '%.2f' % objbnd, '%.2f' % gap])
    #
    H, K, N, A = map(prmt.get, ['H', 'K', 'N', 'A'])
    h_k, n_k, v_k, w_k, r_k = map(prmt.get, ['h_k', 'n_k', 'v_k', 'w_k', 'r_k'])
    v_a, w_a, E_a = map(prmt.get, ['v_a', 'w_a', 'E_a'])
    p_ae, l_ae, u_ae = map(prmt.get, ['p_ae', 'l_ae', 'u_ae'])
    S_ae, N_ae, c_aeij = map(prmt.get, ['S_ae', 'N_ae', 'c_aeij'])
    t_ij, al_i, be_i, ga_i, F_ae = map(prmt.get, ['t_ij', 'al_i', 'be_i', 'ga_i', 'F_ae'])
    M = len(N) * max(t_ij.values())
    #
    ILP = Model('ILP')
    y_ak = {(a, k): ILP.addVar(vtype=GRB.BINARY, name='y[%d,%d]' % (a, k))
            for a in A for k in K}
    z_aek = {(a, e, k): ILP.addVar(vtype=GRB.BINARY, name='z[%d,%d,%d]' % (a, e, k))
            for a in A for e in E_a[a] for k in K}
    x_aeij, mu_aei = {}, {}
    for a in A:
        for e in E_a[a]:
            aeN = N_ae[a, e]
            for i in aeN:
                for j in aeN:
                    x_aeij[a, e, i, j] = ILP.addVar(vtype=GRB.BINARY, name='x[%d,%d,%s,%s]' % (a, e, i, j))
                mu_aei[a, e, i] = ILP.addVar(vtype=GRB.CONTINUOUS, name='a[%d,%d,%s]' % (a, e, i))
    ILP.update()
    #
    obj = LinExpr()
    for k in K:
        for a in A:
            obj += r_k[k] * y_ak[a, k]
            for e in E_a[a]:
                obj -= r_k[k] * p_ae[a, e] * z_aek[a, e, k]
    ILP.setObjective(obj, GRB.MAXIMIZE)
    #
    for k in K:
        ILP.addConstr(quicksum(y_ak[a, k] for a in A) <= 1,
                     name='TA[%d]' % k)
    for a in A:
        ILP.addConstr(quicksum(v_k[k] * y_ak[a, k] for k in K) <= v_a[a],
                      name='V[%d]' % k)
        ILP.addConstr(quicksum(w_k[k] * y_ak[a, k] for k in K) <= w_a[a],
                      name='W[%d]' % k)
        for e in E_a[a]:
            for k in K:
                ILP.addConstr(z_aek[a, e, k] <= y_ak[a, k],
                     name='TC[%d,%d,%d]' % (a, e, k))
    #
    for a in A:
        for e in E_a[a]:
            aeS, aeN, aeF = S_ae[a, e], N_ae[a, e], F_ae[a, e]
            o_ae, d_ae = 's0_%d_%d' % (a, e), 's%d_%d_%d' % (len(aeS) - 1, a, e)
            # Initiate flow
            ILP.addConstr(quicksum(x_aeij[a, e, o_ae, j] for j in aeN) == 1,
                        name='iFO[%d,%d]' % (a, e))
            ILP.addConstr(quicksum(x_aeij[a, e, j, d_ae] for j in aeN) == 1,
                        name='iFD[%d,%d]' % (a, e))
            for i in aeS:
                if i == o_ae or i == d_ae:
                    continue
                ILP.addConstr(quicksum(x_aeij[a, e, i, j] for j in aeN if j != i) == 1,
                             name='iFS1[%d,%d,%s]' % (a, e, i))
                ILP.addConstr(quicksum(x_aeij[a, e, j, i] for j in aeN if j != i) == 1,
                             name='iFS2[%d,%d,%s]' % (a, e, i))
            # No flow
            ILP.addConstr(quicksum(x_aeij[a, e, j, o_ae] for j in aeN) == 0,
                         name='xFO[%d,%d]' % (a, e))
            ILP.addConstr(quicksum(x_aeij[a, e, d_ae, j] for j in aeN) == 0,
                         name='xFD[%d,%d]' % (a, e))
            for k in set(K).difference(set(aeF)):
                ILP.addConstr(quicksum(x_aeij[a, e, n_k[k], j] for j in aeN) == 0,
                              name='xFN[%d,%d,%d]' % (a, e, k))
            # Flow about delivery nodes; only when the warehouse visited
            for k in K:
                ILP.addConstr(quicksum(x_aeij[a, e, n_k[k], j] for j in aeN) <=
                              quicksum(x_aeij[a, e, j, h_k[k]] for j in aeN),
                            name='tFC[%d,%d,%d]' % (a, e, k))
            # Flow conservation
            for i in N:
                ILP.addConstr(quicksum(x_aeij[a, e, i, j] for j in aeN) ==
                             quicksum(x_aeij[a, e, j, i] for j in aeN),
                            name='FC[%d,%d,%s]' % (a, e, i))
    for a in A:
        for e in E_a[a]:
            aeS, aeN = S_ae[a, e], N_ae[a, e]
            # Time Window
            for i in aeN:
                ILP.addConstr(al_i[i] <= mu_aei[a, e, i],
                              name='TW_L[%d,%d,%s]' % (a, e, i))
                ILP.addConstr(mu_aei[a, e, i] <= be_i[i],
                              name='TW_U[%d,%d,%s]' % (a, e, i))
            # Warehouse and Delivery Sequence
            for k in K:
                ILP.addConstr(mu_aei[a, e, h_k[k]] <= mu_aei[a, e, n_k[k]],
                              name='WD_S[%d,%d,%d]' % (a, e, k))
            # Routine route preservation
            for i in aeS:
                for j in aeS:
                    ILP.addConstr(c_aeij[a, e, i, j] * mu_aei[a, e, i] <= mu_aei[a, e, j],
                                  name='RR_P[%d,%d,%s,%s]' % (a, e, i, j))
            # Arrival time calculation
            for i in aeN:
                for j in aeN:
                    ILP.addConstr(mu_aei[a, e, i] + ga_i[i] + t_ij[i, j] <=
                                  mu_aei[a, e, j] + M * (1 - x_aeij[a, e, i, j]),
                                  name='AT[%d,%d,%s,%s]' % (a, e, i, j))
            # Detour Limit
            ILP.addConstr(quicksum(t_ij[i, j] * x_aeij[a, e, i, j]
                              for i in aeN for j in aeN) - l_ae[a, e] <= u_ae[a, e],
                         name='DL[%d,%d]' % (a, e))
            # Complicated and Combined constraints
            for k in K:
                ILP.addConstr(y_ak[a, k] - quicksum(x_aeij[a, e, j, n_k[k]] for j in aeN) <=
                             z_aek[a, e, k],
                             name='CC[%d,%d,%d]' % (a, e, k))
    #
    ILP.setParam('LazyConstraints', True)
    ILP.setParam('Threads', NUM_CORES)
    if etc['logFile']:
        ILP.setParam('LogFile', etc['logFile'])
    ILP.optimize(callbackF)
    #
    if ILP.status == GRB.Status.INFEASIBLE:
        ILP.write('%s.lp' % prmt['problemName'])
        ILP.computeIIS()
        ILP.write('%s.ilp' % prmt['problemName'])
    #
    if etc and ILP.status != GRB.Status.INFEASIBLE:
        for k in ['solFileCSV', 'solFilePKL', 'solFileTXT']:
            assert k in etc
        #
        endCpuTime, endWallTime = time.clock(), time.time()
        eliCpuTime, eliWallTime = endCpuTime - startCpuTime, endWallTime - startWallTime
        res2file(etc['solFileCSV'], ILP.objVal, ILP.MIPGap, eliCpuTime, eliWallTime)
        #

        _y_ak = {(a, k): y_ak[a, k].x for a in A for k in K}
        _z_aek = {(a, e, k): z_aek[a, e, k].x for a in A for e in E_a[a] for k in K}
        _x_aeij, _mu_aei = {}, {}
        for a in A:
            for e in E_a[a]:
                aeN = N_ae[a, e]
                for i in aeN:
                    for j in aeN:
                        _x_aeij[a, e, i, j] = x_aeij[a, e, i, j].x
                    _mu_aei[a, e, i] = mu_aei[a, e, i].x
        sol = {
            'y_ak': _y_ak, 'z_aek': _z_aek,
            'x_aeij': _x_aeij, 'mu_aei': _mu_aei,
        }
        with open(etc['solFilePKL'], 'wb') as fp:
            pickle.dump(sol, fp)
        #
        with open(etc['solFileTXT'], 'w') as f:
            endCpuTime, endWallTime = time.clock(), time.time()
            eliCpuTime, eliWallTime = endCpuTime - startCpuTime, endWallTime - startWallTime
            logContents = 'Summary\n'
            logContents += '\t Cpu Time: %f\n' % eliCpuTime
            logContents += '\t Wall Time: %f\n' % eliWallTime
            logContents += '\t ObjV: %.3f\n' % ILP.objVal
            logContents += '\t Gap: %.3f\n' % ILP.MIPGap
            logContents += '\n'
            logContents += 'Details\n'
            for a in A:
                assignedTasks, meaninglessNodes = [], set(N)
                for k in K:
                    if y_ak[a, k].x > 0.5:
                        assignedTasks.append(k)
                        for i in [h_k[k], n_k[k]]:
                            if i in meaninglessNodes:
                                meaninglessNodes.remove(i)
                logContents += 'A%d: %s\n' % (a, str(assignedTasks))
                for e in E_a[a]:
                    aeS, aeN = S_ae[a, e], N_ae[a, e]
                    o_ae, d_ae = 's0_%d_%d' % (a, e), 's%d_%d_%d' % (len(aeS) - 1, a, e)
                    _route = {}
                    for j in aeN:
                        for i in aeN:
                            if x_aeij[a, e, i, j].x > 0.5:
                                _route[i] = j
                    i = o_ae
                    route = []
                    accomplishedTasks = []
                    while i != d_ae:
                        if i not in meaninglessNodes:
                            route.append('%s(%.2f)' % (i, mu_aei[a, e, i].x))
                            if i.startswith('n'):
                                accomplishedTasks.append(int(i[len('n'):]))
                        i = _route[i]
                    route.append('%s(%.2f)' % (i, mu_aei[a, e, i].x))
                    logContents += '\t R%d%s: %s\n' % (e, str(accomplishedTasks), '-'.join(route))
            f.write(logContents)


if __name__ == '__main__':
    from problems import euclideanDistEx0
    prmt = euclideanDistEx0()


    # import pickle
    #
    # with open(opath.join('_temp', 'prmt_g0-na005-nt003-sn00.pkl'), 'rb') as fp:
    #     prmt = pickle.load(fp)


    problemName = prmt['problemName']
    approach = 'ILP'
    etc = {'solFilePKL': opath.join('_temp', 'sol_%s_%s.pkl' % (problemName, approach)),
           'solFileCSV': opath.join('_temp', 'sol_%s_%s.csv' % (problemName, approach)),
           'solFileTXT': opath.join('_temp', 'sol_%s_%s.txt' % (problemName, approach)),
           'logFile': opath.join('_temp', '%s_%s.log' % (problemName, approach)),
           'itrFileCSV': opath.join('_temp', '%s_itr%s.csv' % (problemName, approach)),
           }

    run(prmt, etc)
