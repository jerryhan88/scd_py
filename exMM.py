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
    T, w_i = map(prmt.get, ['T', 'w_i'])
    N, alpha_i, beta_i, c_i = map(prmt.get, ['N', 'alpha_i', 'beta_i', 'c_i'])
    K, R_k = map(prmt.get, ['K', 'R_k'])
    C_kr, gamma_kr, u_kr, = map(prmt.get, ['C_kr', 'gamma_kr', 'u_kr'])
    t_ij, p_krij = map(prmt.get, ['t_ij', 'p_krij'])
    M = len(N) * max(t_ij.values())
    #
    EX = Model('EX')
    y_ki = {(k, i): EX.addVar(vtype=GRB.BINARY, name='y[%d,%d]' % (k, i))
            for k in K for i in T}
    z_kri = {(k, r, i): EX.addVar(vtype=GRB.BINARY, name='z[%d,%d,%d]' % (k, r, i))
            for k in K for r in R_k[k] for i in T}
    x_krij, a_kri = {}, {}
    for k in K:
        for r in R_k[k]:
            krN = N.union(set(C_kr[k, r]))
            for i in krN:
                for j in krN:
                    x_krij[k, r, i, j] = EX.addVar(vtype=GRB.BINARY, name='x[%d,%d,%s,%s]' % (k, r, i, j))
                a_kri[k, r, i] = EX.addVar(vtype=GRB.CONTINUOUS, name='a[%d,%d,%s]' % (k, r, i))
    EX.update()
    #
    obj = LinExpr()
    for i in T:
        for k in K:
            obj += w_i[i] * y_ki[k, i]
            for r in R_k[k]:
                obj -= w_i[i] * gamma_kr[k, r] * z_kri[k, r, i]
    EX.setObjective(obj, GRB.MAXIMIZE)
    #
    for i in T:
        EX.addConstr(quicksum(y_ki[k, i] for k in K) <= 1,
                     name='TA[%d]' % i)
    for k in K:
        for r in R_k[k]:
            krN = N.union(set(C_kr[k, r]))
            krP, krM = 'o_%d_%d' % (k, r), 'd_%d_%d' % (k, r)
            # Initiate flow
            EX.addConstr(quicksum(x_krij[k, r, krP, j] for j in krN) == 1,
                        name='iFO[%d,%d]' % (k, r))
            EX.addConstr(quicksum(x_krij[k, r, j, krM] for j in krN) == 1,
                        name='iFD[%d,%d]' % (k, r))
            for i in C_kr[k, r]:
                if i == krP or i == krM:
                    continue
                EX.addConstr(quicksum(x_krij[k, r, i, j] for j in krN if j != i) == 1,
                             name='iFS1[%d,%d,%s]' % (k, r, i))
                EX.addConstr(quicksum(x_krij[k, r, j, i] for j in krN if j != i) == 1,
                             name='iFS2[%d,%d,%s]' % (k, r, i))
            # No flow
            EX.addConstr(quicksum(x_krij[k, r, j, krP] for j in krN) == 0,
                         name='xFO[%d,%d]' % (k, r))
            EX.addConstr(quicksum(x_krij[k, r, krM, j] for j in krN) == 0,
                         name='xFD[%d,%d]' % (k, r))
            for i in T:
                # Flow conservation related to a task
                EX.addConstr(quicksum(x_krij[k, r, 'p%d' % i, j] for j in krN) ==
                             quicksum(x_krij[k, r, j, 'd%d' % i] for j in krN),
                            name='tFC[%d,%d,%d]' % (k, r, i))
            for i in N:
                # Flow conservation
                EX.addConstr(quicksum(x_krij[k, r, i, j] for j in krN) ==
                             quicksum(x_krij[k, r, j, i] for j in krN),
                            name='FC[%d,%d,%s]' % (k, r, i))
    for k in K:
        for r in R_k[k]:
            krN = N.union(set(C_kr[k, r]))
            krP, krM = 'o_%d_%d' % (k, r), 'd_%d_%d' % (k, r)
            # Initiate arrival time
            EX.addConstr(a_kri[k, r, krP] == 0,
                         name='iA1[%d,%d]' % (k, r))
            EX.addConstr(a_kri[k, r, krM] <= u_kr[k, r],
                         name='iA2[%d,%d]' % (k, r))
            for i in N:
                # Time Window
                EX.addConstr(alpha_i[i] <= a_kri[k, r, i],
                             name='TW_L[%d,%d,%s]' % (k, r, i))
                EX.addConstr(a_kri[k, r, i] <= beta_i[i],
                             name='TW_U[%d,%d,%s]' % (k, r, i))
            for i in T:
                iP, iM = 'p%d' % i, 'd%d' % i
                # Pickup and Delivery Sequence
                EX.addConstr(a_kri[k, r, iP] <= a_kri[k, r, iM],
                             name='PD_S[%d,%d,%d]' % (k, r, i))
            for i in C_kr[k, r]:
                for j in C_kr[k, r]:
                    # Routine route preservation
                    EX.addConstr(p_krij[k, r, i, j] * a_kri[k, r, i] <= a_kri[k, r, j],
                                 name='RR_P[%d,%d,%s,%s]' % (k, r, i, j))
            for i in krN:
                for j in krN:
                    # Arrival time calculation
                    EX.addConstr(a_kri[k, r, i] + c_i[i] + t_ij[i, j] <=
                                 a_kri[k, r, j] + M * (1 - x_krij[k, r, i, j]),
                                 name='AT[%d,%d,%s,%s]' % (k, r, i, j))
            for i in T:
                # Task assignment and accomplishment
                EX.addConstr(y_ki[k, i] - quicksum(x_krij[k, r, 'p%d' % i, j] for j in krN) <=
                             z_kri[k, r, i],
                             name='tAA[%d,%d,%d]' % (k, r, i))
    #
    EX.setParam('LazyConstraints', True)
    EX.setParam('Threads', NUM_CORES)
    if etc['logFile']:
        EX.setParam('LogFile', etc['logFile'])
    EX.optimize(callbackF)
    #
    if EX.status == GRB.Status.INFEASIBLE:
        EX.write('%s.lp' % prmt['problemName'])
        EX.computeIIS()
        EX.write('%s.ilp' % prmt['problemName'])
    #
    if etc and EX.status != GRB.Status.INFEASIBLE:
        for k in ['solFileCSV', 'solFilePKL', 'solFileTXT']:
            assert k in etc
        #
        endCpuTime, endWallTime = time.clock(), time.time()
        eliCpuTime, eliWallTime = endCpuTime - startCpuTime, endWallTime - startWallTime
        res2file(etc['solFileCSV'], EX.objVal, EX.MIPGap, eliCpuTime, eliWallTime)
        #

        _y_ki = {(k, i): y_ki[k, i].x for k in K for i in T}
        _z_kri = {(k, r, i): z_kri[k, r, i].x for k in K for r in R_k[k] for i in T}
        _x_krij, _a_kri = {}, {}
        for k in K:
            for r in R_k[k]:
                krN = N.union(set(C_kr[k, r]))
                for i in krN:
                    for j in krN:
                        _x_krij[k, r, i, j] = x_krij[k, r, i, j].x
                    _a_kri[k, r, i] = a_kri[k, r, i].x
        sol = {
            'y_ki': _y_ki, 'z_kri': _z_kri,
            'x_krij': _x_krij, 'a_kri': _a_kri,
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
            logContents += '\t ObjV: %.3f\n' % EX.objVal
            logContents += '\t Gap: %.3f\n' % EX.MIPGap
            f.write(logContents)
            f.write('\n')
            logContents = 'Details\n'
            logContents += 'Assignment\n'
            for k in K:
                assignedTasks = [i for i in T if y_ki[k, i].x > 0.5]
                logContents += '\t A%d: %s\n' % (k, str(assignedTasks))
                for tid in assignedTasks:
                    for r in R_k[k]:
                        z = 1 if z_kri[k, r, tid].x > 0.5 else 0
                        logContents += '\t\t T%d-R%d: %d(%s)\n' % (tid, r, z, 'Accomplished' if z == 0 else 'Fail')
                        #
                        if z == 0:
                            krN = N.union(set(C_kr[k, r]))
                            krP, krM = 'o_%d_%d' % (k, r), 'd_%d_%d' % (k, r)
                            _route = {}
                            for j in krN:
                                for i in krN:
                                    if x_krij[k, r, i, j].x > 0.5:
                                        _route[i] = j
                            i = krP
                            route = []
                            while i != krM:
                                route.append('%s(%.2f)' % (i, a_kri[k, r, i].x))
                                i = _route[i]
                            route.append('%s(%.2f)' % (i, a_kri[k, r, i].x))

                            logContents += '\t\t  %s\n' % str(route)
            f.write(logContents)

if __name__ == '__main__':
    from problems import ex0, euclideanDistEx0
    # prmt = ex0()
    prmt = euclideanDistEx0()
    problemName = prmt['problemName']
    etc = {'solFilePKL': opath.join('_temp', 'sol_%s_EX.pkl' % problemName),
           'solFileCSV': opath.join('_temp', 'sol_%s_EX.csv' % problemName),
           'solFileTXT': opath.join('_temp', 'sol_%s_EX.txt' % problemName),
           'logFile': opath.join('_temp', '%s_EX.log' % problemName),
           'itrFileCSV': opath.join('_temp', '%s_itrEX.csv' % problemName),
           }

    run(prmt, etc)
