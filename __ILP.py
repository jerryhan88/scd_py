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


def insert_task(prmt, seq0, tid):
    h_i = prmt['h_i']
    t_ij = prmt['t_ij']
    a_i, b_i, c_i, t_ij = map(prmt.get, ['a_i', 'b_i', 'c_i', 't_ij'])
    #
    def check_TW_violation(seq):
        n0 = seq[0]
        erest_deptTime = a_i[n0] + c_i[n0]
        for n1 in seq[1:]:
            erest_arrvTime = erest_deptTime + t_ij[n0, n1]
            if b_i[n1] < erest_arrvTime:
                return True
            else:
                erest_deptTime = max(erest_arrvTime, a_i[n1]) + c_i[n1]
            n0 = n1
        return False
    #
    wn, dn = h_i[tid], 'n%d' % tid
    is_wh_visited = False
    for s0, n in enumerate(seq0):
        if wn == n:
            is_wh_visited = True
            break
    min_tt, best_seq = 1e400, None
    if is_wh_visited:
        # Insert the delivery node only
        for s1 in range(s0, len(seq0) - 1):
            seq1 = seq0[:]
            seq1.insert(s1, dn)
            if check_TW_violation(seq1):
                continue
            tt = sum(t_ij[seq1[i], seq1[i + 1]] for i in range(len(seq1) - 1))
            if tt < min_tt:
                min_tt, best_seq = tt, seq1
    else:
        # Insert both the warehouse and delivery nodes
        for s0 in range(1, len(seq0) - 1):
            for s1 in range(s0, len(seq0) - 1):
                seq1 = seq0[:]
                seq1.insert(s0, wn)
                seq1.insert(s1 + 1, dn)
                if check_TW_violation(seq1):
                    continue
                tt = sum(t_ij[seq1[i], seq1[i + 1]] for i in range(len(seq1) - 1))
                if tt < min_tt:
                    min_tt, best_seq = tt, seq1
    #
    return min_tt, best_seq


def get_feasibleTasks(prmt):
    T, K = map(prmt.get, ['T', 'K'])
    R_k, l_kr, u_kr, C_kr = map(prmt.get, ['R_k', 'l_kr', 'u_kr', 'C_kr'])
    #
    F_kr = {}
    for k in K:
        for r in R_k[k]:
            krC = C_kr[k, r]
            F_kr[k, r] = []
            for i in T:
                min_tt, best_seq = insert_task(prmt, krC, i)
                if min_tt - l_kr[k, r] < u_kr[k, r]:
                    F_kr[k, r].append(i)
    #
    return F_kr


def get_N_kr(prmt, F_kr):
    T, K = map(prmt.get, ['T', 'K'])
    h_i = prmt['h_i']
    R_k, C_kr = map(prmt.get, ['R_k', 'C_kr'])
    #
    N_kr = {}
    for k in K:
        for r in R_k[k]:
            krC, krF = C_kr[k, r], F_kr[k, r]
            krN = set(krC)
            for i in krF:
                wn, dn = h_i[i], 'n%d' % i
                krN.add(wn)
                krN.add(dn)
            N_kr[k, r] = list(krN)
    #
    return N_kr


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
    H, T, N, K = map(prmt.get, ['H', 'T', 'N', 'K'])
    h_i, n_i, w_i = map(prmt.get, ['h_i', 'n_i', 'w_i'])
    v_k, R_k = map(prmt.get, ['v_k', 'R_k'])
    r_kr, l_kr, u_kr = map(prmt.get, ['r_kr', 'l_kr', 'u_kr'])
    C_kr, p_krij, _ = map(prmt.get, ['C_kr', 'p_krij', 'N_kr'])
    a_i, b_i, c_i, t_ij = map(prmt.get, ['a_i', 'b_i', 'c_i', 't_ij'])
    #
    F_kr = get_feasibleTasks(prmt)
    nN_kr = get_N_kr(prmt, F_kr)
    M = len(N) * max(t_ij.values())
    #
    ILP = Model('ILP')
    y_ki = {(k, i): ILP.addVar(vtype=GRB.BINARY, name='y[%d,%d]' % (k, i))
            for k in K for i in T}
    z_kri = {(k, r, i): ILP.addVar(vtype=GRB.BINARY, name='z[%d,%d,%d]' % (k, r, i))
            for k in K for r in R_k[k] for i in T}
    x_krij, a_kri = {}, {}
    for k in K:
        for r in R_k[k]:
            krN = N_kr[k, r]
            for i in krN:
                for j in krN:
                    x_krij[k, r, i, j] = ILP.addVar(vtype=GRB.BINARY, name='x[%d,%d,%s,%s]' % (k, r, i, j))
                a_kri[k, r, i] = ILP.addVar(vtype=GRB.CONTINUOUS, name='a[%d,%d,%s]' % (k, r, i))
    ILP.update()
    #
    obj = LinExpr()
    for i in T:
        for k in K:
            obj += w_i[i] * y_ki[k, i]
            for r in R_k[k]:
                obj -= w_i[i] * gamma_kr[k, r] * z_kri[k, r, i]
    ILP.setObjective(obj, GRB.MAXIMIZE)
    #
    for i in T:
        ILP.addConstr(quicksum(y_ki[k, i] for k in K) <= 1,
                     name='TA[%d]' % i)
    for i in T:
        for k in K:
            for r in R_k[k]:
                ILP.addConstr(z_kri[k, r, i] <= y_ki[k, i],
                     name='TC[%d,%d,%d]' % (i, k, r))
    #
    for k in K:
        for r in R_k[k]:
            krN = N_kr[k, r]
            krP, krM = 'o_%d_%d' % (k, r), 'd_%d_%d' % (k, r)
            # Initiate flow
            ILP.addConstr(quicksum(x_krij[k, r, krP, j] for j in krN) == 1,
                        name='iFO[%d,%d]' % (k, r))
            ILP.addConstr(quicksum(x_krij[k, r, j, krM] for j in krN) == 1,
                        name='iFD[%d,%d]' % (k, r))
            for i in C_kr[k, r]:
                if i == krP or i == krM:
                    continue
                ILP.addConstr(quicksum(x_krij[k, r, i, j] for j in krN if j != i) == 1,
                             name='iFS1[%d,%d,%s]' % (k, r, i))
                ILP.addConstr(quicksum(x_krij[k, r, j, i] for j in krN if j != i) == 1,
                             name='iFS2[%d,%d,%s]' % (k, r, i))
            # No flow
            ILP.addConstr(quicksum(x_krij[k, r, j, krP] for j in krN) == 0,
                         name='xFO[%d,%d]' % (k, r))
            ILP.addConstr(quicksum(x_krij[k, r, krM, j] for j in krN) == 0,
                         name='xFD[%d,%d]' % (k, r))
            for i in T:
                # Flow conservation related to a task
                ILP.addConstr(quicksum(x_krij[k, r, 'p%d' % i, j] for j in krN) ==
                             quicksum(x_krij[k, r, j, 'd%d' % i] for j in krN),
                            name='tFC[%d,%d,%d]' % (k, r, i))
            for i in N:
                # Flow conservation
                ILP.addConstr(quicksum(x_krij[k, r, i, j] for j in krN) ==
                             quicksum(x_krij[k, r, j, i] for j in krN),
                            name='FC[%d,%d,%s]' % (k, r, i))
    for k in K:
        for r in R_k[k]:
            krN = N_kr[k, r]
            krP, krM = 'o_%d_%d' % (k, r), 'd_%d_%d' % (k, r)
            # Initiate arrival time
            ILP.addConstr(a_kri[k, r, krP] == 0,
                         name='iAT[%d,%d]' % (k, r))
            # Arrival time calculation
            for i in krN:
                for j in krN:
                    ILP.addConstr(a_kri[k, r, i] + c_i[i] + t_ij[i, j] <=
                                 a_kri[k, r, j] + M * (1 - x_krij[k, r, i, j]),
                                 name='AT[%d,%d,%s,%s]' % (k, r, i, j))
            # Time Window
            for i in N:
                ILP.addConstr(alpha_i[i] <= a_kri[k, r, i],
                             name='TW_L[%d,%d,%s]' % (k, r, i))
                ILP.addConstr(a_kri[k, r, i] <= beta_i[i],
                             name='TW_U[%d,%d,%s]' % (k, r, i))
            # Pickup and Delivery Sequence
            for i in T:
                iP, iM = 'p%d' % i, 'd%d' % i
                ILP.addConstr(a_kri[k, r, iP] <= a_kri[k, r, iM],
                             name='PD_S[%d,%d,%d]' % (k, r, i))
            # Routine route preservation
            for i in C_kr[k, r]:
                for j in C_kr[k, r]:
                    ILP.addConstr(p_krij[k, r, i, j] * a_kri[k, r, i] <= a_kri[k, r, j],
                                 name='RR_P[%d,%d,%s,%s]' % (k, r, i, j))
            # Detour Limit
            ILP.addConstr(quicksum(t_ij[i, j] * x_krij[k, r, i, j]
                              for i in krN for j in krN) - l_kr[k, r] <= u_kr[k, r],
                         name='DL[%d,%d]' % (k, r))
            # Task assignment and accomplishment
            for i in T:
                ILP.addConstr(y_ki[k, i] - quicksum(x_krij[k, r, 'p%d' % i, j] for j in krN) <=
                             z_kri[k, r, i],
                             name='tAA[%d,%d,%d]' % (k, r, i))
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

        _y_ki = {(k, i): y_ki[k, i].x for k in K for i in T}
        _z_kri = {(k, r, i): z_kri[k, r, i].x for k in K for r in R_k[k] for i in T}
        _x_krij, _a_kri = {}, {}
        for k in K:
            for r in R_k[k]:
                krN = N_kr[k, r]
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
            logContents += '\t ObjV: %.3f\n' % ILP.objVal
            logContents += '\t Gap: %.3f\n' % ILP.MIPGap
            logContents += '\n'
            logContents += 'Details\n'
            for k in K:
                assignedTasks = [i for i in T if y_ki[k, i].x > 0.5]
                logContents += 'A%d: %s\n' % (k, str(assignedTasks))
                for r in R_k[k]:
                    krN = N_kr[k, r]
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
                    logContents += '\t R%d: %s\n' % (r, '-'.join(route))
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
