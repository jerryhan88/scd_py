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
    startCpuTime, startWallTime = time.process_time(), time.perf_counter()
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
            if time.process_time() - etc['startTS'] > etc['TimeLimit']:
                logContents = '\n'
                logContents += 'Interrupted by time limit\n'
                write_log(etc['logFile'], logContents)
                m.terminate()
            if time.perf_counter() - etc['lastLoggingTime'] > LOGGING_INTERVAL:
                etc['lastLoggingTime'] = time.perf_counter()
                eliCpuTimeP, eliWallTimeP = time.process_time() - etc['startCpuTime'], time.perf_counter() - etc['startWallTime']
                objbst = m.cbGet(GRB.Callback.MIP_OBJBST)
                objbnd = m.cbGet(GRB.Callback.MIP_OBJBND)
                gap = abs(objbst - objbnd) / (0.000001 + abs(objbst))
                itr2file(etc['itrFileCSV'], ['%.2f' % eliCpuTimeP, '%.2f' % eliWallTimeP,
                                             '%.2f' % objbst, '%.2f' % objbnd, '%.2f' % gap])
    #
    K, A = map(prmt.get, ['K', 'A'])
    v_k, w_k, r_k, h_k, n_k = map(prmt.get, ['v_k', 'w_k', 'r_k', 'h_k', 'n_k'])
    v_a, w_a, E_a = map(prmt.get, ['v_a', 'w_a', 'E_a'])
    p_ae, u_ae, o_ae, d_ae = map(prmt.get, ['p_ae', 'u_ae', 'o_ae', 'd_ae'])
    R_ae, K_ae, P_ae, D_ae, N_ae = map(prmt.get, ['R_ae', 'K_ae', 'P_ae', 'D_ae', 'N_ae'])
    cN, t_ij, al_i, be_i, c_aeij = map(prmt.get, ['cN', 't_ij', 'al_i', 'be_i', 'c_aeij'])
    M = prmt.get('M')
    #
    ILP = Model('ILP')
    y_ak = [
                [
                    ILP.addVar(vtype=GRB.BINARY,
                        name='y(%d)(%d)' % (a, k))
                for k in K]
            for a in A]
    z_aek = [
                [
                    [
                        ILP.addVar(vtype=GRB.BINARY, name='z(%d)(%d)(%d)' % (a, e, k))
                    for k in K]
                for e in E_a[a]]
             for a in A]
    u_aei = [
                [
                    [
                        ILP.addVar(vtype=GRB.CONTINUOUS, name='u(%d)(%d)(%d)' % (a, e, i, ))
                    for i in cN]
                for e in E_a[a]]
             for a in A]
    x_aeij = [
                [
                    [
                        [
                            None
                            for _ in cN]
                        for _ in cN]
                    for _ in E_a[a]]
                for a in A]
    for a in A:
        for e in E_a[a]:
            for i in N_ae[a][e]:
                for j in N_ae[a][e]:
                    x_aeij[a][e][i][j] = ILP.addVar(vtype=GRB.BINARY, name='x(%d)(%d)(%d)(%d)' % (a, e, i, j))
    ILP.update()
    #
    obj = LinExpr()
    for k in K:
        for a in A:
            obj += r_k[k] * y_ak[a][k]
            for e in E_a[a]:
                obj -= r_k[k] * p_ae[a][e] * z_aek[a][e][k]
    ILP.setObjective(obj, GRB.MAXIMIZE)
    #
    # Evaluation of the Task Assignment
    #
    for k in K:
        ILP.addConstr(quicksum(y_ak[a][k] for a in A) <= 1,
                     name='TAS(%d)' % k)  # Task Assignment
    for a in A:
        for e in E_a[a]:
            for k in K:
                if k not in K_ae[a][e]:
                    ILP.addConstr(y_ak[a][k] == 0,
                                  name='IA(%d)(%d)(%d)' % (a, e, k))  # Infeasible Assignment
    for a in A:
        for e in E_a[a]:
            for k in K:
                ILP.addConstr(z_aek[a][e][k] <= y_ak[a][k],
                     name='TAC(%d)(%d)(%d)' % (a, e, k))  # Task Accomplishment
    #
    # Constraints associated with the flow conservation
    #
    for a in A:
        for e in E_a[a]:
            # Initiate flow
            ILP.addConstr(quicksum(x_aeij[a][e][o_ae[a][e]][j] for j in N_ae[a][e]) == 1,
                          name='iFO(%d)(%d)' % (a, e))
            ILP.addConstr(quicksum(x_aeij[a][e][j][d_ae[a][e]] for j in N_ae[a][e]) == 1,
                          name='iFD(%d)(%d)' % (a, e))
            for i in R_ae[a][e]:
                if i == o_ae[a][e] or i == d_ae[a][e]:
                    continue
                ILP.addConstr(quicksum(x_aeij[a][e][i][j] for j in N_ae[a][e] if j != i) == 1,
                             name='iFR1(%d)(%d)(%s)' % (a, e, i))
                ILP.addConstr(quicksum(x_aeij[a][e][j][i] for j in N_ae[a][e] if j != i) == 1,
                             name='iFR2(%d)(%d)(%s)' % (a, e, i))
            #
            ILP.addConstr(x_aeij[a][e][d_ae[a][e]][o_ae[a][e]] == 1, name='CF(%d)(%d)' % (a, e))  # Circular Flow for the branch-and-cut algorithm
            ILP.addConstr(quicksum(x_aeij[a][e][i][i] for i in N_ae[a][e]) == 0, name='NS(%d)(%d)' % (a, e))  # No Self Flow; tightening bounds
            #
            # Flow about delivery nodes; only when the warehouse visited
            for k in K_ae[a][e]:
                ILP.addConstr(quicksum(x_aeij[a][e][n_k[k]][j] for j in N_ae[a][e]) <=
                              quicksum(x_aeij[a][e][j][h_k[k]] for j in N_ae[a][e]),
                            name='tFC(%d)(%d)(%d)' % (a, e, k))
            # Flow conservation
            for i in P_ae[a][e] + D_ae[a][e]:
                ILP.addConstr(quicksum(x_aeij[a][e][i][j] for j in N_ae[a][e]) <= 1,
                              name='FC_1(%d)(%d)(%s)' % (a, e, i))
                ILP.addConstr(quicksum(x_aeij[a][e][i][j] for j in N_ae[a][e]) ==
                             quicksum(x_aeij[a][e][j][i] for j in N_ae[a][e]),
                            name='FC(%d)(%d)(%s)' % (a, e, i))
    #
    # Constraints associated with the arrival time
    #
    for a in A:
        for e in E_a[a]:
            # Initialize the arrival time on the origin node
            ILP.addConstr(u_aei[a][e][i] == al_i[o_ae[a][e]],
                          name='IA(%d)(%s)' % (a, e))
            # Arrival time calculation
            for i in N_ae[a][e]:
                for j in N_ae[a][e]:
                    if i == d_ae[a][e] and j == o_ae[a][e]:
                        continue
                    ILP.addConstr(u_aei[a][e][i] + t_ij[i][j] <=
                                  u_aei[a][e][j] + M * (1 - x_aeij[a][e][i][j]),
                                  name='AT(%d)(%d)(%s)(%s)' % (a, e, i, j))
            # Time Window
            for i in N_ae[a][e]:
                ILP.addConstr(al_i[i] <= u_aei[a][e][i],
                              name='TW1(%d)(%d)(%s)' % (a, e, i))
                ILP.addConstr(u_aei[a][e][i] <= be_i[i],
                              name='TW2(%d)(%d)(%s)' % (a, e, i))
            # Routine route preservation
            for i in R_ae[a][e]:
                for j in R_ae[a][e]:
                    ILP.addConstr(c_aeij[a][e][i][j] * u_aei[a][e][i] <= u_aei[a][e][j],
                                  name='RR_P(%d)(%d)(%s)(%s)' % (a, e, i, j))
            # Warehouse and Delivery Sequence
            for k in K_ae[a][e]:
                ILP.addConstr(u_aei[a][e][h_k[k]] <=
                              u_aei[a][e][n_k[k]] + M * (1 - quicksum(x_aeij[a][e][n_k[k]][j] for j in N_ae[a][e])),
                              name='WD_S(%d)(%d)(%d)' % (a, e, k))
            # Volume Limit
            const = LinExpr()
            for k in K_ae[a][e]:
                for j in N_ae[a][e]:
                    const += v_k[k] * x_aeij[a][e][j][n_k[k]]
            ILP.addConstr(const <= v_a[a], name='VL(%d)(%d)' % (a, e))
            # Weight Limit
            const = LinExpr()
            for k in K_ae[a][e]:
                for j in N_ae[a][e]:
                    const += w_k[k] * x_aeij[a][e][j][n_k[k]]
            ILP.addConstr(const <= w_a[a], name='VL(%d)(%d)' % (a, e))
            # Time Limit
            const = LinExpr()
            for i in N_ae[a][e]:
                for j in N_ae[a][e]:
                    if (i == d_ae[a][e] and j == o_ae[a][e]):
                        continue
                    const += t_ij[i][j] * x_aeij[a][e][i][j]
            ILP.addConstr(const <= u_ae[a][e], name='TL(%d)(%d)' % (a, e))
    #
    # Complicated and Combined constraints
    #
    for a in A:
        for e in E_a[a]:
            for k in K_ae[a][e]:
                ILP.addConstr(y_ak[a][k] - quicksum(x_aeij[a][e][j][n_k[k]] for j in N_ae[a][e]) <=
                             z_aek[a][e][k],
                             name='CC(%d)(%d)(%d)' % (a, e, k))
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
        endCpuTime, endWallTime = time.process_time(), time.perf_counter()
        eliCpuTime, eliWallTime = endCpuTime - startCpuTime, endWallTime - startWallTime
        res2file(etc['solFileCSV'], ILP.objVal, ILP.MIPGap, eliCpuTime, eliWallTime)
        #
        _y_ak = [[y_ak[a][k].x for k in K] for a in A]
        _z_aek = [[[z_aek[a][e][k].x for k in K] for e in E_a[a]] for a in A]
        _u_aei = [[[u_aei[a][e][i].x for i in cN] for e in E_a[a]] for a in A]
        _x_aeij = [[[[None for _ in cN] for _ in cN] for _ in E_a[a]] for a in A]
        for a in A:
            for e in E_a[a]:
                for i in N_ae[a][e]:
                    for j in N_ae[a][e]:
                        _x_aeij[a][e][i][j] = x_aeij[a][e][i][j].x
        sol = {
            'y_ak': _y_ak, 'z_aek': _z_aek,
            'x_aeij': _x_aeij, 'u_aei': _u_aei,
        }
        with open(etc['solFilePKL'], 'wb') as fp:
            pickle.dump(sol, fp)
        #
        with open(etc['solFileTXT'], 'w') as f:
            f.write('Summary\n')
            f.write('\t Cpu Time: %f\n' % eliCpuTime)
            f.write('\t Wall Time: %f\n' % eliWallTime)
            f.write('\t ObjV: %.3f\n' % ILP.objVal)
            f.write('\t Gap: %.3f\n' % ILP.MIPGap)
            f.write('\n')
            f.write('Details\n')
            for a in A:
                assignedTasks = [k for k in K if _y_ak[a][k] > 0.5]
                associatedNodes = [(h_k[k], n_k[k]) for k in assignedTasks]
                f.write('A%d: T %s; N %s\n' % (a, str(assignedTasks), str(associatedNodes)))
                for e in E_a[a]:
                    fromToPairs = {}
                    for i in N_ae[a][e]:
                        for j in N_ae[a][e]:
                            if _x_aeij[a][e][i][j] > 0.5:
                                fromToPairs[i] = j
                    i = o_ae[a][e]
                    route = []
                    while i != d_ae[a][e]:
                        route.append(i)
                        i = fromToPairs[i]
                    route.append(i)
                    _RR = '-'.join([str(i) for i in R_ae[a][e]])
                    _CT = str([k for k in assignedTasks if h_k[k] in route and n_k[k] in route])
                    f.write('\t R%d: RR %s; CT %s\n' % (e, _RR, _CT))
                    f.write('\t\t %s\n' % '-'.join(['%s(%.2f)' % (i, _u_aei[a][e][i]) for i in route]))


if __name__ == '__main__':
    import pickle

    # from problems import euclideanDistEx0
    # prmt = euclideanDistEx0()

    with open(opath.join('_temp', 'prob_ED_Ex0.pkl'), 'rb') as fp:
        prob = pickle.load(fp)


    problemName = prob['problemName']
    approach = 'ILP'
    etc = {'solFilePKL': opath.join('_temp', 'sol_%s_%s.pkl' % (problemName, approach)),
           'solFileCSV': opath.join('_temp', 'sol_%s_%s.csv' % (problemName, approach)),
           'solFileTXT': opath.join('_temp', 'sol_%s_%s.txt' % (problemName, approach)),
           'logFile': opath.join('_temp', '%s_%s.log' % (problemName, approach)),
           'itrFileCSV': opath.join('_temp', '%s_itr%s.csv' % (problemName, approach)),
           }

    run(prob, etc)
