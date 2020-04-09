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


def def_FC_cnsts_aeGiven(a, e, prob, ILP, x_aeij):
    # Initiate flow
    ILP.addConstr(quicksum(x_aeij[a][e][prob['o_ae'][a][e]][j] for j in prob['N_ae'][a][e]) == 1,
                  name='iFO(%d)(%d)' % (a, e))
    ILP.addConstr(quicksum(x_aeij[a][e][j][prob['d_ae'][a][e]] for j in prob['N_ae'][a][e]) == 1,
                  name='iFD(%d)(%d)' % (a, e))
    for i in prob['R_ae'][a][e]:
        if i == prob['o_ae'][a][e] or i == prob['d_ae'][a][e]:
            continue
        ILP.addConstr(quicksum(x_aeij[a][e][i][j] for j in prob['N_ae'][a][e] if j != i) == 1,
                      name='iFR1(%d)(%d)(%s)' % (a, e, i))
        ILP.addConstr(quicksum(x_aeij[a][e][j][i] for j in prob['N_ae'][a][e] if j != i) == 1,
                      name='iFR2(%d)(%d)(%s)' % (a, e, i))
    #
    ILP.addConstr(x_aeij[a][e][prob['d_ae'][a][e]][prob['o_ae'][a][e]] == 1,
                  name='CF(%d)(%d)' % (a, e))  # Circular Flow for the branch-and-cut algorithm
    ILP.addConstr(quicksum(x_aeij[a][e][i][i] for i in prob['N_ae'][a][e]) == 0,
                  name='NS(%d)(%d)' % (a, e))  # No Self Flow; tightening bounds
    #
    # Flow about delivery nodes; only when the warehouse visited
    for k in prob['K_ae'][a][e]:
        ILP.addConstr(quicksum(x_aeij[a][e][prob['n_k'][k]][j] for j in prob['N_ae'][a][e]) <=
                      quicksum(x_aeij[a][e][j][prob['h_k'][k]] for j in prob['N_ae'][a][e]),
                      name='tFC(%d)(%d)(%d)' % (a, e, k))
    # Flow conservation
    for i in prob['PD_ae'][a][e]:
        ILP.addConstr(quicksum(x_aeij[a][e][i][j] for j in prob['N_ae'][a][e]) <= 1,
                      name='FC_1(%d)(%d)(%s)' % (a, e, i))
        ILP.addConstr(quicksum(x_aeij[a][e][i][j] for j in prob['N_ae'][a][e]) ==
                      quicksum(x_aeij[a][e][j][i] for j in prob['N_ae'][a][e]),
                      name='FC(%d)(%d)(%s)' % (a, e, i))


def def_AT_cnsts_aeGiven(a, e, prob, ILP, x_aeij, u_aei):
    # Initialize the arrival time on the origin node
    ILP.addConstr(u_aei[a][e][prob['o_ae'][a][e]] == prob['al_i'][prob['o_ae'][a][e]],
                  name='IA(%d)(%s)' % (a, e))
    # Arrival time calculation
    for i in prob['N_ae'][a][e]:
        for j in prob['N_ae'][a][e]:
            if i == prob['d_ae'][a][e] and j == prob['o_ae'][a][e]:
                continue
            ILP.addConstr(u_aei[a][e][i] + prob['t_ij'][i][j] <=
                          u_aei[a][e][j] + prob['M'] * (1 - x_aeij[a][e][i][j]),
                          name='AT(%d)(%d)(%s)(%s)' % (a, e, i, j))
    # Time Window
    for i in prob['N_ae'][a][e]:
        ILP.addConstr(prob['al_i'][i] <= u_aei[a][e][i],
                      name='TW1(%d)(%d)(%s)' % (a, e, i))
        ILP.addConstr(u_aei[a][e][i] <= prob['be_i'][i],
                      name='TW2(%d)(%d)(%s)' % (a, e, i))
    # Routine route preservation
    for i in prob['R_ae'][a][e]:
        for j in prob['R_ae'][a][e]:
            ILP.addConstr(prob['c_aeij'][a][e][i][j] * u_aei[a][e][i] <= u_aei[a][e][j],
                          name='RR_P(%d)(%d)(%s)(%s)' % (a, e, i, j))
    # Warehouse and Delivery Sequence
    for k in prob['K_ae'][a][e]:
        ILP.addConstr(u_aei[a][e][prob['h_k'][k]] <=
                      u_aei[a][e][prob['n_k'][k]] + prob['M'] * (1 - quicksum(x_aeij[a][e][prob['n_k'][k]][j] for j in prob['N_ae'][a][e])),
                      name='WD_S(%d)(%d)(%d)' % (a, e, k))
    # Volume Limit
    const = LinExpr()
    for k in prob['K_ae'][a][e]:
        for j in prob['N_ae'][a][e]:
            const += prob['v_k'][k] * x_aeij[a][e][j][prob['n_k'][k]]
    ILP.addConstr(const <= prob['v_a'][a], name='VL(%d)(%d)' % (a, e))
    # Weight Limit
    const = LinExpr()
    for k in prob['K_ae'][a][e]:
        for j in prob['N_ae'][a][e]:
            const += prob['w_k'][k] * x_aeij[a][e][j][prob['n_k'][k]]
    ILP.addConstr(const <= prob['w_a'][a], name='VL(%d)(%d)' % (a, e))
    # Time Limit
    const = LinExpr()
    for i in prob['N_ae'][a][e]:
        for j in prob['N_ae'][a][e]:
            if (i == prob['d_ae'][a][e] and j == prob['o_ae'][a][e]):
                continue
            const += prob['t_ij'][i][j] * x_aeij[a][e][i][j]
    ILP.addConstr(const <= prob['u_ae'][a][e], name='TL(%d)(%d)' % (a, e))


def run(prob, etc=None):
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
    ILP = Model('ILP')
    y_ak = [
                [
                    ILP.addVar(vtype=GRB.BINARY,
                        name='y(%d)(%d)' % (a, k))
                for k in prob['K']]
            for a in prob['A']]
    z_aek = [
                [
                    [
                        ILP.addVar(vtype=GRB.BINARY, name='z(%d)(%d)(%d)' % (a, e, k))
                    for k in prob['K']]
                for e in prob['E_a'][a]]
             for a in prob['A']]
    u_aei = [
                [
                    [
                        ILP.addVar(vtype=GRB.CONTINUOUS, name='u(%d)(%d)(%d)' % (a, e, i, ))
                    for i in prob['cN']]
                for e in prob['E_a'][a]]
             for a in prob['A']]
    x_aeij = [
                [
                    [
                        [
                            None
                            for _ in prob['cN']]
                        for _ in prob['cN']]
                    for _ in prob['E_a'][a]]
                for a in prob['A']]
    for a in prob['A']:
        for e in prob['E_a'][a]:
            for i in prob['N_ae'][a][e]:
                for j in prob['N_ae'][a][e]:
                    x_aeij[a][e][i][j] = ILP.addVar(vtype=GRB.BINARY, name='x(%d)(%d)(%d)(%d)' % (a, e, i, j))
    ILP.update()
    #
    obj = LinExpr()
    for k in prob['K']:
        for a in prob['A']:
            obj += prob['r_k'][k] * y_ak[a][k]
            for e in prob['E_a'][a]:
                obj -= prob['r_k'][k] * prob['p_ae'][a][e] * z_aek[a][e][k]
    ILP.setObjective(obj, GRB.MAXIMIZE)
    #
    # Evaluation of the Task Assignment
    #
    for k in prob['K']:
        ILP.addConstr(quicksum(y_ak[a][k] for a in prob['A']) <= 1,
                     name='TAS(%d)' % k)  # Task Assignment
    for a in prob['A']:
        for e in prob['E_a'][a]:
            for k in prob['K']:
                if k not in prob['K_ae'][a][e]:
                    ILP.addConstr(y_ak[a][k] == 0,
                                  name='IA(%d)(%d)(%d)' % (a, e, k))  # Infeasible Assignment
    for a in prob['A']:
        for e in prob['E_a'][a]:
            for k in prob['K']:
                ILP.addConstr(z_aek[a][e][k] <= y_ak[a][k],
                     name='TAC(%d)(%d)(%d)' % (a, e, k))  # Task Accomplishment
    #
    # Constraints associated with the routing
    #
    for a in prob['A']:
        for e in prob['E_a'][a]:
            def_FC_cnsts_aeGiven(a, e, prob, ILP, x_aeij)
            def_AT_cnsts_aeGiven(a, e, prob, ILP, x_aeij, u_aei)
    #
    # Complicated and Combined constraints
    #
    for a in prob['A']:
        for e in prob['E_a'][a]:
            for k in prob['K_ae'][a][e]:
                ILP.addConstr(y_ak[a][k] - quicksum(x_aeij[a][e][j][prob['n_k'][k]] for j in prob['N_ae'][a][e]) <=
                             z_aek[a][e][k],
                             name='CC(%d)(%d)(%d)' % (a, e, k))
    #

    # ILP.addConstr(y_ak[0][5] == 1)
    # ILP.addConstr(y_ak[2][1] == 1)
    # ILP.addConstr(y_ak[2][3] == 1)










    #
    ILP.setParam('LazyConstraints', True)
    ILP.setParam('Threads', NUM_CORES)
    if etc['logFile']:
        ILP.setParam('LogFile', etc['logFile'])
    ILP.optimize(callbackF)
    #
    if ILP.status == GRB.Status.INFEASIBLE:
        ILP.write('%s.lp' % prob['problemName'])
        ILP.computeIIS()
        ILP.write('%s.ilp' % prob['problemName'])
    #
    if etc and ILP.status != GRB.Status.INFEASIBLE:
        for k in ['solFileCSV', 'solFilePKL', 'solFileTXT']:
            assert k in etc
        #
        endCpuTime, endWallTime = time.process_time(), time.perf_counter()
        eliCpuTime, eliWallTime = endCpuTime - startCpuTime, endWallTime - startWallTime
        res2file(etc['solFileCSV'], ILP.objVal, ILP.MIPGap, eliCpuTime, eliWallTime)
        #
        _y_ak = [[y_ak[a][k].x for k in prob['K']] for a in prob['A']]
        _z_aek = [[[z_aek[a][e][k].x for k in prob['K']] for e in prob['E_a'][a]] for a in prob['A']]
        _u_aei = [[[u_aei[a][e][i].x for i in prob['cN']] for e in prob['E_a'][a]] for a in prob['A']]
        _x_aeij = [[[[None for _ in prob['cN']] for _ in prob['cN']] for _ in prob['E_a'][a]] for a in prob['A']]
        for a in prob['A']:
            for e in prob['E_a'][a]:
                for i in prob['N_ae'][a][e]:
                    for j in prob['N_ae'][a][e]:
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
            for a in prob['A']:
                assignedTasks = [k for k in prob['K'] if _y_ak[a][k] > 0.5]
                associatedNodes = [(prob['h_k'][k], prob['n_k'][k]) for k in assignedTasks]
                f.write('A%d: T %s; N %s\n' % (a, str(assignedTasks), str(associatedNodes)))
                for e in prob['E_a'][a]:
                    fromToPairs = {}
                    for i in prob['N_ae'][a][e]:
                        for j in prob['N_ae'][a][e]:
                            if _x_aeij[a][e][i][j] > 0.5:
                                fromToPairs[i] = j
                    i = prob['o_ae'][a][e]
                    route = []
                    while i != prob['d_ae'][a][e]:
                        route.append(i)
                        i = fromToPairs[i]
                    route.append(i)
                    _RR = '-'.join([str(i) for i in prob['R_ae'][a][e]])
                    _CT = str([k for k in assignedTasks if prob['h_k'][k] in route and prob['n_k'][k] in route])
                    f.write('\t R%d: RR %s; CT %s\n' % (e, _RR, _CT))
                    f.write('\t\t %s\n' % '-'.join(['%s(%.2f)' % (i, _u_aei[a][e][i]) for i in route]))


if __name__ == '__main__':
    import pickle, json

    # from problems import euclideanDistEx0
    # prmt = euclideanDistEx0()

    # with open(opath.join('_temp', 'prob_na005-nt010-ca010-sn19.json'), 'rb') as fp:
    #     prob = pickle.load(fp)
    fpath = opath.join('_temp', 'prob_na005-nt010-ca010-sn19.json')
    with open(fpath) as f:
        prob = json.load(f)

    problemName = prob['problemName']
    approach = 'ILP'
    etc = {'solFilePKL': opath.join('_temp', 'ILP_%s_%s.pkl' % (problemName, approach)),
           'solFileCSV': opath.join('_temp', 'ILP_%s_%s.csv' % (problemName, approach)),
           'solFileTXT': opath.join('_temp', 'sILP_%s_%s.txt' % (problemName, approach)),
           'logFile': opath.join('_temp', 'ILP_%s_%s.log' % (problemName, approach)),
           'itrFileCSV': opath.join('_temp', 'ILP_%s_itr%s.csv' % (problemName, approach)),
           }

    run(prob, etc)
