import os.path as opath
import os
import csv, pickle
import json
import numpy as np
from geopy.distance import vincenty
from random import random
#
from __path_organizer import exp_dpath

MIN60, SEC60 = 60.0, 60.0
Meter1000 = 1000.0
CAR_SPEED = 30.0  # km/hour

DEFAULT_REWARD = 1.0
DEFAULT_SERVICE_TIME = 5.0 / MIN60  # 5 min. (the unit is hour)
HOUR24 = 24.0
DEFAULT_TW = np.array([0.0, HOUR24])
TW_NOON_BEFORE = np.array([0.0, HOUR24 / 2])
TW_NOON_AFTER = np.array([HOUR24 / 2, HOUR24])

# DEFAULT_VOLUME_CAPACITY, DEFAULT_WEIGHT_CAPACITY = 5.0, 5.0
DEFAULT_VOLUME_CAPACITY, DEFAULT_WEIGHT_CAPACITY = 10.0, 10.0
DEFAULT_VOLUME, DEFAULT_WEIGHT = 1.0, 1.0
DEFAULT_STAYING_TIME = 30.0 / MIN60  # 30 min. (the unit is hour)
ZERO_DURATION = 0.0
ARRIVAL_TIME_SPREAD = 30.0 / MIN60  # 30 min. (the unit is hour)



convert_t2h = lambda dt: dt.hour + dt.minute / MIN60 + dt.second / (MIN60 * SEC60)


def euclideanDistEx0(dpath='_temp'):
    prefix = 'ED_Ex0'
    _warehouses = [
                    {'locXY': np.array([0.20, 0.21]),
                     'locTW': np.array([0.00, 10.00]),
                     'locST': 0.10},
                    {'locXY': np.array([0.10, 0.40]),
                     'locTW': np.array([0.00, 10.00]),
                     'locST': 0.10},
                  ]
    _tasks = [
                {'reward': DEFAULT_REWARD, 'volume': DEFAULT_VOLUME, 'weight': DEFAULT_WEIGHT,
                 'wid': 0,
                 'locXY': np.array([0.7, 0.1]), 'locTW': np.array([0.00, 5.90]), 'locST': 0.1},
                {'reward': DEFAULT_REWARD, 'volume': DEFAULT_VOLUME, 'weight': DEFAULT_WEIGHT,
                 'wid': 0,
                 'locXY': np.array([0.8, 0.2]), 'locTW': np.array([0.00, 5.95]), 'locST': 0.1},
                {'reward': DEFAULT_REWARD, 'volume': DEFAULT_VOLUME, 'weight': DEFAULT_WEIGHT,
                 'wid': 1,
                 'locXY': np.array([0.5, 0.6]), 'locTW': np.array([0.00, 5.95]), 'locST': 0.1},
                {'reward': DEFAULT_REWARD, 'volume': DEFAULT_VOLUME, 'weight': DEFAULT_WEIGHT,
                 'wid': 1,
                 'locXY': np.array([0.5, 0.6]), 'locTW': np.array([0.00, 5.99]), 'locST': 0.1},
                {'reward': DEFAULT_REWARD, 'volume': DEFAULT_VOLUME, 'weight': DEFAULT_WEIGHT,
                 'wid': 1,
                 'locXY': np.array([0.1, 0.6]), 'locTW': np.array([0.00, 5.90]), 'locST': 0.1},
             ]
    _agents = [
                {'volumeCap': DEFAULT_VOLUME_CAPACITY, 'weightCap': DEFAULT_WEIGHT_CAPACITY,
                 'RRs': [
                    {'prob': 0.2,
                     'TB': 10.0,
                     'locXY': [np.array([0.10, 0.15]), np.array([0.15, 0.20]), np.array([0.70, 0.20]), np.array([0.90, 0.15])],
                     'locTW': [np.array([0.00, 0.20]), np.array([0.20, 5.60]), np.array([0.60, 5.80]), np.array([0.80, 8.15])],
                     'locST': [0.00, 0.05, 0.05, 0.00]},
                    {'prob': 0.5,
                     'TB': 3.0,
                     'locXY': [np.array([0.10, 0.18]), np.array([0.20, 0.16]), np.array([0.60, 0.10]), np.array([0.87, 0.18])],
                     'locTW': [np.array([0.00, 0.28]), np.array([0.28, 5.70]), np.array([0.70, 5.90]), np.array([1.00, 6.38])],
                     'locST': [0.00, 0.10, 0.10, 0.00]},
                    {'prob': 0.3,
                     'TB': 1.0,
                     'locXY': [np.array([0.11, 0.20]), np.array([0.20, 0.25]), np.array([0.93, 0.20])],
                     'locTW': [np.array([0.00, 0.20]), np.array([0.20, 5.50]), np.array([0.60, 7.20])],
                     'locST': [0.00, 0.15, 0.00]},
                   ]
                },
                {'volumeCap': DEFAULT_VOLUME_CAPACITY, 'weightCap': DEFAULT_WEIGHT_CAPACITY,
                 'RRs': [
                     {'prob': 0.7,
                      'TB': 1.0,
                      'locXY': [np.array([0.10, 0.85]), np.array([0.20, 0.80]), np.array([0.80, 0.60]), np.array([0.95, 0.50])],
                      'locTW': [np.array([0.00, 0.35]), np.array([0.35, 5.50]), np.array([0.80, 6.00]), np.array([1.20, 6.50])],
                      'locST':[0.00, 0.10, 0.10, 0.00]},
                     {'prob': 0.3,
                      'TB': 1.0,
                      'locXY': [np.array([0.13, 0.90]), np.array([0.25, 0.75]), np.array([0.75, 0.65]), np.array([0.95, 0.60])],
                      'locTW': [np.array([0.00, 0.20]), np.array([0.25, 5.75]), np.array([0.75, 5.95]), np.array([0.95, 7.60])],
                      'locST': [0.00, 0.10, 0.10, 0.00]}
                   ]
                },
              ]
    #
    return get_prob(_warehouses, _tasks, _agents, prefix, dpath)


def get_prob(_warehouses, _tasks, _agents, prefix, dpath, isEuDist=True):
    warehouses, tasks, agents = [], [], []
    locXY_locID = []
    for wh0 in _warehouses:
        wLocID = len(locXY_locID)
        locXY_locID.append([wh0['locXY'], wLocID])
        wh1 = {'locID': wLocID}
        for k in ['locTW', 'locST']:
            wh1[k] = wh0[k]
        warehouses.append(wh1)
    for tk0 in _tasks:
        wLocID = warehouses[tk0['wid']]['locID']
        dLocID = len(locXY_locID)
        locXY_locID.append([tk0['locXY'], dLocID])
        tk1 = {'wID': wLocID, 'locID': dLocID, }
        for k in ['reward', 'volume', 'weight', 'locTW', 'locST']:
            tk1[k] = tk0[k]
        tasks.append(tk1)
    for ag0 in _agents:
        ag1 = {'volumeCap': ag0['volumeCap'],
               'weightCap': ag0['weightCap']}
        RRs1 = []
        for rr0 in ag0['RRs']:
            locIDs = []
            for locXY in rr0['locXY']:
                locID = len(locXY_locID)
                locXY_locID.append([locXY, locID])
                locIDs.append(locID)
            rr1 = {'locID': locIDs}
            for k in ['prob', 'TB', 'locTW', 'locST']:
                rr1[k] = rr0[k]
            RRs1.append(rr1)
        ag1['RRs'] = RRs1
        agents.append(ag1)
    travel_time = {}
    for loc0XY, locID0 in locXY_locID:
        for loc1XY, locID1 in locXY_locID:
            if isEuDist:
                travel_time[locID0, locID1] = np.linalg.norm(loc0XY - loc1XY)
            else:
                travel_time[locID0, locID1] = vincenty(loc0XY, loc1XY).km / CAR_SPEED
    #
    with open(opath.join(dpath, 'WTA_%s.pkl' % prefix), 'wb') as fp:
        pickle.dump([_warehouses, _tasks, _agents], fp)
    problem = [prefix,
               warehouses, tasks, agents, travel_time]
    mn = convert_prob2mn(*problem)
    with open(opath.join(dpath, 'prob_%s.pkl' % prefix), 'wb') as fp:
        pickle.dump(mn, fp)
    #
    with open(opath.join(dpath, 'prob_%s.json' % mn['problemName']), 'w') as outfile:
        outfile.write(json.dumps(mn))
    return mn


def convert_prob2mn(problemName,
                    warehouses, tasks, agents, travel_time):
    #
    # This function is for converting the problem into the notation of the mathematical model
    #
    #
    # Ready for matching IDs
    #
    nLocID_oLocID, oLocID_nLocID = {}, {}
    TW_oLocID, ST_oLocID = {}, {}
    for wh in warehouses:
        nLocID = len(nLocID_oLocID)
        oLocID = wh['locID']
        nLocID_oLocID[nLocID] = oLocID
        oLocID_nLocID[oLocID] = nLocID
        TW_oLocID[oLocID] = wh['locTW'].tolist()
        ST_oLocID[oLocID] = wh['locST']
    for tk in tasks:
        nLocID = len(nLocID_oLocID)
        oLocID = tk['locID']
        nLocID_oLocID[nLocID] = oLocID
        oLocID_nLocID[oLocID] = nLocID
        TW_oLocID[oLocID] = tk['locTW'].tolist()
        ST_oLocID[oLocID] = tk['locST']
    for ag in agents:
        for rr in ag['RRs']:
            for i, oLocID in enumerate(rr['locID']):
                nLocID = len(nLocID_oLocID)
                nLocID_oLocID[nLocID] = oLocID
                oLocID_nLocID[oLocID] = nLocID
                TW_oLocID[oLocID] = rr['locTW'][i].tolist()
                ST_oLocID[oLocID] = rr['locST'][i]
    #
    # Initialize Inputs
    #
    numTasks = len(tasks)
    K = list(range(numTasks))
    v_k = [None for _ in range(numTasks)]
    w_k = [None for _ in range(numTasks)]
    r_k = [None for _ in range(numTasks)]
    h_k = [None for _ in range(numTasks)]
    n_k = [None for _ in range(numTasks)]
    #
    numAgents = len(agents)
    A = list(range(numAgents))
    v_a = [None for _ in range(numAgents)]
    w_a = [None for _ in range(numAgents)]
    E_a = [None for _ in range(numAgents)]
    for a in A:
        ag = agents[a]
        E_a[a] = list(range(len(ag['RRs'])))
    p_ae = [[None for _ in range(len(E_a[a]))]
                for a in range(numAgents)]
    u_ae = [[None for _ in range(len(E_a[a]))]
                for a in range(numAgents)]
    o_ae = [[None for _ in range(len(E_a[a]))]
                for a in range(numAgents)]
    d_ae = [[None for _ in range(len(E_a[a]))]
                for a in range(numAgents)]
    #
    R_ae = [[[] for _ in range(len(E_a[a]))]
                for a in range(numAgents)]
    K_ae = [[[] for _ in range(len(E_a[a]))]
                for a in range(numAgents)]
    P_ae = [[[] for _ in range(len(E_a[a]))]
                for a in range(numAgents)]
    D_ae = [[[] for _ in range(len(E_a[a]))]
                for a in range(numAgents)]
    PD_ae = [[[] for _ in range(len(E_a[a]))]
                for a in range(numAgents)]
    N_ae = [[[] for _ in range(len(E_a[a]))]
                for a in range(numAgents)]
    #
    numNodes = len(nLocID_oLocID)
    cN = list(range(numNodes))
    t_ij = [[None for _ in range(numNodes)] for _ in range(numNodes)]
    al_i = [None for _ in range(numNodes)]
    be_i = [None for _ in range(numNodes)]
    c_aeij = [
                [
                    [
                        [None for _ in range(numNodes)]
                        for _ in range(numNodes)]
                    for _ in range(len(E_a[a]))]
                for a in range(numAgents)
            ]
    #
    # Convert the problem into the notations of the mathematical programming
    #
    for k in K:
        tk = tasks[k]
        v_k[k] = tk['volume']
        w_k[k] = tk['weight']
        r_k[k] = tk['reward']
        #
        wh = warehouses[tk['wID']]
        new_pLocID, new_dLocID = oLocID_nLocID[wh['locID']], oLocID_nLocID[tk['locID']]
        h_k[k] = new_pLocID
        n_k[k] = new_dLocID
    #
    for a in A:
        ag = agents[a]
        v_a[a] = ag['volumeCap']
        w_a[a] = ag['weightCap']
        for e, rr in enumerate(ag['RRs']):
            p_ae[a][e] = rr['prob']
            u_ae[a][e] = rr['TB']
            for i, oLocID in enumerate(rr['locID']):
                nLocID = oLocID_nLocID[oLocID]
                R_ae[a][e].append(nLocID)
            o_ae[a][e] = R_ae[a][e][0]
            d_ae[a][e] = R_ae[a][e][-1]
            for i0 in range(len(R_ae[a][e])):
                nLocID0 = R_ae[a][e][i0]
                if i0 != len(R_ae[a][e]) - 1:
                    oLocID0 = nLocID_oLocID[R_ae[a][e][i0]]
                    oLocID1 = nLocID_oLocID[R_ae[a][e][i0 + 1]]
                    u_ae[a][e] += travel_time[oLocID0, oLocID1]
                for i1 in range(i0, len(R_ae[a][e])):
                    nLocID1 = R_ae[a][e][i1]
                    c_aeij[a][e][nLocID0][nLocID1] = 1
                    c_aeij[a][e][nLocID1][nLocID0] = 0
            #
            for k in K:
                tk = tasks[k]
                wh = warehouses[tk['wID']]
                oLocPID, oLocDID = wh['locID'], tk['locID']
                min_tt, best_seq = 1e400, None
                # Insert both pickup and delivery nodes
                for i0 in range(1, len(R_ae[a][e])):
                    for i1 in range(i0, len(R_ae[a][e])):
                        seq_oLocIDs = [nLocID_oLocID[nLocID] for nLocID in R_ae[a][e]]
                        seq_oLocIDs.insert(i0, oLocPID)
                        seq_oLocIDs.insert(i1 + 1, oLocDID)
                        # check_TW_violation
                        is_TW_feasible = True
                        n0_oLocID = seq_oLocIDs[0]
                        erest_deptTime = TW_oLocID[n0_oLocID][0] + ST_oLocID[n0_oLocID]
                        for n1_oLocID in seq_oLocIDs[1:]:
                            erest_arrvTime = erest_deptTime + travel_time[n0_oLocID, n1_oLocID]
                            if TW_oLocID[n1_oLocID][1] < erest_arrvTime:
                                is_TW_feasible = False
                                break
                            else:
                                erest_deptTime = max(erest_arrvTime, TW_oLocID[n1_oLocID][0]) + ST_oLocID[n1_oLocID]
                            n0_oLocID = n1_oLocID
                        if not is_TW_feasible:
                            continue
                        tt = sum(travel_time[seq_oLocIDs[i], seq_oLocIDs[i + 1]] + ST_oLocID[seq_oLocIDs[i]]
                                 for i in range(len(seq_oLocIDs) - 1))
                        if tt < min_tt:
                            min_tt, best_seq = tt, seq_oLocIDs
                if min_tt <= u_ae[a][e]:
                    K_ae[a][e].append(k)
                    P_ae[a][e].append(oLocID_nLocID[oLocPID])
                    D_ae[a][e].append(oLocID_nLocID[oLocDID])
            #
            P_ae[a][e] = list(set(P_ae[a][e]))
            D_ae[a][e] = list(set(D_ae[a][e]))
            PD_ae[a][e] = P_ae[a][e][:] + D_ae[a][e][:]
            N_ae[a][e] = PD_ae[a][e][:] + R_ae[a][e][:]
    maxTravelTime, maxServiceTime = 0.0, 0.0
    for nLocID0 in cN:
        oLocID0 = nLocID_oLocID[nLocID0]
        al_i[nLocID0], be_i[nLocID0] = TW_oLocID[oLocID0]
        for nLocID1 in cN:
            oLocID1 = nLocID_oLocID[nLocID1]
            if nLocID0 == nLocID1:
                t_ij[nLocID0][nLocID1] = 0
            else:
                t_ij[nLocID0][nLocID1] = travel_time[oLocID0, oLocID1] + ST_oLocID[oLocID0]
                if maxTravelTime < travel_time[oLocID0, oLocID1]:
                    maxTravelTime = travel_time[oLocID0, oLocID1]
        if maxServiceTime < ST_oLocID[oLocID0]:
            maxServiceTime = ST_oLocID[oLocID0]
    M = len(cN) * (maxTravelTime + maxServiceTime)
    #
    return {'problemName': problemName,
            'K': K,
                'v_k': v_k, 'w_k': w_k, 'r_k': r_k, 'h_k': h_k, 'n_k': n_k,
            'A': A,
                'v_a': v_a, 'w_a': w_a,
                'E_a': E_a,
                    'p_ae': p_ae, 'u_ae': u_ae, 'o_ae': o_ae, 'd_ae': d_ae,
                    'R_ae': R_ae, 'K_ae': K_ae, 'P_ae': P_ae, 'D_ae': D_ae, 'PD_ae': PD_ae, 'N_ae': N_ae,
            'cN': cN,
            't_ij': t_ij,
            'al_i': al_i, 'be_i': be_i,
            'c_aeij': c_aeij,
            'M': M,
            'nLocID_oLocID': nLocID_oLocID,
            'oLocID_nLocID': oLocID_nLocID}


def gen_prmt_AGTK(_agents, _tasks, prefix, dpath=exp_dpath, tb_ratio=0.3):
    with open(opath.join(dpath, 'AGTK_%s.pkl' % prefix), 'wb') as fp:
        pickle.dump([_agents, _tasks], fp)
    #
    warehouses, warehouses_wid = [], {}
    tasks = []
    for tk in _tasks:
        locW = tk['LocW']
        LatW, LngW, LatD, LngD = [tk[k] for k in ['LatW', 'LngW', 'LatD', 'LngD']]
        if locW not in warehouses_wid:
            warehouses_wid[locW] = len(warehouses_wid)
            warehouses.append({'locXY': np.array([LatW, LngW]),
                               'locTW': DEFAULT_TW,
                               'locST': DEFAULT_SERVICE_TIME})
        wid = warehouses_wid[locW]
        tasks.append({'reward': DEFAULT_REWARD, 'volume': DEFAULT_VOLUME, 'weight': DEFAULT_WEIGHT,
                      'wid': wid,
                      'locXY': np.array([LatD, LngD]),
                      'locTW': TW_NOON_BEFORE if random() > 0.5 else TW_NOON_AFTER,
                      'locST': DEFAULT_SERVICE_TIME})
    agents = []
    for agt in _agents:
        probSum = sum([aRR['prob'] for aRR in agt['RRs']])
        RRs = []
        for aRR in agt['RRs']:
            mvts = aRR['mvts']
            first_mvt, last_mvt = mvts[0], mvts[-1]
            o_kr, d_kr = first_mvt['traj'][0], last_mvt['traj'][-1]
            f_embark_time = convert_t2h(first_mvt['sTime'])
            l_alight = convert_t2h(last_mvt['eTime'])
            #
            distSum = 0.0
            locXY = [o_kr]
            locTW = [np.array([f_embark_time - ARRIVAL_TIME_SPREAD,
                               f_embark_time + ARRIVAL_TIME_SPREAD])]
            locST = [ZERO_DURATION]
            for i in range(len(mvts) - 1):
                p_mvts, n_mvts = mvts[i], mvts[i + 1]
                p_et, n_st, n_et = map(convert_t2h, [p_mvts['eTime'], n_mvts['sTime'], n_mvts['eTime']])
                meanP = np.mean([p_mvts['traj'][-1], n_mvts['traj'][0]], axis=0)
                distSum += vincenty(locXY[-1] if i != 0 else o_kr, meanP).km
                locXY.append(meanP)
                locTW.append(np.array([p_et - ARRIVAL_TIME_SPREAD,
                                       p_et + ARRIVAL_TIME_SPREAD]))
                locST.append(n_st - p_et)
            distSum += vincenty(locXY[-1], d_kr).km
            locXY.append(d_kr)
            locTW.append(np.array([l_alight - ARRIVAL_TIME_SPREAD,
                                   l_alight + ARRIVAL_TIME_SPREAD]))
            locST.append(ZERO_DURATION)
            RRs.append({'prob': aRR['prob'] / probSum,
                        'TB': (distSum / CAR_SPEED) * tb_ratio,
                        'locXY': locXY,
                        'locTW': locTW,
                        'locST': locST},)
        agents.append({'volumeCap': DEFAULT_VOLUME_CAPACITY, 'weightCap': DEFAULT_WEIGHT_CAPACITY,
                       'RRs': RRs})
    #
    return get_prob(warehouses, tasks, agents, prefix, dpath, isEuDist=False)


if __name__ == '__main__':
   euclideanDistEx0()
#     gNum, numAgents, seedNum = 0, 5, 0
#     numTasks = 3
#     prefix = 'g%d-na%03d-sn%02d' % (gNum, numAgents, seedNum)
#     with open(opath.join('_temp', '%s.pkl' % prefix), 'rb') as fp:
#         agents, tasks = pickle.load(fp)
#     gen_prmt_AGTK(agents, tasks, prefix, dpath='_temp', tb_ratio=0.3)

    #
    # agt_fpath = opath.join(exp_dpath, 'agent-g0-na005-sn00.pkl')
    # tk_fpath = opath.join(exp_dpath, 'task-g0-na005-sn00-nt003.pkl')
    # prmt = gen_prmt_AGTK(agt_fpath, tk_fpath)
    # prmt_pkl2json(prmt, dpath=exp_dpath)


