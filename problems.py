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
    return get_prmt(_warehouses, _tasks, _agents, prefix, dpath)


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
    return get_prmt(warehouses, tasks, agents, prefix, dpath, isEuDist=False)


def get_locID(_loc, loc_id):
    loc = tuple(_loc)
    if loc not in loc_id:
        locID = len(loc_id)
        loc_id[loc] = len(loc_id)
    else:
        locID = loc_id[loc]
    return locID


def get_prmt(_warehouses, _tasks, _agents, prefix, dpath, isEuDist=True):
    warehouses, tasks, agents = [], [], []
    loc_id = {}
    for wh0 in _warehouses:
        wLocID = get_locID(wh0['locXY'], loc_id)
        wh1 = {'locID': wLocID}
        for k in ['locTW', 'locST']:
            wh1[k] = wh0[k]
        warehouses.append(wh1)
    for tk0 in _tasks:
        wLocID = warehouses[tk0['wid']]['locID']
        dLocID = get_locID(tk0['locXY'], loc_id)
        tk1 = {'wID': wLocID, 'locID': dLocID, }
        for k in ['reward', 'volume', 'weight', 'locTW', 'locST']:
            tk1[k] = tk0[k]
        tasks.append(tk1)
    for ag0 in _agents:
        ag1 = {'volumeCap': ag0['volumeCap'],
               'weightCap': ag0['weightCap']}
        RRs1 = []
        for rr0 in ag0['RRs']:
            rr1 = {'locID': [get_locID(loc, loc_id) for loc in rr0['locXY']]}
            for k in ['prob', 'TB', 'locTW', 'locST']:
                rr1[k] = rr0[k]
            RRs1.append(rr1)
        ag1['RRs'] = RRs1
        agents.append(ag1)
    travel_time = {}
    for loc0, locID0 in loc_id.items():
        for loc1, locID1 in loc_id.items():
            if isEuDist:
                travel_time[locID0, locID1] = np.linalg.norm(np.array(loc0) - np.array(loc1))
            else:
                travel_time[locID0, locID1] = vincenty(np.array(loc0), np.array(loc1)).km / CAR_SPEED
    #
    with open(opath.join(dpath, 'WTA_%s.pkl' % prefix), 'wb') as fp:
        pickle.dump([_warehouses, _tasks, _agents], fp)
    problem = [prefix,
               warehouses, tasks, agents, travel_time]
    prmt = convert_prob2prmt(*problem)
    with open(opath.join(dpath, 'prmt_%s.pkl' % prefix), 'wb') as fp:
        pickle.dump(prmt, fp)
    #
    return prmt


def insert_task(seq0, tid, h_i, a_i, b_i, c_i, t_ij):
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
        for s1 in range(s0, len(seq0)):
            seq1 = seq0[:]
            seq1.insert(s1, dn)
            if check_TW_violation(seq1):
                continue
            tt = sum(t_ij[seq1[i], seq1[i + 1]] for i in range(len(seq1) - 1))
            if tt < min_tt:
                min_tt, best_seq = tt, seq1
    else:
        # Insert both the warehouse and delivery nodes
        for s0 in range(1, len(seq0)):
            for s1 in range(s0, len(seq0)):
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


def convert_prob2prmt(problemName,
                      warehouses, tasks, agents, travel_time):
    H = list(range(len(warehouses)))
    #
    K = list(range(len(tasks)))
    h_k, n_k, v_k, w_k, r_k = [], [], [], [], []
    #
    A = list(range(len(agents)))
    v_a, w_a, E_a = [], [], []
    p_ae, l_ae, u_ae = {}, {}, {}
    #
    N = []
    S_ae, c_aeij, N_ae = {}, {}, {}
    #
    t_ij = {}
    al_i, be_i, ga_i = {}, {}, {}
    F_ae = {}
    #
    _N = {}
    for h in H:
        wh = warehouses[h]
        _n = 'w%d' % h
        al_i[_n], be_i[_n] = wh['locTW'].tolist()
        ga_i[_n] = wh['locST']
        _N[_n] = wh['locID']
        N.append(_n)
    for k in K:
        tk = tasks[k]
        h_k.append('w%d' % tk['wID'])
        _n = 'n%d' % k
        n_k.append(_n)
        v_k.append(tk['volume'])
        w_k.append(tk['weight'])
        r_k.append(tk['reward'])
        al_i[_n], be_i[_n] = tk['locTW'].tolist()
        ga_i[_n] = tk['locST']
        _N[_n] = tk['locID']
        N.append(_n)
    _S_ae = {}
    for a in A:
        ag = agents[a]
        v_a.append(ag['volumeCap'])
        w_a.append(ag['weightCap'])
        E_a.append(list(range(len(ag['RRs']))))
        for e, rr in enumerate(ag['RRs']):
            _S_ae[a, e] = rr['locID']
            S_ae[a, e] = []
            p_ae[a, e] = rr['prob']
            u_ae[a, e] = rr['TB']
            l_ae[a, e] = 0.0
            for s0 in range(len(_S_ae[a, e])):
                _n0 = 's%d_%d_%d' % (s0, a, e)
                S_ae[a, e].append(_n0)
                al_i[_n0], be_i[_n0] = rr['locTW'][s0].tolist()
                ga_i[_n0] = rr['locST'][s0]
                if s0 != len(_S_ae[a, e]) - 1:
                    l_ae[a, e] += travel_time[_S_ae[a, e][s0], _S_ae[a, e][s0 + 1]]
                for s1 in range(s0, len(_S_ae[a, e])):
                    _n1 = 's%d_%d_%d' % (s1, a, e)
                    c_aeij[a, e, _n0, _n1] = 1
                    c_aeij[a, e, _n1, _n0] = 0
            N_ae[a, e] = N[:] + S_ae[a, e][:]
    #
    for _i in _N:
        for _j in _N:
            t_ij[_i, _j] = travel_time[_N[_i], _N[_j]]
    for a in A:
        for e in E_a[a]:
            for s0, locID0 in enumerate(_S_ae[a, e]):
                _n0 = 's%d_%d_%d' % (s0, a, e)
                for _n1 in _N:
                    t_ij[_n1, _n0] = travel_time[_N[_n1], locID0]
                    t_ij[_n0, _n1] = travel_time[locID0, _N[_n1]]
                for s1, locID1 in enumerate(_S_ae[a, e]):
                    _n1 = 's%d_%d_%d' % (s1, a, e)
                    t_ij[_n0, _n1] = travel_time[locID0, locID1]
    #
    for a in A:
        for e in E_a[a]:
            krS = S_ae[a, e]
            F_ae[a, e] = []
            for k in K:
                min_tt, best_seq = insert_task(krS, k, h_k, al_i, be_i, ga_i, t_ij)
                if min_tt - l_ae[a, e] <= u_ae[a, e]:
                    F_ae[a, e].append(k)
    #
    return {'problemName': problemName,
            'H': H,
            'K': K,
                'h_k': h_k, 'n_k': n_k, 'v_k': v_k, 'w_k': w_k,'r_k': r_k,
            'N': N,
            'A': A,
                'v_a': v_a, 'w_a': w_a,
                'E_a': E_a,
                    'p_ae': p_ae, 'l_ae': l_ae, 'u_ae': u_ae,
                    'S_ae': S_ae, 'N_ae': N_ae,
                    'c_aeij': c_aeij,
            't_ij': t_ij,
            'al_i': al_i, 'be_i': be_i, 'ga_i': ga_i,
            'F_ae': F_ae
            }


def prmt_pkl2json(prmt, dpath='_temp'):
    for prmtName in ['p_ae', 'l_ae', 'u_ae',
                     'S_ae', 'N_ae', 'F_ae']:
        converted = {}
        for (a, e), v in prmt[prmtName].items():
            converted['%d&%d' % (a, e)] = v
        prmt[prmtName] = converted
    converted = {}
    for (i, j), v in prmt['t_ij'].items():
        converted['%s&%s' % (i, j)] = v
    prmt['t_ij'] = converted
    converted = {}
    for (a, e, i, j), v in prmt['c_aeij'].items():
        converted['%d&%d&%s&%s' % (a, e, i, j)] = v
    prmt['c_aeij'] = converted

    with open(opath.join(dpath, 'prmt_%s.json' % prmt['problemName']), 'w') as outfile:
        outfile.write(json.dumps(prmt))


if __name__ == '__main__':
#    prmt_pkl2json(euclideanDistEx0())
    gNum, numAgents, seedNum = 0, 5, 0
    numTasks = 3
    prefix = 'g%d-na%03d-sn%02d' % (gNum, numAgents, seedNum)
    with open(opath.join('_temp', '%s.pkl' % prefix), 'rb') as fp:
        agents, tasks = pickle.load(fp)
    gen_prmt_AGTK(agents, tasks, prefix, dpath='_temp', tb_ratio=0.3)

    #
    # agt_fpath = opath.join(exp_dpath, 'agent-g0-na005-sn00.pkl')
    # tk_fpath = opath.join(exp_dpath, 'task-g0-na005-sn00-nt003.pkl')
    # prmt = gen_prmt_AGTK(agt_fpath, tk_fpath)
    # prmt_pkl2json(prmt, dpath=exp_dpath)


