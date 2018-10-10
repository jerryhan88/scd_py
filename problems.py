import os.path as opath
import os
import csv, pickle
import json
import numpy as np
from geopy.distance import vincenty

#


def euclideanDistEx0(dpath='_temp'):
    def get_locID(_loc, loc_id):
        loc = tuple(_loc)
        if loc not in loc_id:
            locID = len(loc_id)
            loc_id[loc] = len(loc_id)
        else:
            locID = loc_id[loc]
        return locID
    #
    problemName = 'euclideanDistEx0'
    tasksLocTW = [
                # [reward, pSerTime, dSerTime
                #       (pLocX, pLocY), (dLocX, dLocY),
                #       (pTWa, pTWb), (pTWa, pTWb),
                # ]
                [1.0, 0.1, 0.1,
                    np.array((0.2, 0.21)), np.array((0.7, 0.1)),
                    np.array((0.1, 0.5)), np.array((0.5, 0.6)),
                ],
                [1.0, 0.1, 0.1,
                    np.array((0.3, 0.3)), np.array((0.8, 0.2)),
                    np.array((0.05, 0.95)), np.array((0.05, 0.95))],
                [1.0, 0.1, 0.1,
                    np.array((0.15, 0.9)), np.array((0.5, 0.6)),
                    np.array((0.05, 0.42)), np.array((0.5, 0.95))],
                [1.0, 0.1, 0.1,
                    np.array((0.2, 0.75)), np.array((0.6, 0.6)),
                    np.array((0.1, 0.4)), np.array((0.35, 0.99))],
                [1.0, 0.1, 0.1,
                    np.array((0.9, 0.9)), np.array((0.1, 0.6)),
                    np.array((0.16, 0.6)), np.array((0.3, 0.9))],
                ]
    agentsRRs = [
                # [
                #   [], routine route 0
                #   [Probability, timeBudget
                #       (oLocX, oLocY),
                #       [(sLoc0X, sLoc0Y), (sLoc1X, sLoc1Y), ...],
                #       (dLocX, dLocY),
                #   ], routine route 1
                #   ...
                # ],
                  [
                    [0.2, 10.0,
                        np.array((0.1, 0.15)),
                        [np.array((0.15, 0.2)), np.array((0.7, 0.2))],
                        np.array((0.9, 0.15))
                     ],
                    [0.5, 3.0,
                       np.array((0.1, 0.18)),
                       [np.array((0.2, 0.16)), np.array((0.6, 0.1))],
                       np.array((0.87, 0.18))
                     ],
                    [0.3, 1.0,
                        np.array((0.11, 0.2)),
                        [np.array((0.2, 0.25))],
                        np.array((0.93, 0.2))
                     ],

                  ],
                  [
                    [0.7, 1.0,
                        np.array((0.1, 0.85)),
                        [np.array((0.2, 0.8)), np.array((0.8, 0.6))],
                        np.array((0.95, 0.5))
                     ],
                    [0.3, 1.0,
                        np.array((0.13, 0.9)),
                        [np.array((0.25, 0.75)), np.array((0.75, 0.65))],
                        np.array((0.95, 0.6))
                     ],
                  ],
                 ]
    #
    tasks, agents = [], []
    loc_id = {}
    for reward, pST, dST, pLoc, dLoc, pTW, dTW in tasksLocTW:
        pLocID = get_locID(pLoc, loc_id)
        pa, pb = pTW.tolist()
        dLocID = get_locID(dLoc, loc_id)
        da, db = dTW.tolist()
        tasks.append([reward,
                      (pLocID, pa, pb, pST),
                      (dLocID, da, db, dST)])
    for arr in agentsRRs:
        routineRoutes = []
        for prob, timeBudget, oLoc, seqLocs, dLoc in arr:
            oLocID = get_locID(oLoc, loc_id)
            seqIDs = tuple([get_locID(loc, loc_id) for loc in seqLocs])
            dLocID = get_locID(dLoc, loc_id)
            routineRoutes.append((oLocID, seqIDs, dLocID, timeBudget, prob))
        agents.append(routineRoutes)
    travel_time = {}
    for loc0, locID0 in loc_id.items():
        for loc1, locID1 in loc_id.items():
            travel_time[locID0, locID1] = np.linalg.norm(np.array(loc0) - np.array(loc1))
    #
    dplym = [tasksLocTW, agentsRRs]
    with open(opath.join(dpath, 'dplym_%s.pkl' % problemName), 'wb') as fp:
        pickle.dump(dplym, fp)
    problem = [problemName,
               agents, tasks, travel_time]
    prmt = convert_prob2prmt(*problem)
    with open(opath.join(dpath, 'prmt_%s.pkl' % problemName), 'wb') as fp:
        pickle.dump(prmt, fp)
    return prmt


def ex0():
    class ServicePoint(object):
        def __init__(self, _id, lng, lat, earliestT, latestT, serviceT, reward):
            self._id, self.lng, self.lat = _id, lng, lat
            self.earliestT, self.latestT = earliestT, latestT
            self.serviceT, self.reward = serviceT, reward

    servicePoints = []
    tasks, agents = [], []
    #
    problemName = 'MRTLRT_list_2'
    SERIVE_EARLIEST_TIME, SERVICE_LATEST_TIME = 0, 1440
    SERVICE_TIME, REWARD = 10, 50
    sp_id = 0
    with open('%s.csv' % problemName) as r_csvfile:
        reader = csv.DictReader(r_csvfile)
        for row in reader:
            lng, lat = map(eval, [row[cn] for cn in ['longitude', 'latitude']])
            servicePoints.append(ServicePoint(sp_id, lng, lat,
                                              SERIVE_EARLIEST_TIME, SERVICE_LATEST_TIME,
                                              SERVICE_TIME, REWARD))
            sp_id += 1
    #
    travel_time = {}
    for sp0 in servicePoints:
        for sp1 in servicePoints:
            travel_time[sp0._id, sp1._id] = vincenty((sp0.lat, sp0.lng),
                                                     (sp1.lat, sp1.lng)).km
    #
    numTasks = int(len(servicePoints) / 2)
    for i in range(numTasks):
        pp = servicePoints[i]
        dp = servicePoints[numTasks + i]
        tasks.append([pp.reward,
                      (pp._id, pp.earliestT, pp.latestT, pp.serviceT),
                      (dp._id, dp.earliestT, dp.latestT, dp.serviceT)])
    #
    #  (ori_locId, seq, dest_locID, timeBudget, prob)
    a1_routineRoutes = [(0, (11,), 3, 100, 0.9),
                        (1, (10,), 3, 100, 0.1)]
    a2_routineRoutes = [(0, (11,), 4, 100, 0.1),
                        (1, (10,), 4, 100, 0.9)]
    a3_routineRoutes = [(0, (11,), 2, 100, 0.9),
                        (1, (10,), 2, 100, 0.1)]
    for rrs in [a1_routineRoutes, a2_routineRoutes, a3_routineRoutes]:
        agents.append(rrs)
    #
    problem = [problemName,
               agents, tasks, travel_time]
    prmt = convert_prob2prmt(*problem)
    return prmt


def convert_prob2prmt(problemName, agents, tasks, travel_time):
    T = list(range(len(tasks)))
    w_i = []
    _N = {}
    alpha_i, beta_i, c_i = {}, {}, {}
    for i in T:
        reward, pp, dp = tasks[i]
        w_i.append(reward)
        #
        pp_locId, pp_earT, pp_lastT, pp_serT = pp
        alpha_i['p%d' % i] = pp_earT
        beta_i['p%d' % i] = pp_lastT
        c_i['p%d' % i] = pp_serT
        #
        dp_locId, dp_earT, dp_lastT, dp_serT = dp
        alpha_i['d%d' % i] = dp_earT
        beta_i['d%d' % i] = dp_lastT
        c_i['d%d' % i] = dp_serT
        #
        _N['p%d' % i] = pp_locId
        _N['d%d' % i] = dp_locId
    #
    K = list(range(len(agents)))
    R_k, _C_kr = [], {}
    _o_kr, _d_kr = {}, {}
    gamma_kr, u_kr = {}, {}
    for k, routineRoutes in enumerate(agents):
        R_k.append(list(range(len(routineRoutes))))
        for r, (ori_locId, seq, dest_locID, timeBudget, prob) in enumerate(routineRoutes):
            _o_kr[k, r] = ori_locId
            _d_kr[k, r] = dest_locID
            _C_kr[k, r] = [ori_locId]
            for s, locID in enumerate(seq):
                _C_kr[k, r].append(locID)
            _C_kr[k, r].append(dest_locID)
            gamma_kr[k, r] = prob
            u_kr[k, r] = timeBudget
    #
    t_ij = {}
    for _i in _N:
        for _j in _N:
            t_ij[_i, _j] = travel_time[_N[_i], _N[_j]]
    for k in K:
        for r in R_k[k]:
            krP, krM = 'o_%d_%d' % (k, r), 'd_%d_%d' % (k, r)
            t_ij[krP, krP] = travel_time[_o_kr[k, r], _o_kr[k, r]]
            t_ij[krM, krM] = travel_time[_d_kr[k, r], _d_kr[k, r]]
            t_ij[krP, krM] = travel_time[_o_kr[k, r], _d_kr[k, r]]
            t_ij[krM, krP] = travel_time[_d_kr[k, r], _o_kr[k, r]]
            #
            for s0, locID0 in enumerate(_C_kr[k, r][1:-1]):
                _s0 = 's%d_%d_%d' % (s0, k, r)
                t_ij[krP, _s0] = travel_time[_o_kr[k, r], locID0]
                t_ij[_s0, krP] = travel_time[locID0, _o_kr[k, r]]
                t_ij[krM, _s0] = travel_time[_d_kr[k, r], locID0]
                t_ij[_s0, krM] = travel_time[locID0, _d_kr[k, r]]
                for s1, locID1 in enumerate(_C_kr[k, r][1:-1]):
                    _s1 = 's%d_%d_%d' % (s1, k, r)
                    t_ij[_s0, _s1] = travel_time[locID0, locID1]
            #
            for i in _N:
                t_ij[krP, i] = travel_time[_o_kr[k, r], _N[i]]
                t_ij[i, krP] = travel_time[_N[i], _o_kr[k, r]]
                t_ij[krM, i] = travel_time[_d_kr[k, r], _N[i]]
                t_ij[i, krM] = travel_time[_N[i], _d_kr[k, r]]
                for s, locID in enumerate(_C_kr[k, r][1:-1]):
                    _s = 's%d_%d_%d' % (s, k, r)
                    t_ij[i, _s] = travel_time[_N[i], locID]
                    t_ij[_s, i] = travel_time[locID, _N[i]]
                    c_i[_s] = 0.0
            c_i[krP] = 0.0
            c_i[krM] = 0.0
    #
    p_krij, C_kr, N_kr = {}, {}, {}
    for k in K:
        for r in R_k[k]:
            krP, krM = 'o_%d_%d' % (k, r), 'd_%d_%d' % (k, r)
            C_kr[k, r] = [krP]
            for s0 in range(len(_C_kr[k, r][1:-1])):
                _s0 = 's%d_%d_%d' % (s0, k, r)
                C_kr[k, r].append(_s0)
                p_krij[k, r, krP, krP] = 1
                p_krij[k, r, krM, krM] = 1
                p_krij[k, r, krP, krM] = 1
                p_krij[k, r, krM, krP] = 0
                #
                p_krij[k, r, krP, _s0] = 1
                p_krij[k, r, _s0, krP] = 0
                p_krij[k, r, krM, _s0] = 0
                p_krij[k, r, _s0, krM] = 1
                for s1 in range(s0, len(_C_kr[k, r][1:-1])):
                    _s1 = 's%d_%d_%d' % (s1, k, r)
                    p_krij[k, r, _s0, _s1] = 1
                    p_krij[k, r, _s1, _s0] = 0
            C_kr[k, r].append(krM)
            N_kr[k, r] = C_kr[k, r] + list(set(_N.keys()))
    #
    N = list(set(_N.keys()))
    #
    return {'problemName': problemName,
            'T': T,
                'w_i': w_i,
            'N': N,
                'alpha_i': alpha_i, 'beta_i': beta_i, 'c_i': c_i,
            'K': K,
                'R_k': R_k,
            'C_kr': C_kr, 'N_kr': N_kr, 'gamma_kr': gamma_kr, 'u_kr': u_kr,
            't_ij': t_ij, 'p_krij': p_krij
            }


def prmt_pkl2json(prmt, dpath='_temp'):
    for prmtName in ['C_kr', 'N_kr', 'gamma_kr', 'u_kr']:
        converted = {}
        for (k, r), v in prmt[prmtName].items():
            converted['%d&%d' % (k, r)] = v
        prmt[prmtName] = converted
    converted = {}
    for (i, j), v in prmt['t_ij'].items():
        converted['%s&%s' % (i, j)] = v
    prmt['t_ij'] = converted
    converted = {}
    for (k, r, i, j), v in prmt['p_krij'].items():
        converted['%d&%d&%s&%s' % (k,r, i, j)] = v
    prmt['p_krij'] = converted

    with open(opath.join(dpath, 'prmt_%s.json' % prmt['problemName']), 'w') as outfile:
        outfile.write(json.dumps(prmt))
        # json.dump(y, outfile)


if __name__ == '__main__':
    # print(convert_prob2prmt(*ex0()))

    prmt_pkl2json(euclideanDistEx0(dpath='_temp'))


