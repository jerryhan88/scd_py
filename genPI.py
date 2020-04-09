import os.path as opath
import os
from random import random, uniform
from geopy.distance import geodesic
#
from sgGeo import get_regions, get_planningAreas
from genAG import gen_agents
from genTK import gen_tasks


MIN60, SEC60 = 60.0, 60.0
ARRIVAL_TIME_SPREAD = 30.0 / MIN60  # 30 min. (the unit is hour)
CAR_SPEED = 30.0  # km/hour
ZERO_DURATION = 0.0

DEFAULT_SERVICE_TIME = 5.0 / MIN60  # 5 min. (the unit is hour)
HOUR24 = 24.0
DEFAULT_TW = 0.0, HOUR24
TW_NOON_BEFORE = (0.0, HOUR24 / 2)
TW_NOON_AFTER = (HOUR24 / 2, HOUR24)

DEFAULT_VOLUME, DEFAULT_WEIGHT = 1.0, 1.0
# DEFAULT_VOLUME_CAPACITY, DEFAULT_WEIGHT_CAPACITY = 5.0, 5.0
DEFAULT_VOLUME_CAPACITY, DEFAULT_WEIGHT_CAPACITY = 10.0, 10.0
EPSILON = 1e-6
MAX_TRAVEL_TIME = 1e3

convert_t2h = lambda dt: dt.hour + dt.minute / MIN60 + dt.second / (MIN60 * SEC60)


def get_latlng(point):
    latlng = list(tuple(zip(*point.xy))[0])
    latlng.reverse()
    return latlng


def gen_problemInstance(problemName, agents, tasks,
                        tb_ratio=1.05, volume_capacity=DEFAULT_VOLUME_CAPACITY, weight_capacity=DEFAULT_WEIGHT_CAPACITY):
    regions = {ele['name']: ele for ele in get_regions()}
    planningAreas = {ele['name']: ele for ele in get_planningAreas()}
    
    pickups, deliveries = [], []    
    region_pid = {}
    for tk in tasks:
        if tk['Region'] not in region_pid:
            region_pid[tk['Region']] = len(region_pid)
            pickups.append({'pid': region_pid[tk['Region']],
                            'Region': tk['Region'],
                            'centroid': regions[tk['Region']]['centroid'],
                            'TW': DEFAULT_TW,
                            'ST': DEFAULT_SERVICE_TIME})    
        pid = region_pid[tk['Region']]
        v = uniform(EPSILON, DEFAULT_VOLUME)
        w = uniform(EPSILON, DEFAULT_WEIGHT)
        r = max(v, w)
        d = {'did': tk['tid'], 'pid': pid, 
             'reward': r, 'volume': v, 'weight': w}
        for k in ['Region', 'PlanningArea', 'SubZone', 'Lat', 'Lng']:
            d[k] = tk[k]
        d['TW'] = TW_NOON_BEFORE if random() > 0.5 else TW_NOON_AFTER
        d['ST'] = DEFAULT_SERVICE_TIME
        deliveries.append(d)
    #        
    nodeCounter = 0
    TW_locID, ST_locID = {}, {}
    for p in pickups:
        locID = nodeCounter
        p['locID'] = locID
        TW_locID[locID] = p['TW']
        ST_locID[locID] = p['ST']
        nodeCounter += 1
    for d in deliveries:
        locID = nodeCounter
        d['locID'] = locID
        TW_locID[locID] = d['TW']
        ST_locID[locID] = d['ST']
        nodeCounter += 1   
    for agt in agents:
        probSum = sum([aRR['prob'] for aRR in agt['RRs']])
        for aRR in agt['RRs']:
            mvts = aRR['mvts']
            first_mvt, last_mvt = mvts[0], mvts[-1]
            o_rg, d_rg = first_mvt['sPlanningArea'], last_mvt['ePlanningArea']
            f_embark_time = convert_t2h(first_mvt['sTime'])
            l_alight = convert_t2h(last_mvt['eTime'])
            #
            paTran= [o_rg]
            TWs = [([f_embark_time - ARRIVAL_TIME_SPREAD,
                      f_embark_time + ARRIVAL_TIME_SPREAD])]
            STs = [ZERO_DURATION]
            for i in range(len(mvts) - 1):
                p_mvts, n_mvts = mvts[i], mvts[i + 1]
                p_et, n_st, n_et = map(convert_t2h, [p_mvts['eTime'], n_mvts['sTime'], n_mvts['eTime']])
                #
                paTran.append(p_mvts['sPlanningArea'])
                TWs.append(([p_et - ARRIVAL_TIME_SPREAD,
                              p_et + ARRIVAL_TIME_SPREAD]))
                STs.append(n_st - p_et)
            paTran.append(d_rg)
            TWs.append([l_alight - ARRIVAL_TIME_SPREAD,
                         l_alight + ARRIVAL_TIME_SPREAD])
            STs.append(ZERO_DURATION)
            aRR['paTran'] = paTran
            aRR['TWs'] = TWs
            aRR['STs'] = STs
            aRR['locIDs'] = []
            distSum = 0.0
            for i in range(len(aRR['paTran'])):
                if i != 0:
                    pa0, pa1 = [planningAreas[aRR['paTran'][x]] for x in [i - 1, i]]
                    LatLng0 = get_latlng(pa0['centroid'])
                    LatLng1 = get_latlng(pa1['centroid'])
                    distSum += geodesic(LatLng0, LatLng1).km
                locID = nodeCounter
                aRR['locIDs'].append(locID)
                TW_locID[locID] = aRR['TWs'][i]
                ST_locID[locID] = aRR['STs'][i]
                nodeCounter += 1        
            ST_sum = sum(aRR['STs'])
            aRR['TL'] = (distSum / CAR_SPEED + ST_sum) * tb_ratio
            aRR['prob'] = aRR['prob'] / probSum            
        agt['volumeCap'] = volume_capacity
        agt['weightCap'] = weight_capacity
    #
    travel_time = {}
    for p0 in pickups:
        locID0 = p0['locID']
        LatLng0 = get_latlng(p0['centroid'])
        for p1 in pickups:
            locID1 = p1['locID']
            if locID0 == locID1:
                travel_time[locID0, locID1] = 0.0
            else:
                LatLng1 = get_latlng(p1['centroid'])
                travel_time[locID0, locID1] = geodesic(LatLng0, LatLng1).km / CAR_SPEED
        for d in deliveries:
            locID1 = d['locID']
            pa = planningAreas[d['PlanningArea']]
            LatLng1 = get_latlng(pa['centroid'])
            LatLng2 = [d['Lat'], d['Lng']]
            #
            dist = geodesic(LatLng0, LatLng1).km
            dist += geodesic(LatLng1, LatLng2).km
            travel_time[locID0, locID1] = dist / CAR_SPEED
        for agt in agents:
            for aRR in agt['RRs']:
                for i in range(len(aRR['locIDs'])):
                    locID1 = aRR['locIDs'][i]
                    pa = planningAreas[aRR['paTran'][i]]
                    #
                    LatLng1 = get_latlng(pa['centroid'])
                    travel_time[locID0, locID1] = geodesic(LatLng0, LatLng1).km / CAR_SPEED
    #
    for d0 in deliveries:
        locID0 = d0['locID']
        pa0 = planningAreas[d0['PlanningArea']]
        #
        LatLng0 = [d0['Lat'], d0['Lng']]
        LatLng1 = get_latlng(pa0['centroid'])
        for p in pickups:
            locID1 = p['locID']
            LatLng2 = get_latlng(p['centroid'])
            #
            dist = geodesic(LatLng0, LatLng1).km
            dist += geodesic(LatLng1, LatLng2).km
            travel_time[locID0, locID1] = dist / CAR_SPEED
        for d1 in deliveries:
            locID1 = d1['locID']
            if locID0 == locID1:
                travel_time[locID0, locID1] = 0.0
            else:
                pa1 = planningAreas[d1['PlanningArea']]
                if pa0 == pa1:
                    LatLng2 = [d1['Lat'], d1['Lng']]
                    #
                    travel_time[locID0, locID1] = geodesic(LatLng0, LatLng2).km / CAR_SPEED
                else:
                    LatLng2 = get_latlng(pa1['centroid'])
                    LatLng3 = [d1['Lat'], d1['Lng']]
                    #
                    dist = geodesic(LatLng0, LatLng1).km
                    dist += geodesic(LatLng1, LatLng2).km
                    dist += geodesic(LatLng2, LatLng3).km
                    travel_time[locID0, locID1] = dist / CAR_SPEED
        for agt in agents:
            for aRR in agt['RRs']:
                for i in range(len(aRR['locIDs'])):
                    locID1 = aRR['locIDs'][i]           
                    pa = planningAreas[aRR['paTran'][i]]
                    LatLng2 = get_latlng(pa['centroid'])                
                    #
                    dist = geodesic(LatLng0, LatLng1).km
                    dist += geodesic(LatLng1, LatLng2).km                
                    travel_time[locID0, locID1] = dist / CAR_SPEED
    #
    for agt in agents:
        for aRR in agt['RRs']:
            for i0 in range(len(aRR['locIDs'])):
                locID0 = aRR['locIDs'][i0]           
                pa0 = planningAreas[aRR['paTran'][i0]]
                LatLng0 = get_latlng(pa0['centroid'])
                for p in pickups:
                    locID1 = p['locID']
                    LatLng1 = get_latlng(p['centroid'])
                    #
                    travel_time[locID0, locID1] = geodesic(LatLng0, LatLng1).km / CAR_SPEED
                for d in deliveries:
                    locID1 = d['locID']
                    pa1 = planningAreas[d['PlanningArea']]
                    if pa0 == pa1:
                        LatLng1 = [d['Lat'], d['Lng']]
                        #
                        travel_time[locID0, locID1] = geodesic(LatLng0, LatLng1).km / CAR_SPEED
                    else:
                        LatLng1 = get_latlng(pa1['centroid'])
                        LatLng2 = [d['Lat'], d['Lng']]
                        #
                        dist = geodesic(LatLng0, LatLng1).km
                        dist += geodesic(LatLng1, LatLng2).km                
                        travel_time[locID0, locID1] = dist / CAR_SPEED
                for i1 in range(len(aRR['locIDs'])):
                    locID1 = aRR['locIDs'][i1]           
                    pa1 = planningAreas[aRR['paTran'][i1]]
                    LatLng1 = get_latlng(pa1['centroid'])
                    #
                    travel_time[locID0, locID1] = geodesic(LatLng0, LatLng1).km / CAR_SPEED
    
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
    numNodes = nodeCounter
    cN = list(range(numNodes))
    t_ij = [[MAX_TRAVEL_TIME for _ in range(numNodes)] for _ in range(numNodes)]
    al_i = [None for _ in range(numNodes)]
    be_i = [None for _ in range(numNodes)]
    c_aeij = [
                [
                    [
                        [0 for _ in range(numNodes)]
                        for _ in range(numNodes)]
                    for _ in range(len(E_a[a]))]
                for a in range(numAgents)
            ]
    #
    # Convert the problem into the notations of the mathematical programming
    #
    for k in K:
        d = deliveries[k]
        v_k[k] = d['volume']
        w_k[k] = d['weight']
        r_k[k] = d['reward']
        #
        p = pickups[d['pid']]
        assert p['pid'] == d['pid']
        h_k[k] = p['locID']
        n_k[k] = d['locID']
    #
    for a in A:
        ag = agents[a]
        v_a[a] = ag['volumeCap']
        w_a[a] = ag['weightCap']
        for e, rr in enumerate(ag['RRs']):
            p_ae[a][e] = rr['prob']
            u_ae[a][e] = rr['TL']
            for locID in rr['locIDs']:
                R_ae[a][e].append(locID)
            o_ae[a][e] = R_ae[a][e][0]
            d_ae[a][e] = R_ae[a][e][-1]
            for i0 in range(len(R_ae[a][e])):
                locID0 = R_ae[a][e][i0]
                for i1 in range(i0, len(R_ae[a][e])):
                    locID1 = R_ae[a][e][i1]
                    c_aeij[a][e][locID0][locID1] = 1
                    c_aeij[a][e][locID1][locID0] = 0
            #
            for k in K:
                locPID, locDID = h_k[k], n_k[k]
                min_tt = 1e400
                # Insert both pickup and delivery nodes
                for i0 in range(1, len(R_ae[a][e])):
                    for i1 in range(i0, len(R_ae[a][e])):
                        seq_locIDs = R_ae[a][e][:]
                        seq_locIDs.insert(i0, locPID)
                        seq_locIDs.insert(i1 + 1, locDID)
                        # check_TW_violation
                        is_TW_feasible = True
                        n0_locID = seq_locIDs[0]
                        erest_deptTime = TW_locID[n0_locID][0] + ST_locID[n0_locID]
                        for n1_locID in seq_locIDs[1:]:
                            erest_arrvTime = erest_deptTime + travel_time[n0_locID, n1_locID]
                            if TW_locID[n1_locID][1] < erest_arrvTime:
                                is_TW_feasible = False
                                break
                            else:
                                erest_deptTime = max(erest_arrvTime, TW_locID[n1_locID][0]) + ST_locID[n1_locID]
                            n0_locID = n1_locID
                        if not is_TW_feasible:
                            continue
                        tt = sum(travel_time[seq_locIDs[i], seq_locIDs[i + 1]] + ST_locID[seq_locIDs[i]]
                                 for i in range(len(seq_locIDs) - 1))
                        if tt < min_tt:
                            min_tt = tt
                if min_tt <= u_ae[a][e]:
                    K_ae[a][e].append(k)
                    P_ae[a][e].append(locPID)
                    D_ae[a][e].append(locDID)
            #
            P_ae[a][e] = list(set(P_ae[a][e]))
            D_ae[a][e] = list(set(D_ae[a][e]))
            PD_ae[a][e] = P_ae[a][e][:] + D_ae[a][e][:]
            N_ae[a][e] = PD_ae[a][e][:] + R_ae[a][e][:]
    
    maxTravelTime, maxServiceTime = 0.0, 0.0
    for locID0 in cN:
        al_i[locID0], be_i[locID0] = TW_locID[locID0]
        for locID1 in cN:
            if locID0 == locID1:
                t_ij[locID0][locID1] = 0
            else:
                try:
                    t_ij[locID0][locID1] = travel_time[locID0, locID1] + ST_locID[locID0]
                    if maxTravelTime < travel_time[locID0, locID1]:
                        maxTravelTime = travel_time[locID0, locID1]
                except KeyError:
                    # locID0 and locID1 are associated with diffrent routine routes
                    continue
        if maxServiceTime < ST_locID[locID0]:
            maxServiceTime = ST_locID[locID0]
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
            'M': M}


def test():
    gNum, seedNum = 0, 0
    numAgents = 5
    numTasks = 10
    problemName = 'g%d-na%03d-sn%02d' % (gNum, numAgents, seedNum)
    agents = gen_agents(seedNum, problemName, gNum, numAgents, dpath='_temp')
    tasks = gen_tasks(seedNum, problemName, numTasks, agents, dpath='_temp')    
    tb_ratio = 1.05
    #
    gen_problemInstance(problemName, agents, tasks, tb_ratio)


if __name__ == '__main__':
    test()
