import os.path as opath
import csv, pickle
import numpy as np
from shapely.geometry import Polygon, LineString
from random import seed, choice, sample
from itertools import chain
#
from __path_organizer import exp_dpath, ef_dpath
from sgDistrict import get_distPoly, get_districtZone, zoneCentroid


get_crossingDist = lambda distPoly, line: [dist_name for dist_name, poly in distPoly.items() if poly.intersection(line)]


def load_baseData():
    _distPoly = get_distPoly()
    distPoly = {}
    for dist_name, poly in _distPoly.items():
        distPoly[dist_name] = Polygon(poly)
    #
    distPD = {}
    with open(opath.join(ef_dpath, 'LocationPD.csv')) as r_csvfile:
        reader = csv.DictReader(r_csvfile)
        for row in reader:
            loc, dist = [row[cn] for cn in ['Location', 'District']]
            lat, lng = [eval(row[cn]) for cn in ['Lat', 'Lng']]
            if dist not in distPD:
                distPD[dist] = []
            distPD[dist].append([loc, dist, lat, lng])
    #
    return distPoly, distPD


def init_csv(csv_fpath):
    with open(csv_fpath, 'w') as w_csvfile:
        writer = csv.writer(w_csvfile, lineterminator='\n')
        writer.writerow(['tid',
                         'LocW', 'LatW', 'LngW',
                         'LocD', 'LatD', 'LngD'])


def gen_tasks(seedNum, prefix, numTasks, agents, dpath=exp_dpath):
    seed(seedNum)
    csv_fpath = opath.join(dpath, 'TK_%s.csv' % prefix)
    init_csv(csv_fpath)
    distPoly, distPD = load_baseData()
    candi_trajs = []
    for agt in agents:
        for aRR in agt['RRs']:
            traj = [[np.array([lat, lng]) for lat, lng in mvt['traj']] for mvt in aRR['mvts']]
            candi_trajs.append(list(chain(*traj)))
    #
    districtZone = get_districtZone()
    tasks = []
    while len(tasks) < numTasks:
        chosen_traj = choice(candi_trajs)
        #
        candiPDs = set()
        for dist_name in get_crossingDist(distPoly, LineString(chosen_traj)):
            try:
                for loc in distPD[dist_name]:
                    candiPDs.add(tuple(loc))
            except:
                pass
        loc = choice(list(candiPDs))
        loc_name, loc_dn, loc_lat, loc_lng = loc
        zn = districtZone[loc_dn]
        zLat, zLng = zoneCentroid[zn]
        #
        instance = {'tid': len(tasks),
                    'LocW': zn, 'LatW': zLat, 'LngW': zLng,
                    'LocD': loc_name, 'LatD': loc_lat, 'LngD': loc_lng}
        with open(csv_fpath, 'a') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            writer.writerow([instance[attr] for attr in ['tid',
                                                         'LocW', 'LatW', 'LngW',
                                                         'LocD', 'LatD', 'LngD']])
        tasks.append(instance)
    #
    return tasks


if __name__ == '__main__':
    from genAG import gen_agents

    gNum, numAgents, seedNum = 0, 5, 0
    numTasks = 3
    prefix = 'g%d-na%03d-sn%02d' % (gNum, numAgents, seedNum)
    agents = gen_agents(seedNum, prefix, gNum, numAgents, dpath='_temp')
    tasks = gen_tasks(seedNum, prefix, numTasks, agents, dpath='_temp')

    with open(opath.join('_temp', '%s.pkl' % prefix), 'wb') as fp:
        pickle.dump([agents, tasks], fp)
