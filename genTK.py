import os.path as opath
import csv, pickle
import numpy as np
from shapely.geometry import Polygon, LineString
from random import seed, choice, sample
from itertools import chain
#
from __path_organizer import exp_dpath, ef_dpath
from sgDistrict import get_distPoly


get_crossingDist = lambda distPoly, line: [dist_name for dist_name, poly in distPoly.items() if poly.intersection(line)]

# def get_crossingDist(distPoly, line):
#     return [dist_name for dist_name, poly in distPoly.items() if poly.intersection(line)]

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
            distPD[dist].append([loc, lat, lng])
    #
    return distPoly, distPD


def init_csv(csv_fpath):
    with open(csv_fpath, 'w') as w_csvfile:
        writer = csv.writer(w_csvfile, lineterminator='\n')
        writer.writerow(['tid',
                         'LocP', 'LatP', 'LngP',
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
    tasks = []
    while len(tasks) < numTasks:
        chosen_traj = choice(candi_trajs)
        #
        candiPDs = set()
        firstPoint = chosen_traj[0]
        for dist_name in get_crossingDist(distPoly, LineString(chosen_traj)):
            try:
                for loc in distPD[dist_name]:
                    candiPDs.add(tuple(loc))
            except:
                pass
        if len(candiPDs) < 2:
            continue
        loc0, loc1 = sample(candiPDs, 2)
        assert loc0 != loc1
        loc0_name, loc0_lat, loc0_lng = loc0
        loc1_name, loc1_lat, loc1_lng = loc1
        loc0N = np.linalg.norm(firstPoint - np.array([loc0_lat, loc0_lng]))
        loc1N = np.linalg.norm(firstPoint - np.array([loc1_lat, loc1_lng]))
        if loc0N < loc1N:
            LocP, LatP, LngP = loc0_name, loc0_lat, loc0_lng
            LocD, LatD, LngD = loc1_name, loc1_lat, loc1_lng
        else:
            LocP, LatP, LngP = loc1_name, loc1_lat, loc1_lng
            LocD, LatD, LngD = loc0_name, loc0_lat, loc0_lng
        with open(csv_fpath, 'a') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            writer.writerow([len(tasks),
                             LocP, LatP, LngP,
                             LocD, LatD, LngD])
        tasks.append({'tid': len(tasks),
                      'LocP': LocP, 'LatP': LatP, 'LngP': LngP,
                      'LocD': LocD, 'LatD': LatD, 'LngD': LngD})
    #
    return tasks


if __name__ == '__main__':
    numTasks = 3
    agt_fpath = opath.join(exp_dpath, 'agent-g0-na005-sn00.pkl')
    _, _g, _na, _sn = opath.basename(agt_fpath)[:-len('.pkl')].split('-')
    prefix = 'task-%s-%s-%s-nt%03d' % (_g, _na, _sn, numTasks)
    seedNum = int(_sn[len('sn'):])
    gen_tasks(agt_fpath, seedNum, prefix, numTasks, exp_dpath)
