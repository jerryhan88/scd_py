import os.path as opath
import csv, pickle
import numpy as np
from shapely.geometry import LineString
from shapely.geometry import Point
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from random import seed, choice, sample
from itertools import chain
#
from __path_organizer import exp_dpath, ef_dpath
from sgDistrict import get_distPoly, get_districtZone, zoneCentroid
from sgGeo import get_subZones, get_regions

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


def gen_tasks0(seedNum, prefix, numTasks, agents, dpath=exp_dpath):
    seed(seedNum)
    csv_fpath = opath.join(dpath, 'TK_%s.csv' % prefix)
    with open(csv_fpath, 'w') as w_csvfile:
        writer = csv.writer(w_csvfile, lineterminator='\n')
        writer.writerow(['tid',
                         'LocW', 'LatW', 'LngW',
                         'LocD', 'LatD', 'LngD'])
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
        crossingDists = []
        line = LineString(chosen_traj)
        for dist_name, poly in distPoly.items():
            try:
                if poly.intersection(line):
                    crossingDists.append(dist_name)
            except:
                if poly.buffer(0).intersection(line):
                    crossingDists.append(dist_name)
        candiPDs = set()
        for dist_name in crossingDists:
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


def get_wholePD():
    wholePD = []
    subzones = get_subZones()
    with open(opath.join(ef_dpath, 'LocationPD.csv')) as r_csvfile:
        reader = csv.DictReader(r_csvfile)
        for row in reader:
            loc, dist = [row[cn] for cn in ['Location', 'District']]
            lat, lng = [eval(row[cn]) for cn in ['Lat', 'Lng']]
            point = Point(lng, lat)
            dSubzone, dPlanningArea, dRegion = None, None, None
            for sz in subzones:
                if type(sz['geometry']) == Polygon:
                    if point.within(sz['geometry']):
                        dSubzone = sz['SUBZONE_N']
                        dRegion = sz['REGION_N']
                        dPlanningArea = sz['PLN_AREA_N']
                        break
                else:
                    assert type(sz['geometry']) == MultiPolygon
                    for poly in sz['geometry']:
                        if point.within(poly):
                            dSubzone = sz['SUBZONE_N']
                            dRegion = sz['REGION_N']
                            dPlanningArea = sz['PLN_AREA_N']
            wholePD.append({'Location': loc,
                            'Lat': lat, 'Lng': lng,
                            'Subzone': dSubzone, 'PlanningArea': dPlanningArea, 'Region': dRegion})
    return wholePD
    

def gen_tasks(seedNum, prefix, numTasks, agents, dpath=exp_dpath):
    pkl_fpath = opath.join(dpath, 'TK_%s.pkl' % prefix)
    if opath.exists(pkl_fpath):
        with open(pkl_fpath, 'rb') as fp:
            tasks = pickle.load(fp)
        return tasks
    #
    seed(seedNum)
    csv_fpath = opath.join(dpath, 'TK_%s.csv' % prefix)
    with open(csv_fpath, 'w') as w_csvfile:
        writer = csv.writer(w_csvfile, lineterminator='\n')
        writer.writerow(['tid',
                         'Region', 'PlanningArea', 'SubZone',
                         'Lat', 'Lng',
                         'Note'])
    #
    wholePD = get_wholePD()
    candi_pas = set()
    for agt in agents:
        for rr in agt['RRs']:
            for mvt in rr['mvts']:
                candi_pas.add(mvt['sPlanningArea'])
                candi_pas.add(mvt['ePlanningArea'])
    cadi_tasks = [t for t in wholePD if t['PlanningArea'] in candi_pas]
    #
    tasks = []
    while len(tasks) < numTasks:
        t = choice(cadi_tasks)
        loc_name, loc_lat, loc_lng = [t[k] for k in ['Location', 'Lat', 'Lng']]
        sz_name = t['Subzone']
        pa_name = t['PlanningArea']
        rg_name = t['Region']
        #
        instance = {'tid': len(tasks),
                    'Region': rg_name, 'PlanningArea': pa_name, 'SubZone': sz_name,
                    'Lat': loc_lat, 'Lng': loc_lng, 'Note': loc_name}
        with open(csv_fpath, 'a') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            writer.writerow([instance[attr] for attr in ['tid',
                                                         'Region', 'PlanningArea', 'SubZone',
                                                         'Lat', 'Lng',
                                                         'Note']])
        tasks.append(instance)
    #
    with open(pkl_fpath, 'wb') as fp:
        pickle.dump(tasks, fp)
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
