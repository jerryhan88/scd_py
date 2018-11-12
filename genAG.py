import os.path as opath
import os
import csv, pickle
import numpy as np
import pandas as pd
from random import randrange, random, seed
from datetime import datetime, timedelta
from geopy.distance import vincenty
#
from __path_organizer import pf_dpath, exp_dpath
from sgMRT import get_coordMRT
from sgBus import get_coordBS

NUM_GROUP = 10
MISSING_BS = {
        '42041': (1.334463, 103.786861),
        '48129': (1.407440, 103.783049),
        '52051': (1.343413, 103.854698),
        '52059': (1.343407, 103.854722),
        '31091': (1.384365, 103.698154),
        '46461': (1.447967, 103.797617),
        '62141': (1.351408, 103.875079),
        '96209': (1.340749, 103.959087),
        '14219': (1.282217, 103.806782),
        '41031': (1.323080, 103.811908),
        '70069': (1.338482, 103.875898),
        '23241': (1.310248, 103.679869),
        '70061': (1.338622, 103.875324),
        '42059': (1.336809, 103.781731),
        '55101': (1.390924, 103.859722),
        '31151': (1.373666, 103.695917),
        '28009': (1.333507, 103.741530),
        '61051': (1.339445, 103.871988),
        '42019': (1.331690, 103.796593),
        '48121': (1.407561, 103.783087),
        '96201': (1.340792, 103.959036),
        '28061': (1.335986, 103.742482),
        '52361': (1.339725, 103.852922),
        '61059': (1.339579, 103.871999),
        '42049': (1.335792, 103.784664),
        '71131': (1.331227, 103.901439),
        '10589': (1.278236, 103.837497),
        '46481': (1.449596, 103.800991),
        '48131': (1.404750, 103.789638),
        '46471': (1.447959, 103.797535),
        }

MODI_NAME = {'Harbour Front': 'HarbourFront',
             'South VIEW': 'South View',
             'Ten Mile Junctio': 'Ten Mile Junction',
             }

TIME_BUFFER = 30 * 60  # 30 min.
MIN_DIST_TRAJ = 5  # km
MAX_NUM_RR = 5  # the maximum number of routine routes
SELECTION_PROB = 0.5

RR_DPATH = opath.join(pf_dpath, 'RoutineRoutes')


class Node(object):
    def __init__(self, cid, numDates, seqCounter, 
                 sTime, eTime, 
                 traj):
        self.cid, self.numDates, self.seqCounter = cid, numDates, seqCounter
        self.sTime, self.eTime = sTime, eTime
        self.traj = traj
        self.ancestors, self.descendants = set(), set()
        self.parents, self.children = set(), set()
        self.movements = []
        
    def __repr__(self):
        return '%s*%s-%s' % (self.cid, self.sTime.strftime('%H:%M:%S'), self.eTime.strftime('%H:%M:%S'))

    def remove_descendant(self, n1):
        self.descendants.remove(n1)
        for n0 in self.ancestors:
            if n1 in n0.descendants:
                n0.remove_descendant(n1)


def EZ2RR():
    dpath = opath.join(pf_dpath, 'RoutineRoutes')
    if not opath.exists(dpath):
        os.mkdir(dpath)
    for gid in range(NUM_GROUP):
        ofpath = opath.join(dpath, 'RoutineRoutes-g%d.csv' % gid)
        with open(ofpath, 'w') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            writer.writerow(['cid', 'numDates', 'prob', 'movements'])
    #
    def handle_movements(_nodes, agent_wid):
        nodes = sorted(_nodes, key=lambda n: n.sTime)
        for i in range(len(nodes)):
            n0 = nodes[i]
            for j in range(i + 1, len(nodes)):
                n1 = nodes[j]
                if n0.eTime + timedelta(seconds=TIME_BUFFER) < n1.sTime:
                    n0.descendants.add(n1)
                    n1.ancestors.add(n0)
        root_nodes = []
        while nodes:
            n1 = nodes.pop()
            if n1.descendants:
                nodes.append(n1)
                continue
            for n0 in n1.ancestors:
                if len(n0.descendants.intersection(n1.ancestors)) == 0:
                    n1.parents.add(n0)
                    n0.children.add(n1)
                    n0.remove_descendant(n1)
            if not n1.ancestors:
                root_nodes.append(n1)
        for rn in root_nodes:
            for n in _nodes:
                n.movements = []
            node_stack = [rn]
            while node_stack:
                n0 = node_stack.pop()
                n0.movements.append(n0)
                for n1 in n0.children:
                    n1.movements = n0.movements[:]
                    if n1.children:
                        node_stack.append(n1)
                    else:
                        n1.movements.append(n1)
                        prob = 1.0
                        _movements = []
                        for n in n1.movements:
                            prob *= n.seqCounter / float(n.numDates)
                            _movements.append('%s-%s@%s' % (n.sTime.strftime('%H:%M:%S'),
                                                            n.eTime.strftime('%H:%M:%S'),
                                                            '|'.join(n.traj)))
                        prob = 1.0 if prob > 1.0 else prob
                        ofpath = opath.join(dpath, 'RoutineRoutes-g%d.csv' % agent_wid[n.cid])
                        with open(ofpath, 'a') as w_csvfile:
                            writer = csv.writer(w_csvfile, lineterminator='\n')
                            writer.writerow([n.cid, n.numDates, prob, '^'.join(_movements)])

    def get_designatedName(mrt_name):
        if mrt_name in MODI_NAME:
            return MODI_NAME[mrt_name]
        else:
            return mrt_name
    #
    ez_fpath = opath.join(pf_dpath, 'EZlinkSummary.csv')
    bs_coord = get_coordBS()
    for k, v in MISSING_BS.items():
        bs_coord[k] = v
    mrt_coord = get_coordMRT()
    cid0 = None
    nodes, corrupted = [], False
    agent_wid = {}
    with open(ez_fpath) as r_csvfile:
        reader = csv.DictReader(r_csvfile)
        for row in reader:
            cid = row['cid']
            if cid not in agent_wid:
                agent_wid[cid] = randrange(NUM_GROUP)
            if cid0 is not None and cid0 != cid:
                handle_movements(nodes, agent_wid)
                nodes, corrupted = [], False
            seq = row['sequence']
            traj = []
            for seg in seq.split(';'):
                if 'one-north' in seg:
                    if seg.index('STN one-north') == 0:
                        loc0 = 'STN one-north'
                        loc1 = get_designatedName(seg[len('STN one-north-'):])
                    else:
                        loc0 = get_designatedName(seg[:-len('-STN one-north')])
                        loc1 = 'STN one-north'
                    for loc in [loc0, loc1]:
                        loc = get_designatedName(loc[len('STN '):])
                        try:
                            traj.append('%.16f#%.16f' % tuple(mrt_coord[loc]))
                        except:
                            corrupted = True
                else:
                    for loc in seg.split('-'):
                        if loc.startswith('STN '):
                            loc = get_designatedName(loc[len('STN '):])
                            try:
                                traj.append('%.16f#%.16f' % tuple(mrt_coord[loc]))
                            except:
                                corrupted = True
                        else:
                            try:
                                traj.append('%.16f#%.16f' % bs_coord[loc])
                            except:
                                corrupted = True
            if corrupted:
                cid0 = None
                nodes, corrupted = [], False
                continue
            #
            numDates, seqCounter = [int(row[cn]) for cn in ['numDates', 'seqCounter']]
            sTime, eTime = [datetime.strptime(row[cn], '%H:%M:%S') for cn in ['start_time', 'end_time']]
            nodes.append(Node(cid, numDates, seqCounter, sTime, eTime, traj))
            cid0 = cid
    handle_movements(nodes, agent_wid)


def init_csv(csv_fpath):
    with open(csv_fpath, 'w') as w_csvfile:
        writer = csv.writer(w_csvfile, lineterminator='\n')
        writer.writerow(['cid', 'aid', 'rrid', 'prob', 'sTime', 'eTime', 'movement'])


def get_cid_instances(ifpath):
    cid_instances = {}
    with open(ifpath) as r_csvfile:
        reader = csv.DictReader(r_csvfile)
        for row in reader:
            cid = row['cid']
            if cid not in cid_instances:
                cid_instances[cid] = []
            cid_instances[cid].append(row)
    cidOrders = []
    for cid, rows in cid_instances.items():
        if MAX_NUM_RR < len(rows):
            continue
        cidOrders.append([int(rows[0]['numDates']), sum([eval(row['prob']) for row in rows]), cid])
    cidOrders.sort(reverse=True)
    return [l[2] for l in cidOrders], cid_instances


def gen_agents(seedNum, prefix, gNum, numAgents, dpath=exp_dpath):
    seed(seedNum)
    csv_fpath = opath.join(dpath, 'AG_%s.csv' % prefix)
    init_csv(csv_fpath)
    rr_fpath = opath.join(RR_DPATH, 'RoutineRoutes-g%d.csv' % gNum)
    cids, cid_instances = get_cid_instances(rr_fpath)
    #
    agents = []
    for cid in cids:
        if SELECTION_PROB < random():
            continue
        agt = {'aid': len(agents),
               'cid': cid,
               'RRs': []}
        valid = True
        new_rows = []
        for j, row in enumerate(cid_instances[cid]):
            mvts = []
            for mvt in row['movements'].split('^'):
                _seTime, traj = mvt.split('@')
                sTime, eTime = [datetime.strptime(_time, '%H:%M:%S') for _time in _seTime.split('-')]
                if eTime < sTime:
                    valid = False
                    break
                traj = [np.array(tuple(map(eval, latlng.split('#')))) for latlng in traj.split('|')]
                if vincenty(traj[0], traj[-1]).km < MIN_DIST_TRAJ:
                    valid = False
                    break
                new_rows.append([cid, len(agents), j, row['prob'],
                                 sTime.strftime('%H:%M:%S'), eTime.strftime('%H:%M:%S'),
                                 traj])
                #
                mvts.append({'sTime': sTime, 'eTime': eTime,
                             'traj': traj})
            if not valid:
                break
            agt['RRs'].append({'prob': eval(row['prob']),
                               'mvts': mvts})
        if not valid:
            continue
        #
        agents.append(agt)
        with open(csv_fpath, 'a') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            for row in new_rows:
                writer.writerow(row)
        if len(agents) == numAgents:
            break
    #
    return agents

if __name__ == '__main__':
    # EZ2RR()
    #
    gNum, numAgents, seedNum = 0, 5, 0
    prefix = 'g%d-na%03d-sn%02d' % (gNum, numAgents, seedNum)
    gen_agents(seedNum, prefix, gNum, numAgents, '_temp')
