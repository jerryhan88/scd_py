import os.path as opath
import os
import csv
import pandas as pd
from datetime import datetime, timedelta
from functools import reduce
import multiprocessing
#
from __path_organizer import pf_dpath

EZ_RAW_DATA_HOME = reduce(opath.join, [opath.expanduser("~"), '..', 'SMART', 'ezlink_2013_08'])
NUM_GROUP = 10
MON, TUE, WED, THR, FRI, SAT, SUN = range(7)
WEEKENDS = [SAT, SUN]


def process_raw_data():
    dpath = opath.join(pf_dpath, 'EZlink')
    if not opath.exists(dpath):
        os.mkdir(dpath)
    #
    def write_instances(wid, rows):
        ofpath = opath.join(dpath, 'EZlink-g%d.csv' % wid)
        with open(ofpath, 'a') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            for row in rows:
                writer.writerow([row[cn] for cn in header])
    #
    header = ['JOURNEY_ID', 'CARD_ID', 'PASSENGERTYPE', 'TRAVEL_MODE',
                'BOARDING_STOP_STN', 'ALIGHTING_STOP_STN',
                'RIDE_START_DATE', 'RIDE_START_TIME', 'RIDE_DISTANCE', 'RIDE_TIME']
    for gid in range(NUM_GROUP):
        ofpath = opath.join(dpath, 'EZlink-g%d.csv' % gid)
        with open(ofpath, 'w') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            writer.writerow(header)
    counter = 0
    worker_row = [[] for _ in range(NUM_GROUP)]
    agent_wid = {}
    for fn in os.listdir(EZ_RAW_DATA_HOME):
        if not fn.endswith('.csv'):
            continue
        ifpath = opath.join(EZ_RAW_DATA_HOME, fn)
        with open(ifpath) as r_csvfile:
            reader = csv.DictReader(r_csvfile)
            for row in reader:
                counter += 1
                cid = int(row['CARD_ID'])
                if cid not in agent_wid:
                    agent_wid[cid] = len(agent_wid) % NUM_GROUP
                worker_row[agent_wid[cid]].append(row)
                if counter == 50000:
                    ps = []
                    for wid, rows in enumerate(worker_row):
                        p = multiprocessing.Process(target=write_instances,
                                                    args=(wid, rows))
                        ps.append(p)
                        p.start()
                    for p in ps:
                        p.join()
                    del worker_row
                    worker_row = [[] for _ in range(NUM_GROUP)]
                    counter = 0
    ps = []
    for wid, rows in enumerate(worker_row):
        p = multiprocessing.Process(target=write_instances,
                                    args=(wid, rows))
        ps.append(p)
        p.start()
    for p in ps:
        p.join()


def sort_transactions():
    dpath = opath.join(pf_dpath, 'EZlink1')
    #
    def process_files(_, fns):
        for fn in fns:
            ifpath = opath.join(dpath, fn)
            df = pd.read_csv(ifpath)
            timestamp = []
            for _, row in df.iterrows():
                year, month, day = [int(v) for v in row['RIDE_START_DATE'].split('-')]
                hour, minute, second = [int(v) for v in row['RIDE_START_TIME'].split(':')]
                dt = datetime(year, month, day, hour, minute, second)
                timestamp.append(dt.timestamp())
            df['TIMESTAMP'] = pd.Series(timestamp, index=df.index)
            df = df.sort_values(by=['CARD_ID', 'TIMESTAMP'])
            df.to_csv(ifpath, index=False)

    numProcessors = 3
    worker_fns = [[] for _ in range(numProcessors)]
    for i, fn in enumerate(sorted([fn for fn in os.listdir(dpath) if fn.endswith('.csv')])):
        worker_fns[i % numProcessors].append(fn)
    ps = []
    for wid, fns in enumerate(worker_fns):
        p = multiprocessing.Process(target=process_files,
                                    args=(wid, fns))
        ps.append(p)
        p.start()
    for p in ps:
        p.join()


def arrange_transactions():
    STAYING_TIME_LIMIT = 30 * 60  # 30 minutes
    nd_dt = datetime(2013, 8, 9).date()  # National day
    header = ['cid', 'date',
              'start_time', 'end_time',
              'start_loc', 'end_loc',
              'sequence']
    dpath = opath.join(pf_dpath, 'EZlink1')

    def handling_day_seq(cid0, handling_dt, day_seq, ofpath):
        if handling_dt is not None and \
                (handling_dt.weekday() not in WEEKENDS and handling_dt != nd_dt):
            begin_dt, last_dt, sequence = None, None, []
            for start_dt, sLoc, eLoc, dur in day_seq:
                if begin_dt is None:
                    begin_dt = start_dt
                else:
                    if (start_dt - last_dt).seconds > STAYING_TIME_LIMIT:
                        with open(ofpath, 'a') as w_csvfile:
                            writer = csv.writer(w_csvfile, lineterminator='\n')
                            writer.writerow([cid0, '%s' % handling_dt,
                                             begin_dt.strftime('%H:%M:%S'), last_dt.strftime('%H:%M:%S'),
                                             sequence[0].split('-')[0], sequence[-1].split('-')[1],
                                             ';'.join(sequence)])
                        #
                        begin_dt, sequence = start_dt, []
                last_dt = start_dt + timedelta(minutes=dur)
                sequence.append('%s-%s' % (sLoc, eLoc))
            with open(ofpath, 'a') as w_csvfile:
                writer = csv.writer(w_csvfile, lineterminator='\n')
                writer.writerow([cid0, '%s' % handling_dt,
                                 begin_dt.strftime('%H:%M:%S'), last_dt.strftime('%H:%M:%S'),
                                 sequence[0].split('-')[0], sequence[-1].split('-')[1],
                                 ';'.join(sequence)])

    def process_files(_, fn):
        ofpath = opath.join(dpath, 'arr_%s' % fn)
        with open(ofpath, 'w') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            writer.writerow(header)
        #
        ifpath = opath.join(dpath, fn)
        cid0, handling_dt = None, None
        day_seq = []
        with open(ifpath) as r_csvfile:
            reader = csv.DictReader(r_csvfile)
            for row in reader:
                try:
                    eval(row['RIDE_TIME'])
                except:
                    continue
                cid = int(row['CARD_ID'])
                cur_dt = datetime.strptime('%s %s' % (row['RIDE_START_DATE'], row['RIDE_START_TIME']),
                                           "%Y-%m-%d %H:%M:%S")
                if cid0 != cid or handling_dt != cur_dt.date():
                    handling_day_seq(cid0, handling_dt, day_seq, ofpath)
                    day_seq = []
                cid0, handling_dt = cid, cur_dt.date()
                day_seq.append([cur_dt, row['BOARDING_STOP_STN'], row['ALIGHTING_STOP_STN'], eval(row['RIDE_TIME'])])
    #
    worker_fn = []
    for i, fn in enumerate(sorted([fn for fn in os.listdir(dpath) if fn.endswith('.csv') and not fn.startswith('arr')])):
        worker_fn.append(fn)
    ps = []
    for wid, fn in enumerate(worker_fn):
        p = multiprocessing.Process(target=process_files,
                                    args=(wid, fn))
        ps.append(p)
        p.start()
    for p in ps:
        p.join()


if __name__ == '__main__':
    # process_raw_data()
    # sort_transactions()
    arrange_transactions()