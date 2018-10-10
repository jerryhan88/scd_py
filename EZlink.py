import os.path as opath
import os
import csv
import pandas as pd
from datetime import datetime
from functools import reduce
import multiprocessing
#
from __path_organizer import pf_dpath

EZ_RAW_DATA_HOME = reduce(opath.join, [opath.expanduser("~"), '..', 'SMART', 'ezlink_2013_08'])
NUM_GROUP = 20


def process_raw_data():
    dpath = opath.join(pf_dpath, 'EZlink')
    if not opath.exists(dpath):
        os.mkdir(dpath)
    #
    header = ['JOURNEY_ID', 'CARD_ID', 'PASSENGERTYPE', 'TRAVEL_MODE',
                'BOARDING_STOP_STN', 'ALIGHTING_STOP_STN',
                'RIDE_START_DATE', 'RIDE_START_TIME', 'RIDE_DISTANCE', 'RIDE_TIME']
    for fn in os.listdir(EZ_RAW_DATA_HOME):
        if not fn.endswith('.csv'):
            continue
        ifpath = opath.join(EZ_RAW_DATA_HOME, fn)
        with open(ifpath) as r_csvfile:
            reader = csv.DictReader(r_csvfile)
            for row in reader:
                cid = row['CARD_ID']
                ofpath = opath.join(dpath, 'EZlink-%s.csv' % cid)
                if not opath.exists(ofpath):
                    with open(ofpath, 'w') as w_csvfile:
                        writer = csv.writer(w_csvfile, lineterminator='\n')
                        writer.writerow(header)
                with open(ofpath, 'a') as w_csvfile:
                    writer = csv.writer(w_csvfile, lineterminator='\n')
                    writer.writerow([row[cn] for cn in header])


def process_raw_data1():
    dpath = opath.join(pf_dpath, 'EZlink1')
    if not opath.exists(dpath):
        os.mkdir(dpath)
    #
    header = ['JOURNEY_ID', 'CARD_ID', 'PASSENGERTYPE', 'TRAVEL_MODE',
                'BOARDING_STOP_STN', 'ALIGHTING_STOP_STN',
                'RIDE_START_DATE', 'RIDE_START_TIME', 'RIDE_DISTANCE', 'RIDE_TIME']
    for fn in os.listdir(EZ_RAW_DATA_HOME):
        if not fn.endswith('.csv'):
            continue
        ifpath = opath.join(EZ_RAW_DATA_HOME, fn)
        with open(ifpath) as r_csvfile:
            reader = csv.DictReader(r_csvfile)
            for row in reader:
                cid = int(row['CARD_ID'])
                ofpath = opath.join(dpath, 'EZlink-g%d.csv' % (cid % NUM_GROUP))
                if not opath.exists(ofpath):
                    with open(ofpath, 'w') as w_csvfile:
                        writer = csv.writer(w_csvfile, lineterminator='\n')
                        writer.writerow(header)
                with open(ofpath, 'a') as w_csvfile:
                    writer = csv.writer(w_csvfile, lineterminator='\n')
                    writer.writerow([row[cn] for cn in header])


def sort_individual_transaction():
    dpath = opath.join(pf_dpath, 'EZlink')
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
    #
    numProcessors = multiprocessing.cpu_count()
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


if __name__ == '__main__':
    # process_raw_data()
    process_raw_data1()
    # sort_individual_transaction()