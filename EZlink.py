import os.path as opath
import os
import csv
from functools import reduce
#
from __path_organizer import pf_dpath

EZ_RAW_DATA_HOME = reduce(opath.join, [opath.expanduser("~"), '..', 'SMART', 'ezlink_2013_08'])


def run():
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



if __name__ == '__main__':
    run()