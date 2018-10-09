import os.path as opath
import os
from functools import reduce

import csv


EZ_RAW_DATA_HOME = reduce(opath.join, [opath.expanduser("~"), '..', 'SMART', 'ezlink_2013_08'])

for fn in os.listdir(EZ_RAW_DATA_HOME):
    if not fn.endswith('.csv'):
        continue
    fpath = opath.join(EZ_RAW_DATA_HOME, fn)

    with open(fpath) as r_csvfile:
        reader = csv.DictReader(r_csvfile)
        for row in reader:
            print([row[cn] for cn in ['JOURNEY_ID', 'CARD_ID', 'PASSENGERTYPE', 'TRAVEL_MODE',
                                'BOARDING_STOP_STN', 'ALIGHTING_STOP_STN',
                                'RIDE_START_DATE', 'RIDE_START_TIME', 'RIDE_DISTANCE', 'RIDE_TIME']])
            assert False

