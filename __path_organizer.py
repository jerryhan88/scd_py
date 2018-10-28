import os.path as opath
import os
import sys
from functools import reduce

data_dpath = reduce(opath.join, ['..', '_data', 'scd_py'])
mrtNet_dpath = opath.join(data_dpath, 'MRT_Network')
ef_dpath = opath.join(data_dpath, 'ExternalFiles')
pf_dpath = opath.join(data_dpath, 'ProcessedFiles')
exp_dpath = opath.join(data_dpath, 'Experiments')


dir_paths = [data_dpath,
             ef_dpath, pf_dpath,
             exp_dpath,
             ]

for dpath in dir_paths:
    if opath.exists(dpath):
        continue
    os.mkdir(dpath)

