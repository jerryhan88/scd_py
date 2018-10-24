import os.path as opath
import os
import sys
from functools import reduce

data_dpath = reduce(opath.join, ['..', '_data', 'scd_py'])
ef_dpath = opath.join(data_dpath, 'ExternalFiles')
pf_dpath = opath.join(data_dpath, 'ProcessedFiles')
mrtNet_dpath = opath.join(data_dpath, 'MRT_Network')

dir_paths = [data_dpath,
             ef_dpath, pf_dpath,
             ]

for dpath in dir_paths:
    if opath.exists(dpath):
        continue
    os.mkdir(dpath)

