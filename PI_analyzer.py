#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr  1 13:40:27 2020

@author: ckhan
"""

import os.path as opath
import os
import csv
import json
import numpy as np
import pandas as pd
#
from __path_organizer import exp_dpath


def summary(json_dpath):
    pip_fpath = opath.join(json_dpath, '_PI_profile.csv')    
    with open(pip_fpath, 'w') as w_csvfile:
        writer = csv.writer(w_csvfile, lineterminator='\n')
        header = ['pn', 'na', 'nt', 'sn',
                'avg_rw', 'std_rw', 'min_rw', 'max_rw', 
                'avg_rr', 'std_rr', 'min_rr', 'max_rr', 
                'avg_rrp', 'std_rrp', 'min_rrp', 'max_rrp', 
                'avg_ftw', 'std_ftw', 'min_ftw', 'max_ftw',
                'avg_fta', 'std_fta', 'min_fta', 'max_fta']
        writer.writerow(header)
    for fn in os.listdir(json_dpath):
        if not fn.endswith('.json'):
            continue
        #
        fpath = opath.join(json_dpath, fn)
        with open(fpath) as f:
          data = json.load(f)    
        num_rr, num_rrp, num_ftw, num_fta = [], [], [], []
        for a in data['A']:   
            num_rr.append(len(data['E_a'][a]))
            ft = set()
            for e in data['E_a'][a]:
                num_rrp.append(len(data['R_ae'][a][e]))
                num_ftw.append(len(data['K_ae'][a][e]))
                for k in data['K_ae'][a][e]:
                    ft.add(k)
            num_fta.append(len(ft))
        num_rr, num_rrp, num_ftw, num_fta = map(np.array, [num_rr, num_rrp, num_ftw, num_fta])
        reward = np.array(data['r_k'])
        #
        pn = data['problemName']
        sn = int(pn.split('-')[-1][-len('sn'):])
        with open(pip_fpath, 'a') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            new_row = [pn, len(data['A']), len(data['K']), sn]
            for np_ary in [reward, num_rr, num_rrp, num_ftw, num_fta]:
                new_row += [np_ary.mean(), np_ary.std(), np_ary.min(), np_ary.max()]
            writer.writerow(new_row)
        
    df = pd.read_csv(pip_fpath)
    df = df.sort_values(by=['na', 'nt', 'sn'])
    df.to_csv(pip_fpath, index=False)    
            
if __name__ == '__main__':    
    problem_dpath = opath.join(exp_dpath, 'problem')
    json_dpath = opath.join(problem_dpath, 'json')
    summary(json_dpath)
