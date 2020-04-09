#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 07:43:22 2020

@author: ckhan
"""

import os.path as opath
import os
import csv
import matplotlib.pyplot as plt
import numpy as np
from functools import reduce

from __path_organizer import exp_dpath

FIGSIZE = (8, 6)


def draw_chart_subProbSolWallT(subProbSolWallT_fpath, img_ofpath):
    numIter_wallT = {}
    with open(subProbSolWallT_fpath) as r_csvfile:
        reader = csv.DictReader(r_csvfile)
        for row in reader:
            numIter, wallT = [eval(row[cn]) for cn in ['numIter', 'elapsedWallT']]
            if numIter not in numIter_wallT:
                numIter_wallT[numIter] = []
            numIter_wallT[numIter].append(wallT)
                
    data = [None for _ in range(len(numIter_wallT))]
    for k, v in numIter_wallT.items():
        data[k] = v
    labels = list(range(len(numIter_wallT)))
    
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ax.set_title('Wall-Clock Time for each iteration', fontsize=16)
    ax.boxplot(data, labels=labels)
    for label in (ax.get_xticklabels() + ax.get_yticklabels()):
        label.set_fontname('Arial')
        label.set_fontsize(12)
    plt.savefig(img_ofpath, bbox_inches='tight', pad_inches=0)


def draw_chart_solQual(solQual_fpath, img_ofpath):
    xs = []
    dualSols, primalSols = [], []    
    min_dual, max_primal = 1e400, -1e400
    gaps = []
    with open(solQual_fpath) as r_csvfile:
        reader = csv.DictReader(r_csvfile)
        for row in reader:
            numIter, dualSol, primalSol = [eval(row[cn]) for cn in ['numIter', 'dualSol', 'primalSol']]
            if dualSol < min_dual:
                min_dual = dualSol
            if max_primal < primalSol:
                max_primal = primalSol
            gaps.append((min_dual - max_primal) / (max_primal + 0.00001))
            dualSols.append(dualSol)
            primalSols.append(primalSol)
            xs.append(numIter)
    #
    ind = np.arange(len(xs))
    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, figsize=FIGSIZE, 
                     gridspec_kw={'height_ratios': [3, 1]})
    #
    ax1.set_title('Progress of solutions', fontsize=16)
    ax1.plot(ind, dualSols)
    ax1.plot(ind, primalSols)
    customized_xticks = [numIter if i % 5 == 0 else '' for i, numIter in enumerate(xs)]
    plt.xticks(ind, customized_xticks)
    ax1.legend(['DualSol', 'PrimalSol'], loc='upper right', fontsize=14)  
    #
    ax2.set_title('Gap', fontsize=16)
    ax2.plot(ind, gaps)
    ax2.set_ylim([0, 1])
    #
    ax1.xaxis.tick_top()
    ax1.tick_params(labeltop='off')  # don't put tick labels at the top
    ax2.xaxis.tick_bottom()
    plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    for label in (ax1.get_xticklabels() + ax1.get_yticklabels() 
                    + ax2.get_xticklabels() + ax2.get_yticklabels()):
        label.set_fontname('Arial')
        label.set_fontsize(12)
    plt.savefig(img_ofpath, bbox_inches='tight', pad_inches=0)
    
    
def handle_directory(dpath):
    fpaths_subProbSolWallT, fpaths_solQual = [], []
    for fn in os.listdir(dpath):
        if fn.endswith('subProbSolWallT.csv'):
            fpaths_subProbSolWallT.append(fn)
        elif fn.endswith('solQual.csv'):
            fpaths_solQual.append(fn)
    #
    chart_dpath_subProbSolWallT = opath.join(dpath, '_0chart_subProbSolWallT')
    if not opath.exists(chart_dpath_subProbSolWallT):
        os.mkdir(chart_dpath_subProbSolWallT)
    for fn in fpaths_subProbSolWallT:
        img_ofpath = opath.join(chart_dpath_subProbSolWallT, '%s.png' % fn[:-len('.csv')])
        if opath.exists(img_ofpath):
            continue
        draw_chart_subProbSolWallT(opath.join(dpath, fn), img_ofpath)
    #
    chart_dpath_solQual = opath.join(dpath, '_0chart_solQual')
    if not opath.exists(chart_dpath_solQual):
        os.mkdir(chart_dpath_solQual)
    for fn in fpaths_solQual:
        img_ofpath = opath.join(chart_dpath_solQual, '%s.png' % fn[:-len('.csv')])
        if opath.exists(img_ofpath):
            continue
        draw_chart_solQual(opath.join(dpath, fn), img_ofpath)        
            
  
if __name__ == '__main__':        
    dpath = reduce(opath.join, [exp_dpath, 'LRH-ILP'])
    handle_directory(dpath)
