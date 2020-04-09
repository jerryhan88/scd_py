#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 16:42:11 2020

@author: ckhan
"""

import os.path as opath
import pickle
from functools import reduce
from bs4 import BeautifulSoup
import geopandas as gpd
from shapely.ops import cascaded_union
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
from shapely.ops import cascaded_union

#
from __path_organizer import ef_dpath, pf_dpath

sz_pfpath = opath.join(pf_dpath, 'sgGeo_subZones.pkl')
pa_pfpath = opath.join(pf_dpath, 'sgGeo_planningAreas.pkl')
re_pfpath = opath.join(pf_dpath, 'sgGeo_regions.pkl')
sg_pfpath = opath.join(pf_dpath, 'sgGeo_whole.pkl')


filtered_subzones = {
        'NORTH-EASTERN ISLANDS',
        'CHANGI BAY',
        'SEMAKAU', 
        'SUDONG',
        'JURONG ISLAND AND BUKOM',
        'SOUTHERN GROUP',
        'JURONG ISLAND AND BUKOM',
        'TUAS VIEW EXTENSION',}

def preprocessing():
    # sub zone
    sz_fpath = reduce(opath.join, [ef_dpath,
                                   'master-plan-2019-subzone-boundary-no-sea',
                                   'master-plan-2019-subzone-boundary-no-sea-geojson.geojson'])    
    subZones_gdf = gpd.read_file(sz_fpath)    
    #
    subZones = []
    for _, row in subZones_gdf.iterrows():
        soup = BeautifulSoup(row['Description'], 'html.parser')
        attributes = {}
        for ele in soup.find('table').find_all('tr'):
            if ele.find('td') is not None:
                attributes[ele.find('th').text] = ele.find('td').text
        sz = {k: attributes[k] for k in ['SUBZONE_N', 'PLN_AREA_N', 'REGION_N']}
        sz['geometry'] = row['geometry']
        if sz['SUBZONE_N'] in filtered_subzones:
            continue        
        subZones.append(sz)
    #   
    _planningAreas, planningAreas = {}, []
    for sz in subZones:
        if sz['PLN_AREA_N'] not in _planningAreas:
            _planningAreas[sz['PLN_AREA_N']] = []
        _planningAreas[sz['PLN_AREA_N']].append(sz)
    for pa_name, subzones in _planningAreas.items():
        polygons = []
        for sz in subzones:
            if type(sz['geometry']) == MultiPolygon:
                for poly in sz['geometry']:
                    polygons.append(poly)
            elif type(sz['geometry']) == Polygon:
                polygons.append(sz['geometry'])
            else:
                assert False
        geometry = cascaded_union(polygons)
        cenSubZone = None
        for sz in subzones:
            if cenSubZone is not None:
                break
            if type(sz['geometry']) == MultiPolygon:
                for poly in sz['geometry']:
                    if geometry.centroid.within(poly):
                        cenSubZone = sz
                        break
            elif type(sz['geometry']) == Polygon:
                if geometry.centroid.within(sz['geometry']):
                    cenSubZone = sz
        
        planningAreas.append({'name': pa_name,
                              'geometry': geometry,
                              'centroid': geometry.centroid,
                              'cenSubZone': cenSubZone['SUBZONE_N']})
    #
    _regions, regions = {}, []
    for sz in subZones:
        if sz['REGION_N'] not in _regions:
            _regions[sz['REGION_N']] = []
        _regions[sz['REGION_N']].append(sz)
    for r_name, subzones in _regions.items():
        polygons = []
        for sz in subzones:
            if type(sz['geometry']) == MultiPolygon:
                for poly in sz['geometry']:
                    polygons.append(poly)
            elif type(sz['geometry']) == Polygon:
                polygons.append(sz['geometry'])
            else:
                assert False
        geometry = cascaded_union(polygons)
        cenSubZone = None
        for sz in subzones:
            if cenSubZone is not None:
                break
            if type(sz['geometry']) == MultiPolygon:
                for poly in sz['geometry']:
                    if geometry.centroid.within(poly):
                        cenSubZone = sz
                        break
            elif type(sz['geometry']) == Polygon:
                if geometry.centroid.within(sz['geometry']):
                    cenSubZone = sz
        regions.append({'name': r_name,
                          'geometry': geometry,
                          'centroid': geometry.centroid,
                          'cenSubZone': cenSubZone['SUBZONE_N']})
    #
    with open(sz_pfpath, 'wb') as fp:
        pickle.dump(subZones, fp)
    with open(pa_pfpath, 'wb') as fp:
        pickle.dump(planningAreas, fp)
    with open(re_pfpath, 'wb') as fp:
        pickle.dump(regions, fp)


def get_subZones():
    if not opath.exists(sz_pfpath):
        preprocessing()
    with open(sz_pfpath, 'rb') as fp:
        subZones = pickle.load(fp)
    return subZones


def get_planningAreas():
    if not opath.exists(pa_pfpath):
        preprocessing()
    with open(pa_pfpath, 'rb') as fp:
        planningAreas = pickle.load(fp)
    return planningAreas


def get_regions():
    if not opath.exists(re_pfpath):
        preprocessing()
    with open(re_pfpath, 'rb') as fp:
        regions = pickle.load(fp)
    return regions        


def get_sg():
    if not opath.exists(sg_pfpath):
        regions = get_regions()
        sg = cascaded_union([rgn['geometry'] for rgn in regions])
        with open(sg_pfpath, 'wb') as fp:
            pickle.dump(sg, fp)
    with open(sg_pfpath, 'rb') as fp:
        sg = pickle.load(fp)
    return sg

if __name__ == '__main__':
    regions = get_regions()
    planningAreas = get_planningAreas()
    subZones = get_subZones()
    
    
    len(regions)
    len(planningAreas)
    len(subZones)