import os.path as opath
import pickle, csv
import json
import numpy as np
from xlrd import open_workbook
from pykml import parser
from shapely.ops import cascaded_union
from shapely.geometry import Polygon

#
from __path_organizer import ef_dpath, pf_dpath


xConsiderDist = [   'North-Eastern Islands',
                    'Tuas View Extension',
                    'Jurong Island And Bukom',
                    'Southern Group',
                    'Semakau',
                    'Sudong',
                    'Pulau Seletar',
                 ]


def get_districtPopPoly():
    csv_fpath = opath.join(pf_dpath, 'DistrictsPopulation.csv')
    pop_fpath_PKL = opath.join(pf_dpath, 'DistrictsPopulation.pkl')
    poly_fpath_PKL = opath.join(pf_dpath, 'DistrictsPolygon.pkl')
    if not opath.exists(csv_fpath):
        with open(csv_fpath, 'w') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            new_headers = ['Name', 'Population']
            writer.writerow(new_headers)
        #
        distPop = {}
        xls_fpath = opath.join(ef_dpath, 'ResidentPopulation2015.xls')
        book = open_workbook(xls_fpath)
        sh = book.sheet_by_name('T7(Total)')
        for i in range(sh.nrows):
            district_name = sh.cell(i, 2).value
            if district_name in ['Subzone', 'Total', '']:
                continue
            population = int(sh.cell(i, 3).value) if sh.cell(i, 3).value != '-' else 0
            if district_name in xConsiderDist:
                continue
            distPop[district_name] = population
        #
        distPoly = {}
        kml_fpath = opath.join(ef_dpath, 'MP14_SUBZONE_WEB_PL.kml')
        with open(kml_fpath) as f:
            kml_doc = parser.parse(f).getroot().Document
        for pm in kml_doc.Folder.Placemark:
            str_coords = str(pm.MultiGeometry.Polygon.outerBoundaryIs.LinearRing.coordinates)
            poly_coords = []
            for l in ''.join(str_coords.split()).split(',0')[:-1]:
                lng, lat = map(eval, l.split(','))
                poly_coords.append([lat, lng])
            district_name = str(pm.name).title()
            if "'S" in district_name:
                district_name = district_name.replace("'S", "'s")
            if "S'Pore" in district_name:
                district_name = district_name.replace("S'Pore", "S'pore")
            if district_name in xConsiderDist:
                continue
            assert district_name in distPop
            distPoly[district_name] = poly_coords
            with open(csv_fpath, 'a') as w_csvfile:
                writer = csv.writer(w_csvfile, lineterminator='\n')
                writer.writerow([district_name, distPop[district_name]])
        #
        with open(pop_fpath_PKL, 'wb') as fp:
            pickle.dump(distPop, fp)
        with open(poly_fpath_PKL, 'wb') as fp:
            pickle.dump(distPoly, fp)
    else:
        with open(pop_fpath_PKL, 'rb') as fp:
            distPop = pickle.load(fp)
        with open(poly_fpath_PKL, 'rb') as fp:
            distPoly = pickle.load(fp)
    #
    return distPop, distPoly


def get_distPop():
    pop_fpath = opath.join(pf_dpath, 'DistrictsPopulation.pkl')
    if not opath.exists(pop_fpath):
        get_districtPopPoly()
    with open(pop_fpath, 'rb') as fp:
        distPop = pickle.load(fp)
    return distPop


def get_distPoly():
    poly_fpath = opath.join(pf_dpath, 'DistrictsPolygon.pkl')
    if not opath.exists(poly_fpath):
        get_districtPopPoly()
    with open(poly_fpath, 'rb') as fp:
        distPoly = pickle.load(fp)
    return distPoly


def get_sgBorder():
    sgBorder_fpath = opath.join(pf_dpath, 'sgBorderPolygon.pkl')
    if not opath.exists(sgBorder_fpath):
        distPoly = get_distPoly()
        sgBorderPolys = cascaded_union([Polygon(poly) for _, poly in distPoly.items()])
        sgBorder = [np.array(poly.coords).tolist() for poly in sgBorderPolys.boundary]
        with open(sgBorder_fpath, 'wb') as fp:
            pickle.dump(sgBorder, fp)
    else:
        with open(sgBorder_fpath, 'rb') as fp:
            sgBorder = pickle.load(fp)
    #
    return sgBorder


def get_distCBD():
    distCBD_fpath = opath.join(pf_dpath, 'DistrictCBD.pkl')
    if not opath.exists(distCBD_fpath):
        distCBD = []
        xls_fpath = opath.join(ef_dpath, 'ResidentPopulation2015.xls')
        book = open_workbook(xls_fpath)
        sh = book.sheet_by_name('T7(Total)')
        for i in range(sh.nrows):
            PlanningArea = sh.cell(i, 1).value
            if PlanningArea != 'Downtown Core':
                continue
            else:
                j = i + 1
                district_name = sh.cell(j, 2).value
                while district_name != 'Total':
                    distCBD.append(district_name)
                    j += 1
                    district_name = sh.cell(j, 2).value
                break
    else:
        with open(distCBD_fpath, 'rb') as fp:
            distCBD = pickle.load(fp)
    #
    return distCBD


def gen_distWholeJSON():
    gjson_fpath = opath.join(pf_dpath, 'districtWhole.json')
    geo_json = {"type": "FeatureCollection", "features": []}
    distPoly = get_distPoly()
    for distName, poly_latlng in distPoly.items():
        poly_lnglat = [[lon, lat] for lat, lon in poly_latlng]
        feature = {"type": "Feature",
                   "Name": distName,
                   "geometry":
                       {"type": "Polygon",
                        "coordinates": [poly_lnglat]
                        }
                   }
        geo_json["features"].append(feature)
    with open(gjson_fpath, 'w') as f:
        json.dump(geo_json, f)


def gen_distCBDJSON():
    gjson_fpath = opath.join(pf_dpath, 'districtCBD.json')
    geo_json = {"type": "FeatureCollection", "features": []}
    distCBD, distPoly = get_distCBD(), get_distPoly()
    for distName, poly_latlng in distPoly.items():
        if not distName in distCBD:
            continue
        poly_lnglat = [[lon, lat] for lat, lon in poly_latlng]
        feature = {"type": "Feature",
                   "Name": distName,
                   "geometry":
                       {"type": "Polygon",
                        "coordinates": [poly_lnglat]
                        }
                   }
        geo_json["features"].append(feature)
    with open(gjson_fpath, 'w') as f:
        json.dump(geo_json, f)


def gen_distXCBDJSON():
    gjson_fpath = opath.join(pf_dpath, 'districtXCBD.json')
    geo_json = {"type": "FeatureCollection", "features": []}
    distCBD, distPoly = get_distCBD(), get_distPoly()
    for distName, poly_latlng in distPoly.items():
        if distName in distCBD:
            continue
        poly_lnglat = [[lon, lat] for lat, lon in poly_latlng]
        feature = {"type": "Feature",
                   "Name": distName,
                   "geometry":
                       {"type": "Polygon",
                        "coordinates": [poly_lnglat]
                        }
                   }
        geo_json["features"].append(feature)
    with open(gjson_fpath, 'w') as f:
        json.dump(geo_json, f)


if __name__ == '__main__':
    pass
