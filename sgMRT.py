import os.path as opath
import os
import pickle, csv
from pykml import parser
from itertools import chain
#
from __path_organizer import ef_dpath, mrtNet_dpath, pf_dpath


def get_mrtLines():
    mrtLines = {}
    for fn in os.listdir(mrtNet_dpath):
        if not fn.endswith('.csv'):
            continue
        lineName = fn[len('Line'):-len('.csv')]
        MRTs = []
        with open(opath.join(mrtNet_dpath, fn)) as r_csvfile:
            reader = csv.DictReader(r_csvfile)
            for row in reader:
                MRTs.append(row['STN'])
        mrtLines[lineName] = MRTs
    #
    return mrtLines


def get_coordMRT():
    csv_fpath = opath.join(pf_dpath, 'MRT_coords.csv')
    pkl_fpath = opath.join(pf_dpath, 'MRT_coords.pkl')
    if not opath.exists(csv_fpath):
        alt_name = {
            'Harbourfront': 'HarbourFront',
            'Marymount ': 'Marymount',
            'Jelepang': 'Jelapang',
            'Macpherson': 'MacPherson',
            'One North': 'one-north'
        }
        with open(csv_fpath, 'w') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            new_headers = ['Name', 'Lat', 'Lng']
            writer.writerow(new_headers)
        #
        mrtLines = get_mrtLines()
        mrtName2013 = set(chain(*[mrts for _, mrts in mrtLines.items()]))
        kml_fpath = opath.join(ef_dpath, 'G_MP14_RAIL_STN_PL.kml')
        with open(kml_fpath) as f:
            kml_doc = parser.parse(f).getroot().Document
        mrt_coord = {}
        for pm in kml_doc.Folder.Placemark:
            min_lat, min_lon = 1e400, 1e400
            max_lat, max_lon = -1e400, -1e400
            str_coords = str(pm.MultiGeometry.Polygon.outerBoundaryIs.LinearRing.coordinates)
            for l in ''.join(str_coords.split()).split(',0')[:-1]:
                lon, lat = map(eval, l.split(','))
                if lat < min_lat:
                    min_lat = lat
                if lon < min_lon:
                    min_lon = lon
                if max_lat < lat:
                    max_lat = lat
                if max_lon < lon:
                    max_lon = lon
            cLat, cLng = (min_lat + max_lat) / 2, (min_lon + max_lon) / 2
            mrt_name = str(pm.name).title()
            if mrt_name == 'Null':
                continue
            for postfix in [' Mrt Station', ' Station', ' Interchange', ' Rail', ' Lrt']:
                if postfix in mrt_name:
                    mrt_name = mrt_name[:-len(postfix)]
                    break
            if mrt_name in ['Outram', 'Tsl', 'Nsle', 'Punggol Central',
                            'Bedok Town Park', 'River Valley', 'Sengkang Central',
                            'Thomson Line', 'Springleaf']:
                continue
            #
            if mrt_name in mrt_coord:
                continue
            if mrt_name in alt_name:
                mrt_name = alt_name[mrt_name]
            if mrt_name not in mrtName2013:
                continue
            with open(csv_fpath, 'a') as w_csvfile:
                writer = csv.writer(w_csvfile, lineterminator='\n')
                writer.writerow([mrt_name, cLat, cLng])
            mrt_coord[mrt_name] = [cLat, cLng]
        #
        with open(pkl_fpath, 'wb') as fp:
            pickle.dump(mrt_coord, fp)
    else:
        with open(pkl_fpath, 'rb') as fp:
            mrt_coord = pickle.load(fp)
    #
    return mrt_coord


if __name__ == '__main__':
    for k, v in get_coordMRT().items():
        print(k, v)