import os.path as opath
import sys
import csv, pickle

from __path_organizer import ef_dpath, pf_dpath


def get_coordBS():
    csv_fpath = opath.join(pf_dpath, 'busStops.csv')
    pkl_fpath = opath.join(pf_dpath, 'busStops.pkl')
    if not opath.exists(csv_fpath):
        import geopandas as gpd
        sys.path.append(ef_dpath)
        # noinspection PyUnresolvedReferences
        from SVY21 import SVY21
        cv = SVY21()
        #
        with open(csv_fpath, 'w') as w_csvfile:
            writer = csv.writer(w_csvfile, lineterminator='\n')
            new_headers = ['Bus_Stop_ID', 'Roof_ID', 'Lat', 'Lng']
            writer.writerow(new_headers)
        bs_coord = {}
        #
        gpf = gpd.read_file(opath.join(ef_dpath, 'BusStop.shp'))
        numBusStops = len(gpf)
        data = gpf.to_dict()
        header = list(data.keys())
        for i in range(numBusStops):
            bs_num, br_num, _, geometry = [data[cn][i] for cn in header]
            E, N = geometry.x, geometry.y
            lat, lng = cv.computeLatLon(N, E)
            with open(csv_fpath, 'a') as w_csvfile:
                writer = csv.writer(w_csvfile, lineterminator='\n')
                writer.writerow([bs_num, br_num, lat, lng])
            bs_coord[bs_num] = lat, lng
        with open(pkl_fpath, 'wb') as fp:
            pickle.dump(bs_coord, fp)
    else:
        with open(pkl_fpath, 'rb') as fp:
            bs_coord = pickle.load(fp)
    #
    return bs_coord


if __name__ == '__main__':
    print(len(get_coordBS()))
    # for k, v in get_coordBS().items():
    #     print(k, v)