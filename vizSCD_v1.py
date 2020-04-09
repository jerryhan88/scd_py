import os.path as opath
import os, sys
import csv, pickle
from bisect import bisect
from itertools import chain
from shapely.geometry import Point
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon
#
from PyQt5.QtWidgets import QWidget, QApplication, QShortcut
from PyQt5.QtGui import (QPainter, QFont, QPen, QColor, QKeySequence, QTextDocument,
                         QImage, QPalette)
from PyQt5.QtCore import Qt, QSize, QRectF, QSizeF, QPointF
#
# from sgDistrict import get_sgBorder, get_distPoly, get_districtZone, get_sgGrid, get_validGrid

from sgGeo import get_sg, get_subZones, get_planningAreas, get_regions
from colour import Color

ALPHA1 = 10
ALPHA2 = 150
ALPHA_TRANSPARENT = 0

pallet = [
    Color('blue').get_hex_l(),
    Color('brown').get_hex_l(),
    Color('magenta').get_hex_l(),
    Color('green').get_hex_l(),
    Color('indigo').get_hex_l(),
    Color('red').get_hex_l(),
    Color('khaki').get_hex_l(),
    Color('maroon').get_hex_l(),
    Color('navy').get_hex_l(),
    Color('orange').get_hex_l(),
    Color('pink').get_hex_l(),
    Color('grey').get_hex_l(),
]

# HEATMAP_COLORS = list(Color("white").range_to(Color("magenta"), 20))

HEATMAP_COLORS = list(Color("#CECCF8").range_to(Color("#9B0635"), 20))

lineStyle = [Qt.SolidLine, Qt.DotLine, Qt.DashLine,
             Qt.DashDotLine, Qt.DashDotDotLine, Qt.CustomDashLine]

SG = get_sg()
min_lng, min_lat, max_lng, max_lat = SG.boundary.bounds
lng_gap = max_lng - min_lng
lat_gap = max_lat - min_lat


WIDTH = 1800.0
# WIDTH = 800.0
HEIGHT = lat_gap * (WIDTH / lng_gap)

FRAME_ORIGIN = (60, 100)
LocPD_dotSize = 5
SHOW_AGENT = False


def convert_GPS2xy(lng, lat):
    x = (lng - min_lng) / lng_gap * WIDTH
    y = (max_lat - lat) / lat_gap * HEIGHT
    return x, y


def convert_xy2GPS(x, y):
    lng = (x / WIDTH * lng_gap) + min_lng
    lat = max_lat - (y / HEIGHT * lat_gap)
    return lng, lat


class Viz(QWidget):
    font = QFont('Decorative', 15)
    labelH = 30
    unit_labelW = 15

    def __init__(self, fpaths=None):
        super().__init__()
        self.app_name = 'Viz'
        self.fpaths = fpaths
        self.objForDrawing = []
        #
        self.mousePressed = False
        self.px, self.py = -1, -1
        #
        self.init_drawing()
        self.initUI()
        #
        self.shortcut = QShortcut(QKeySequence('Ctrl+W'), self)
        self.shortcut.activated.connect(self.close)

    def initUI(self):
        self.setGeometry(FRAME_ORIGIN[0], FRAME_ORIGIN[1], WIDTH, HEIGHT)
        self.setWindowTitle(self.app_name)
        self.setFixedSize(QSize(WIDTH, HEIGHT))
        #
        self.image = QImage(WIDTH, HEIGHT, QImage.Format_RGB32)
        self.image.fill(Qt.white)
        pal = self.palette()
        pal.setColor(QPalette.Background, Qt.white)
        self.setAutoFillBackground(True)
        self.setPalette(pal)
        #
        self.show()

    def save_img(self):
        self.image.save('temp.png', 'png')

    def paintEvent(self, e):
        for canvas in [self, self.image]:
            qp = QPainter()
            qp.begin(canvas)
            self.drawCanvas(qp)
            qp.end()

    def drawCanvas(self, qp):
        for o in self.objForDrawing:
            o.draw(qp)
        if self.mousePressed:
            qp.translate(self.px, self.py)
            self.selDistLabel.drawContents(qp, QRectF(0, 0, self.labelW, Viz.labelH))
            qp.translate(-self.px, -self.py)

    def init_drawing(self):
        sgBoarderXY = []
        for poly in SG:
            sgBorderPartial_xy = []
            for lng, lat in zip(*poly.exterior.coords.xy):
                x, y = convert_GPS2xy(lng, lat)
                sgBorderPartial_xy += [(x, y)]
            sgBoarderXY.append(sgBorderPartial_xy)
        #
        subZones = get_subZones()
        sgSubZone = {}
        for sz in subZones:
            mpolyXY = []
            if type(sz['geometry']) == Polygon:
                poly = sz['geometry']
                polyXY = []
                for lng, lat in zip(*poly.exterior.coords.xy):
                    polyXY.append(convert_GPS2xy(lng, lat))
                mpolyXY.append(polyXY)
            else:
                assert type(sz['geometry']) == MultiPolygon
                mpoly = sz['geometry']
                for poly in mpoly:
                    polyXY = []
                    for lng, lat in zip(*poly.exterior.coords.xy):
                        polyXY.append(convert_GPS2xy(lng, lat))
                    mpolyXY.append(polyXY)
            sgSubZone[sz['SUBZONE_N']] = {'PLN_AREA_N': sz['PLN_AREA_N'],
                                            'REGION_N': sz['REGION_N'],
                                            'mpolyXY': mpolyXY}
        #
        planningAreas = get_planningAreas()
        sgPlanningArea = {}
        for pa in planningAreas:
            pa_name, geometry = pa['name'], pa['geometry']
            mpolyXY = []
            if type(geometry) == MultiPolygon:
                for poly in geometry:
                    polyXY = []
                    for lng, lat in zip(*poly.exterior.coords.xy):
                        polyXY.append(convert_GPS2xy(lng, lat))
                    mpolyXY.append(polyXY)
            else:
                assert type(geometry) == Polygon
                polyXY = []
                for lng, lat in zip(*geometry.exterior.coords.xy):
                    polyXY.append(convert_GPS2xy(lng, lat))
                mpolyXY.append(polyXY)
            ct = pa['centroid']
            sgPlanningArea[pa_name] = {'mpolyXY': mpolyXY,
                                       'centroid': convert_GPS2xy(ct.x, ct.y)}
        #
        regions = get_regions()
        sgRegion = {}
        for rg in regions:
            rg_name = rg['name']
            ct = rg['centroid']
            sgRegion[rg_name] = {'centroid': convert_GPS2xy(ct.x, ct.y)}
        self.sg = Singapore(sgBoarderXY, sgSubZone, sgPlanningArea, sgRegion)
        self.objForDrawing = [self.sg]

    def mousePressEvent(self, QMouseEvent):
        if self.mousePressed:
            self.mousePressed = False
            self.px, self.py = -1, -1
            self.update()
        else:
            pos = QMouseEvent.pos()
            x, y = [f() for f in [pos.x, pos.y]]
            dist_name = self.sg.get_subZoneName(x, y)
            if dist_name:
                self.mousePressed = True
                self.selDistLabel = QTextDocument()
                self.selDistLabel.setHtml(dist_name)
                self.selDistLabel.setDefaultFont(Viz.font)
                self.labelW = len(dist_name) * Viz.unit_labelW
                self.px, self.py = x - self.labelW / 2, y - Viz.labelH
                print(dist_name)
                self.update()


class Singapore(object):
    rgCentSize = 10
    paCentSize = 3
    def __init__(self, sgBoarderXY, sgSubZone, sgPlanningArea, sgRegion):
        self.sgBoarderXY = [[QPointF(*xy) for xy in points] for points in sgBoarderXY]
        self.sgPolys = [Polygon(points) for points in sgBoarderXY]
        self.sgSubZone = sgSubZone
        self.sgSubZonePolyXY = {}
        regions = set()
        for sz_n, sz in self.sgSubZone.items():
            mpolyXY = sz['mpolyXY']
            mpoly_coords_qt = []
            mpoly = []
            for polyXY in mpolyXY:
                mpoly_coords_qt.append([QPointF(*xy) for xy in polyXY])
                mpoly.append(Polygon(polyXY))
            sz['mpolyCoordsQT'] = mpoly_coords_qt
            self.sgSubZonePolyXY[sz_n] = mpoly
            regions.add(sz['REGION_N'])
        self.sgPlanningArea = sgPlanningArea
        for pa_n, pa in self.sgPlanningArea.items():
            mpolyXY = pa['mpolyXY']
            mpoly_coords_qt = []
            for polyXY in mpolyXY:
                mpoly_coords_qt.append([QPointF(*xy) for xy in polyXY])
            pa['mpolyCoordsQT'] = mpoly_coords_qt
            pa['centroidQT'] = QPointF(*pa['centroid'])
        self.sgRegion = sgRegion
        for rg_n, rg in self.sgRegion.items():
            rg['centroidQT'] = QPointF(*rg['centroid'])


        self.RegionColor = {re_n: pallet[i % len(pallet)] for i, re_n in enumerate(regions)}

    def get_subZoneName(self, x, y):
        pass
        p0 = Point(x, y)
        for sz_name, mpoly in self.sgSubZonePolyXY.items():
            for poly in mpoly:
                if p0.within(poly):
                    return sz_name
        else:
            return None

    def draw(self, qp):
        pen = QPen(Qt.black, 0.2, Qt.DashLine)
        qp.setPen(pen)
        for sz_name, sz in self.sgSubZone.items():
            qc = QColor(self.RegionColor[sz['REGION_N']])
            qc.setAlpha(ALPHA1)
            qp.setBrush(qc)
            for mpolyCoordsQT in sz['mpolyCoordsQT']:
                qp.drawPolygon(*mpolyCoordsQT)

        pen = QPen(Qt.black, 1.0)
        qp.setPen(pen)
        for pa in self.sgPlanningArea.values():
            for polyXY in pa['mpolyCoordsQT']:
                qp.drawPolyline(*polyXY)
            r = Singapore.paCentSize
            qp.drawEllipse(pa['centroidQT'], r, r)

        pen = QPen(Qt.red, 1.5)
        qp.setPen(pen)
        for rg_n, rg in self.sgRegion.items():
            x = rg['centroidQT'].x()
            y = rg['centroidQT'].y()
            ss = Singapore.rgCentSize
            qp.drawRect(x - ss / 2, y - ss / 2, ss, ss)

        pen = QPen(Qt.black, 1.5)
        qp.setPen(pen)
        for points in self.sgBoarderXY:
            qp.drawPolyline(*points)


def runSingle():
    from __path_organizer import exp_dpath
    from functools import reduce
    #

    fpaths = {}
    #
    app = QApplication(sys.argv)
    viz = Viz(fpaths)
    viz.save_img()
    sys.exit(app.exec_())



if __name__ == '__main__':
    runSingle()