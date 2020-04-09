import os.path as opath
import os, sys
import csv, pickle
from bisect import bisect
from itertools import chain
from shapely.geometry import Polygon, Point
#
from PyQt5.QtWidgets import QWidget, QApplication, QShortcut
from PyQt5.QtGui import (QPainter, QFont, QPen, QColor, QKeySequence, QTextDocument,
                         QImage, QPalette)
from PyQt5.QtCore import Qt, QSize, QRectF, QSizeF, QPointF
#
from sgDistrict import get_sgBorder, get_distPoly, get_districtZone, get_sgGrid, get_validGrid
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

sgBorder = get_sgBorder()
min_lng, max_lng = 1e400, -1e400
min_lat, max_lat = 1e400, -1e400
for poly in sgBorder:
    for lat, lng in poly:
        if lng < min_lng:
            min_lng = lng
        if lng > max_lng:
            max_lng = lng
        if lat < min_lat:
            min_lat = lat
        if lat > max_lat:
            max_lat = lat
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
        for poly in sgBorder:
            sgBorderPartial_xy = []
            for lat, lng in poly:
                x, y = convert_GPS2xy(lng, lat)
                sgBorderPartial_xy += [(x, y)]
            sgBoarderXY.append(sgBorderPartial_xy)
        sgDistrictXY = {}
        distPoly = get_distPoly()
        for dist_name, poly in distPoly.items():
            points = []
            for lat, lng in poly:
                points.append(convert_GPS2xy(lng, lat))
            sgDistrictXY[dist_name] = points
        self.sg = Singapore(sgBoarderXY, sgDistrictXY)
        self.objForDrawing = [self.sg]
        #
        self.gzs = {}
        lats, lngs = get_sgGrid()
        for i, j in get_validGrid():
            lng0, lng1 = lngs[i], lngs[i + 1]
            lat0, lat1 = lats[j], lats[j + 1]
            cLng, cLat = (lng0 + lng1) / 2.0, (lat0 + lat1) / 2.0
            #
            zid = '(%d,%d)' % (i, j)
            cp = QPointF(*convert_GPS2xy(cLng, cLat))
            rect = QRectF(QPointF(*convert_GPS2xy(lng0, lat1)),
                          QPointF(*convert_GPS2xy(lng1, lat0)))
            gz = GridZone(zid, cp, rect)
            self.gzs[i, j] = gz
            self.objForDrawing.append(gz)

        if self.fpaths:
            with open(self.fpaths['AGTK'], 'rb') as fp:
                agents, tasks = pickle.load(fp)

            for tk in tasks:
                zi, zj = bisect(lngs, tk['LngD']) - 1, bisect(lats, tk['LatD']) - 1
                z = self.gzs[zi, zj]
                z.increaseCounter()

            if SHOW_AGENT:
                for agt in agents:
                    RRs = []
                    for aRR in agt['RRs']:
                        trajXY = []
                        for mvt in aRR['mvts']:
                            trajXY.append([convert_GPS2xy(lng, lat) for lat, lng in mvt['traj']])
                        RRs.append([aRR['prob'], trajXY])
                    self.objForDrawing.append(Agent(agt['aid'], agt['cid'], RRs))
                for tk in tasks:
                    pcx, pcy = convert_GPS2xy(tk['LngP'], tk['LatP'])
                    dcx, dcy = convert_GPS2xy(tk['LngD'], tk['LatD'])
                    self.objForDrawing.append(Task(tk['tid'],
                                                   [pcx, pcy], [dcx, dcy]))

    def mousePressEvent(self, QMouseEvent):
        if self.mousePressed:
            self.mousePressed = False
            self.px, self.py = -1, -1
            self.update()
        else:
            pos = QMouseEvent.pos()
            x, y = [f() for f in [pos.x, pos.y]]
            dist_name = self.sg.get_distName(x, y)
            if dist_name:
                self.mousePressed = True
                self.selDistLabel = QTextDocument()
                self.selDistLabel.setHtml(dist_name)
                self.selDistLabel.setDefaultFont(Viz.font)
                self.labelW = len(dist_name) * Viz.unit_labelW
                self.px, self.py = x - self.labelW / 2, y - Viz.labelH
                print(dist_name)
                self.update()


class GridZone(object):
    MAX_COUNTER = 0

    def __init__(self, zid, cp, rect):
        self.zid, self.cp, self.rect = zid, cp, rect
        self.counter = 0

    def increaseCounter(self):
        self.counter += 1
        if GridZone.MAX_COUNTER < self.counter:
            GridZone.MAX_COUNTER = self.counter

    def draw(self, qp):
        if self.counter == 0:
            pen = QPen(Qt.black, 0.5, Qt.SolidLine)
            qp.setPen(pen)
            qc = QColor('white')
            qc.setAlpha(ALPHA_TRANSPARENT)
        else:
            ratio = float(self.counter / GridZone.MAX_COUNTER)
            cIndex = int(len(HEATMAP_COLORS) * ratio)
            if cIndex == 0:
                pen_color = Qt.black
                qc = QColor('white')
                qc.setAlpha(ALPHA_TRANSPARENT)
            else:
                pen_color = QColor(HEATMAP_COLORS[int(len(HEATMAP_COLORS) * ratio) - 1].get_hex_l())
                qc = QColor(pen_color)
                qc.setAlpha(ALPHA2)
            pen = QPen(pen_color, 0.5, Qt.SolidLine)
            qp.setPen(pen)
        qp.setBrush(qc)
        qp.drawRect(self.rect)
        #
        if self.counter != 0:
            pen = QPen(Qt.black, 0.5, Qt.SolidLine)
            qp.setPen(pen)
            qp.drawText(self.cp, '%d' % self.counter)



class Task(object):
    arrow_HS, arrow_VS = 15, 10
    font = QFont('Decorative', 12)
    unit_labelW = 20
    labelH = 30

    def __init__(self, tid, pXY, dXY):
        self.tid = tid
        self.pcx, self.pcy = pXY
        self.dcx, self.dcy = dXY

    def draw(self, qp):
        pen = QPen(Qt.black, 2, Qt.SolidLine)
        qp.setPen(pen)
        for x0, y0, x1, y1 in [
                                (self.pcx, self.pcy, self.pcx + LocPD_dotSize, self.pcy + LocPD_dotSize),
                                (self.pcx + LocPD_dotSize, self.pcy, self.pcx, self.pcy + LocPD_dotSize),
                                (self.dcx, self.dcy, self.dcx + LocPD_dotSize, self.dcy + LocPD_dotSize),
                                (self.dcx + LocPD_dotSize, self.dcy, self.dcx, self.dcy + LocPD_dotSize)
                            ]:
            qp.drawLine(x0, y0, x1, y1)


class Agent(object):
    def __init__(self, aid, cid, RRs):
        self.aid, self.cid = aid, cid
        self.RRs = RRs

    def draw(self, qp):
        pen_color = QColor(pallet[self.aid % len(pallet)])
        for rrid, (_, trajs) in enumerate(self.RRs):
            ix, iy = trajs[0][0]
            qp.setPen(QPen(pen_color, 2, style=lineStyle[rrid % len(lineStyle)]))
            qp.drawText(ix, iy, '%s_%d' % (self.aid, rrid))
            traj = list(chain(*trajs))
            px, py = traj[0]
            for j in range(1, len(traj)):
                cx, cy = traj[j]
                qp.drawLine(px, py, cx, cy)
                px, py = cx, cy


class Singapore(object):
    def __init__(self, sgBoarderXY, sgDistrictXY):
        self.sgBoarderXY = [[QPointF(*xy) for xy in points] for points in sgBoarderXY]
        self.sgPolys = [Polygon(points) for points in sgBoarderXY]
        self.sgDistrictXY = sgDistrictXY
        self.sgDistrictPolyXY = {}
        for dn, points in self.sgDistrictXY.items():
            self.sgDistrictPolyXY[dn] = Polygon(points)
            self.sgDistrictXY[dn] = [QPointF(*xy) for xy in points]
        self.districtZone = get_districtZone()
        self.zoneColor = {zn: pallet[i] for i, zn in enumerate(set(self.districtZone.values()))}

    def get_distName(self, x, y):
        p0 = Point(x, y)
        for dist_name, poly in self.sgDistrictPolyXY.items():
            if p0.within(poly):
                return dist_name
        else:
            return None

    def draw(self, qp):
        pen = QPen(Qt.black, 0.2, Qt.DashLine)
        qp.setPen(pen)
        for dn, points in self.sgDistrictXY.items():
            qc = QColor(self.zoneColor[self.districtZone[dn]])
            qc.setAlpha(ALPHA1)
            qp.setBrush(qc)
            qp.drawPolygon(*points)
        pen = QPen(Qt.black, 1)
        qp.setPen(pen)
        for _, points in enumerate(self.sgBoarderXY):
            qp.drawPolyline(*points)
            # self.drawPoly(qp, poly)


def runSingle():
    from __path_organizer import exp_dpath
    from functools import reduce
    #
    pkl_dpath = reduce(opath.join, [exp_dpath, 'problem', 'pkl'])
    # pkl_fpath = opath.join(pkl_dpath, 'AGTK_na005-nt010-vc10-wc10-sn00.pkl')
    pkl_fpath = opath.join(pkl_dpath, 'AGTK_na050-nt500-vc10-wc10-sn00.pkl')

    # fpaths = {
    #     'AGTK': pkl_fpath,
    # }



    fpaths = {}
    #
    app = QApplication(sys.argv)
    viz = Viz(fpaths)
    viz.save_img()
    sys.exit(app.exec_())



if __name__ == '__main__':
    runSingle()