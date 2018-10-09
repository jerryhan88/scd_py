import os.path as opath
import sys
import pickle
import numpy as np
#
from PyQt5.QtWidgets import QWidget, QApplication
from PyQt5.QtGui import (QPen, QColor, QFont, QTextDocument, QPainter, QImage, QPainterPath)
from PyQt5.QtCore import (Qt,
                            QPoint, QSize, QRectF)
#
pallet = [
        "#0000ff",  # blue
        "#a52a2a",  # brown
        "#ff00ff",  # magenta
        "#008000",  # green
        "#4b0082",  # indigo
        "#f0e68c",  # khaki
        "#800000",  # maroon
        "#000080",  # navy
        "#ffa500",  # orange
        "#ffc0cb",  # pink
        "#ff0000",  # red
        "#808080",  # grey
]


mainFrameOrigin = (100, 150)
frameSize = (1600, 800)

hMargin, vMargin = 10, 10



def drawLabel(qp, label, cx, cy, w, h):
    qp.translate(cx - w / 2, cy - h / 2)
    label.drawContents(qp, QRectF(0, 0, w, h))
    qp.translate(-(cx - w / 2), -(cy - h / 2))


class Agent(object):
    arrow_HS, arrow_VS = 10, 5
    labelW1, labelH = 25, 25
    labelW2 = 150
    font = QFont('Decorative', 12, italic=True)
    maxLineTN = 5

    def __init__(self, aid, agentInfo, drawingInputs):
        self.aid = aid
        self.agentInfo, self.drawingInputs = agentInfo, drawingInputs
        #
        self.prep_trajDrawing()
        self.oLabels, self.dLabels = [], []
        self.luLabels = []
        for r, (_, timeBudget, oLoc, seqLocs, dLoc) in enumerate(agentInfo):
            label = QTextDocument()
            label.setHtml("o<sup>%d</sup><sub>%d</sub>" % (self.aid, r))
            label.setDefaultFont(Task.font)
            self.oLabels.append(label)
            label = QTextDocument()
            label.setHtml("d<sup>%d</sup><sub>%d</sub>" % (self.aid, r))
            label.setDefaultFont(Task.font)
            self.dLabels.append(label)
            #
            locS = [oLoc]
            for loc in seqLocs:
                locS.append(loc)
            locS.append(dLoc)
            dist = 0.0
            for i in range(len(locS) - 1):
                dist += np.linalg.norm(locS[i] - locS[i + 1])
            label = QTextDocument()
            label.setHtml("RR<sup>%d</sup><sub>%d</sub>=%.2f u<sup>%d</sup><sub>%d</sub>=%.2f" %
                          (self.aid, r, dist, self.aid, r, timeBudget))
            label.setDefaultFont(Task.font)
            self.luLabels.append(label)


    def prep_trajDrawing(self):
        self.trajectories = []
        for i, trajXY in enumerate(self.drawingInputs):
            traj = []
            oriXY, seqXY, destXY = trajXY
            oriX, oriY = oriXY
            destX, destY = destXY
            for i, (seqX, seqY) in enumerate(seqXY):
                if i == 0:
                    lastX, lastY = oriX, oriY
                else:
                    lastLine = traj[-1]
                    lastX, lastY = lastLine[2], lastLine[3]
                traj.append([lastX, lastY, seqX, seqY])
            lastLine = traj[-1]
            lastX, lastY = lastLine[2], lastLine[3]
            traj.append([lastX, lastY, destX, destY])
            ax, ay = destX - lastX, destY - lastY
            la = np.sqrt(ax ** 2 + ay ** 2)
            ux, uy = ax / la, ay / la
            px, py = -uy, ux
            traj.append([destX, destY,
                         destX - (ux * Agent.arrow_HS) + (px * Agent.arrow_VS),
                         destY - (uy * Agent.arrow_HS) + (py * Agent.arrow_VS)])
            traj.append([destX, destY,
                         destX - (ux * Agent.arrow_HS) - (px * Agent.arrow_VS),
                         destY - (uy * Agent.arrow_HS) - (py * Agent.arrow_VS)])
            self.trajectories.append([self.agentInfo[i][0], traj])

    def draw(self, qp):
        for prob, traj in self.trajectories:
            pen = QPen(Qt.black, Agent.maxLineTN * prob, Qt.SolidLine)
            qp.setPen(pen)
            qp.setBrush(Qt.NoBrush)
            for x0, y0, x1, y1 in traj:
                qp.drawLine(x0, y0, x1, y1)
        for i in range(len(self.oLabels)):
            x0, y0, _, _ = self.trajectories[i][1][0]
            _, _, x1, y1 = self.trajectories[i][1][-1]
            cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
            #
            drawLabel(qp, self.oLabels[i],
                      x0 - 8, y0 - 2, Agent.labelW1, Agent.labelH)
            drawLabel(qp, self.dLabels[i],
                      x1 + 15, y1 + 10, Agent.labelW1, Agent.labelH)
            drawLabel(qp, self.luLabels[i],
                      cx, cy, Agent.labelW2, Agent.labelH)



class Task(object):
    font = QFont('Decorative', 10)
    labelW, labelH = 20, 20
    pen = QPen(Qt.black, 0.5, Qt.DashLine)
    dotSize = 18

    def __init__(self, tid, taskInfo, drawingInputs):
        self.tid = tid
        #
        self.plabel, self.dlabel = QTextDocument(), QTextDocument()
        self.plabel.setHtml("%d<sup>+</sup>" % self.tid)
        self.dlabel.setHtml("%d<sup>-</sup>" % self.tid)
        self.plabel.setDefaultFont(Task.font)
        self.dlabel.setDefaultFont(Task.font)
        #
        pLocXY, dLocXY, pTW_Xs, dTW_Xs, self.coX4GC, self.oriY_GC, self.yUnit = drawingInputs
        self.ppX, self.ppY = pLocXY
        self.dpX, self.dpY = dLocXY
        self.pTW_rect = self.get_twRect(pTW_Xs, isPickupTW=True)
        self.dTW_rect = self.get_twRect(dTW_Xs, isPickupTW=False)

    def get_twRect(self, tw, isPickupTW=False):
        a, b = tw
        return QRectF(a, self.oriY_GC if isPickupTW else self.oriY_GC + self.yUnit,
                      b - a, self.yUnit)

    def draw(self, qp):
        for cx, cy, label in [(self.ppX, self.ppY, self.plabel),
                              (self.dpX, self.dpY, self.dlabel)]:
            drawLabel(qp, label,
                      cx, cy, Task.labelW, Task.labelH)

        qp.setPen(Task.pen)
        qp.setBrush(Qt.NoBrush)
        for f, (p0, p1) in [(qp.drawEllipse, (self.ppX, self.ppY)),
                            (qp.drawRect, (self.dpX, self.dpY))]:
            f(p0 - Task.dotSize / 2, p1 - Task.dotSize / 2,
                Task.dotSize, Task.dotSize)
        #
        pen = QPen(Qt.black, 1, Qt.SolidLine)
        qp.setPen(pen)
        qp.setBrush(Qt.NoBrush)
        for cx, cy, label in [(self.coX4GC - hMargin * 2.0, vMargin * 3 + self.oriY_GC, self.plabel),
                              (self.coX4GC - hMargin * 2.0, vMargin * 3 + self.oriY_GC + self.yUnit, self.dlabel)]:
            drawLabel(qp, label,
                      cx, cy, Task.labelW, Task.labelH)
        qp.drawRect(self.pTW_rect)
        qp.drawRect(self.dTW_rect)


class Viz(QWidget):
    def __init__(self, pkl_files):
        super().__init__()
        self.drawingInfo = {}
        for k, fpath in pkl_files.items():
            with open(fpath, 'rb') as fp:
                self.drawingInfo[k] = pickle.load(fp)
        self.numIntvGC = 10
        #
        self.init_objOnCanvas()
        self.initUI()

    def convertLoc2XY(self, loc, canvSize):
        hLoc, vLoc = loc
        return hLoc * canvSize[0], vLoc * canvSize[1]

    def init_objOnCanvas(self):
        taskLocTW, agentRR = self.drawingInfo['dplym']
        hCanvSize = frameSize[0] / float(2), frameSize[1]
        self.coX4GC, self.coY4GC = (hCanvSize[0] + hMargin * 4, vMargin * 1) # Upper left of the Gantt chart
        self.xLen = (frameSize[0] - self.coX4GC) - hMargin
        self.yLen = (frameSize[1] - self.coY4GC) - vMargin * 4
        self.xUnit = self.xLen / self.numIntvGC
        yUnit = ((self.yLen / len(taskLocTW)) - vMargin) / 2
        #
        self.tasks, self.agents = [], []
        for tid, taskInfo in enumerate(taskLocTW):
            reward, pST, dST, pLoc, dLoc, pTW, dTW = taskInfo
            pLocXY = self.convertLoc2XY(pLoc, hCanvSize)
            dLocXY = self.convertLoc2XY(dLoc, hCanvSize)
            #
            oriY_GC = self.coY4GC + tid * vMargin + 2 * tid * yUnit
            pTW_Xs = np.array(pTW) * self.xLen + self.coX4GC
            dTW_Xs = np.array(dTW) * self.xLen + self.coX4GC
            #
            drawingInputs = (pLocXY, dLocXY, pTW_Xs, dTW_Xs, self.coX4GC, oriY_GC, yUnit)
            self.tasks.append(Task(tid, taskInfo, drawingInputs))
        for aid in range(len(agentRR)):
            drawingInputs = []
            agentInfo = agentRR[aid]
            for prob, timeBudget, oLoc, seqLocs, dLoc in agentInfo:
                trajXY = []
                trajXY.append(self.convertLoc2XY(oLoc, hCanvSize))
                seqXY = []
                for loc in seqLocs:
                    seqXY.append(self.convertLoc2XY(loc, hCanvSize))
                trajXY.append(seqXY)
                trajXY.append(self.convertLoc2XY(dLoc, hCanvSize))
                drawingInputs.append(trajXY)
            self.agents.append(Agent(aid, agentInfo, drawingInputs))

    def initUI(self):
        w, h = frameSize
        self.setGeometry(mainFrameOrigin[0], mainFrameOrigin[1], w, h)
        self.setWindowTitle('Viz')
        self.setFixedSize(QSize(w, h))
        self.show()

    def paintEvent(self, e):
        # for dev in [self, self.image]:
        #     qp = QPainter()
        #     qp.begin(dev)
        #     self.drawCanvas(qp)
        #     qp.end()
        qp = QPainter()
        qp.begin(self)
        self.drawCanvas(qp)
        qp.end()

    def drawGC_bg(self, qp):
        pen = QPen(Qt.black, 1, Qt.SolidLine)
        qp.setPen(pen)
        qp.setBrush(Qt.NoBrush)
        qp.drawLine(self.coX4GC, self.coY4GC,
                    self.coX4GC, self.coY4GC + self.yLen)
        qp.drawLine(self.coX4GC, self.coY4GC + self.yLen,
                    self.coX4GC + self.xLen, self.coY4GC + self.yLen)
        qp.drawLine(self.coX4GC + self.xLen,
                    self.coY4GC + self.yLen,
                    self.coX4GC + self.xLen - hMargin, self.coY4GC + self.yLen - vMargin / 2)
        qp.drawLine(self.coX4GC + self.xLen,
                    self.coY4GC + self.yLen,
                    self.coX4GC + self.xLen - hMargin, self.coY4GC + self.yLen + vMargin / 2)
        #

        pen = QPen(Qt.black, 0.5, Qt.DashDotLine)
        qp.setPen(pen)
        for i in range(self.numIntvGC - 1):
            x = self.coX4GC + self.xUnit * (i + 1)
            qp.drawLine(x, self.coY4GC,
                        x, self.coY4GC + self.yLen)
            qp.drawText(QRectF(x - 10, self.coY4GC + self.yLen + 5, 20, 15),
                        Qt.AlignCenter | Qt.AlignTop,
                        "%.1f" % ((i + 1) / self.numIntvGC))

    def drawCanvas(self, qp):
        for o in self.tasks + self.agents:
            o.draw(qp)

        pen = QPen(Qt.black, 0.5, Qt.DashLine)
        qp.setPen(pen)
        qp.drawLine(frameSize[0] / float(2), vMargin,
                        frameSize[0] / float(2), frameSize[1] - vMargin)
        #
        self.drawGC_bg(qp)


if __name__ == '__main__':

    from problems import euclideanDistEx0

    euclideanDistEx0(dpath='_temp')

    dplym_fpath = opath.join('_temp', 'dplym_euclideanDistEx0.pkl')
    # prmts_fpath = opath.join('_temp', 'prmts_euclideanDistEx0.pkl')
    # sol_fpath = opath.join('_temp', 'sol_euclideanDistEx0_EX1.pkl')
    pkl_files = {
        'dplym': dplym_fpath,
    }

    app = QApplication(sys.argv)
    viz = Viz(pkl_files)
    # viz.save_img()
    sys.exit(app.exec_())
