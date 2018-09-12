import datetime
import csv


def write_log(fpath, contents):
    with open(fpath, 'a') as f:
        logContents = '\n\n'
        logContents += '======================================================================================\n'
        logContents += '%s\n' % str(datetime.datetime.now())
        logContents += '%s\n' % contents
        logContents += '======================================================================================\n'
        f.write(logContents)


def res2file(fpath, objV, gap, eliCpuTime, eliWallTime):
    with open(fpath, 'wt') as w_csvfile:
        writer = csv.writer(w_csvfile, lineterminator='\n')
        header = ['objV', 'Gap', 'eliCpuTime', 'eliWallTime']
        writer.writerow(header)
        writer.writerow([objV, gap, eliCpuTime, eliWallTime])



