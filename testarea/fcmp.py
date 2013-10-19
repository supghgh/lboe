#!/usr/bin/env python
# one file full load, seconds one line at a time
import time

PRIM_COL = 0
DELIMITER = ','
EXTRA_SRC = []
UNMATCHED = []

def csv_to_dict(fd, ret):
    for i in fd:
        vals = i.split(DELIMITER)
        ret[vals[PRIM_COL]] = vals
        
def compare(fd):
    global d
    for i in fd:
        rowitems = i.split(DELIMITER)
        key = rowitems[PRIM_COL]
        try:
            refvals = d[key]
        except KeyError:
            EXTRA_SRC.append(i)
        else:            
            for idx in range(len(refvals)):
                if rowitems[idx] != refvals[idx]:
                    UNMATCHED.append((i, DELIMITER.join(refvals)))
                    break
            del d[key]


if __name__ == "__main__":
    f1 = open("/home/supratim/Downloads/lahman2012-csv/Master.csv")
    d  = dict()
    f2 = open("/home/supratim/Downloads/lahman2012-csv/Master1.csv")
    st = time.time()
    csv_to_dict(f2, d)
    f2.close()
    print "Time to convert to dict : ", time.time()-st
    st = time.time()
    compare(f1)
    print "Time to compare : ", time.time()-st
    f1.close()
    print "EXTRA_SRC : ", EXTRA_SRC
    print "EXTRA_DST : ", d
    print "UNMATCHED : ", UNMATCHED
