import math
import logging
import pymongo

def calArr(colFunc, field):
    logger = logging.getLogger()
    logger.info("Computing bins start")
    limit = 550
    curSizeArr = colFunc.find().sort(field, pymongo.ASCENDING)
    logging.info("Get sorted data")
    l = curSizeArr.count()
    logger.info(f"Number of size data: {l}")
    lstSizeArr = list(curSizeArr)
    logging.info("Convert to list")
    sizeArr = [d[field] for d in lstSizeArr]
    logging.info("Convert to array")
    binSize = max(math.floor(l / limit), 5)
    logging.info(f"Max bin size {binSize}")
    arr = []
    start = 0
    step = int(math.ceil(l / binSize))
    stop = step
    while start < l:
        if stop >= l - 1:
            arr.append(sizeArr[start: l])
            break
        nextStart = sizeArr[stop]
        if nextStart == sizeArr[stop - 1]:
            countRight = sizeArr[stop: l].count(nextStart)
            countLeft = sizeArr[start: stop].count(nextStart)
            if countRight > countLeft:
                stop = countRight + stop
            else:
                if stop - countLeft == start:
                    stop = countRight + stop
                else:
                    stop = stop - countLeft
        arr.append(sizeArr[start:stop])
        binSize = binSize - 1
        step = int(math.ceil((l - stop) / binSize))
        start = stop
        stop = stop + step
    logging.info("Start to push results")
    # x = []
    # y = []
    counts = []
    relt = []
    for a in arr:
        counts.append((len(a)))
        relt.append([a[0], a[-1]])
        # print(len(a), a[0], a[-1])
    # print(len(sizeArr), sum([len(a) for a in arr]))
    logger.info("Computing bins end")
    return counts, relt