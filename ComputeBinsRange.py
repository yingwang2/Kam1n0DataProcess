import math
import logging

def calArr(sizeArr, binSize):
    logger = logging.getLogger()
    logger.info("Computing bins start")

    l = len(sizeArr)

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
    logger.info("Start to push results")

    relt = []
    eachSizeArrStart = arr[0][0]

    i = 0
    for a in arr:
        relt.append((i, min(a[0], eachSizeArrStart), a[-1]))
        eachSizeArrStart = a[-1] + 1
        i += 1
    logger.info("Computing bins end")
    return relt

