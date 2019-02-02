import math
import logging

def calArr(sizeArr, binSize):
    # logger = logging.getLogger()
    # logger.info("Computing bins start")
    # limit = 550
    # curSizeArr = colFunc.find().sort(field, pymongo.ASCENDING)
    # logger.info("Get sorted data")
    # l = curSizeArr.count()
    # logger.info(f"Number of size data: {l}")
    # lstSizeArr = list(curSizeArr)
    # logger.info("Convert to list")
    # sizeArr = [d[field] for d in lstSizeArr]
    # logger.info("Convert to array")
    # oriBinSize = max(math.floor(l / limit), 5)
    # binSize = max(math.floor(l / limit), 5)
    # logger.info(f"Max bin size {oriBinSize}")
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
    # logger.info("Start to push results")
    # x = []
    # y = []
    counts = []
    relt = []
    eachSizeArrStart = arr[0][0]

    for a in arr:
        counts.append((len(a)))
        relt.append([min(a[0], eachSizeArrStart), a[-1]])
        eachSizeArrStart = a[-1] + 1
        # print(len(a), a[0], a[-1])
    # print(len(sizeArr), sum([len(a) for a in arr]))
    # logger.info("Computing bins end")
    return counts, relt


import sqlite3

limit = 550
conn = sqlite3.connect('Kam1n0.db')
c = conn.cursor()
table='result0131'

c.execute(f'SELECT min(targetFunctionCodeSize) from {table}')
minTargetCodeSize = c.fetchone()[0]
c.execute(f'SELECT max(targetFunctionCodeSize) from {table}')
maxTargetCodeSize = c.fetchone()[0]
c.execute(f'SELECT min(cloneFunctionCodeSize) from {table}')
minCloneCodeSize = c.fetchone()[0]
c.execute(f'SELECT max(cloneFunctionCodeSize) from {table}')
maxCloneCodeSize = c.fetchone()[0]
codeSizeRange = [min(minTargetCodeSize, minCloneCodeSize), max(maxTargetCodeSize, maxCloneCodeSize)]

c.execute(f'SELECT min(targetFunctionBlockSize) from {table}')
minTargetBlockSize = c.fetchone()[0]
c.execute(f'SELECT max(targetFunctionBlockSize) from {table}')
maxTargetBlockSize = c.fetchone()[0]
c.execute(f'SELECT min(cloneFunctionBlockSize) from {table}')
minCloneBlockSize = c.fetchone()[0]
c.execute(f'SELECT max(cloneFunctionBlockSize) from {table}')
maxCloneBlockSize = c.fetchone()[0]
blockSizeRange = [min(minTargetBlockSize, minCloneBlockSize), max(maxTargetBlockSize, maxCloneBlockSize)]

c.execute(f'''SELECT * from functions{table}''')
sizeArr = list(c.fetchall())
codeSizeArr = [f[1] for f in sizeArr]
codeSizeArr.sort()

blockSizeArr = [f[2] for f in sizeArr]
blockSizeArr.sort()
# print(len(codeSizeArr),len(blockSizeArr))
binCodeSize = max(math.floor(len(codeSizeArr) / limit), 5)
binBlockSize = max(math.floor(len(blockSizeArr) / limit), 5)

countsCodeSize, codeSizeRelt = calArr(codeSizeArr, binCodeSize)
countsBlockSize, blockSizeRelt = calArr(blockSizeArr, binBlockSize)

codeSizeRelt[0][0] = codeSizeRange[0]
codeSizeRelt[-1][-1] = codeSizeRange[1]

# print(binCodeSize)
# print(len(countsCodeSize))
# print(len(codeSizeRelt))
#
# print(binBlockSize)
# print(len(countsBlockSize))
# print(len(blockSizeRelt))

