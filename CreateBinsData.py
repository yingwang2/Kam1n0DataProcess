import math
import copy
import logging
import sqlite3
import ComputeBinsRange
import CreateTreemapData

def createBins(dbBinsBySize, sizeRelt, countsSize, binSize, sizeArr, isCodeSize, binsTable, tableFunctions):
    def getBinCount(c, binsRange, isCodeSize, tableFunctions):
        field = 'codeSize' if isCodeSize else 'blockSize'
        c.execute(f'''SELECT COUNT(*) FROM {tableFunctions} WHERE {field} >= {binsRange[0]} and 
            {field} <= {binsRange[1]}''')
        return c.fetchone()[0]
    id = 0
    # logger = logging.getLogger()
    # logger.info('Bin Chart for Code Size Computing Start') if isCodeSize else logger.info('Bin Chart for Block Size Computing Start')
    initialBins = []
    # minSize = sizeRelt[0][0]
    # maxSize = sizeRelt[-1][-1]
    # step = int(math.ceil((maxSize - minSize) / binSize))
    relt = []
    #
    # for start in range(minSize, maxSize, step):
    #     stop = start + step if start + step <= maxSize else maxSize
    #     if stop == max:
    #         count = len([x for x in sizeArr if (x >= start and x <= stop)])
    #         end = stop
    #     else:
    #         count = len([x for x in sizeArr if (x >= start and x < stop)])
    #         end = stop - 1
    #
    #     initialBins.append({
    #         "min": start,
    #         "max": end,
    #         "count": count
    #     })
    #
    # relt.append(initialBins)
    #
    # bins = initialBins

    for binRange in sizeRelt:
        count = getBinCount(dbBinsBySize, binRange, isCodeSize, tableFunctions)
        initialBins.append({
            "min": binRange[0],
            "max": binRange[1],
            "count": count
        })
    relt.append(initialBins)
    bins = initialBins

    i = 0

    while i < binSize - 1:
        newBins = copy.deepcopy(bins)

        diff = bins[i]["count"] - countsSize[i]

        newCurrCount = countsSize[i]
        newCurrRange = sizeRelt[i]
        newNexCount = bins[i+1]["count"] + diff
        newNextRange = [newCurrRange[1] + 1, bins[i+1]["max"]]

        newBins[i] = {
            "min": sizeRelt[i][0],
            "max": sizeRelt[i][1],
            "count": newCurrCount
        }
        newBins[i+1] = {
            "min": newNextRange[0],
            "max": newNextRange[1],
            "count": newNexCount
        }
        relt.append(newBins)


        bins = copy.deepcopy(newBins)

        i += 1
        print("len new bins: ",len(newBins))
    for i, stage in enumerate(relt):
        for j, bins in enumerate(stage):
            if not binsTable:
                dbBinsBySize.insert({
                    "min": bins["min"],
                    "max": bins["max"],
                    "count": bins["count"],
                    "stage": i,
                    "binIdx": j
                })
            else:
                dbBinsBySize.execute(f'''INSERT INTO {binsTable} VALUES ('{id}', '{bins["min"]}',
                    {bins["max"]}, {bins["count"]}, {i}, {j})''')
                id += 1

    # dbBinsBySize.insert({"0": relt})
    # logger.info('Bin Chart for Code Size Computing End') if isCodeSize else logger.info(
    #         'Bin Chart for Block Size Computing End')
    print(relt)


conn = sqlite3.connect('Kam1n0.db')
c = conn.cursor()
table='result0131'

codeSizeTreemapName = f"{table}CodeSizeTreemap"
blockSizeTreemapName = f"{table}BlockSizeTreemap"
binaryTreemapName = f"{table}BinaryTreemap"
binsByCodeSize = f"{table}BinsByCodeSize"
binsByBlockSize = f"{table}BinsByBlockSize"

c.execute(f'''CREATE TABLE if not exists {binsByCodeSize}
             (_id TEXT UNIQUE,
             min NUMERIC,
             max NUMERIC,
             count NUMERIC,
             stage NUMERIC,
             binIdx NUMERIC
              )''')

c.execute(f'''CREATE TABLE if not exists {binsByBlockSize}
             (_id TEXT UNIQUE,
             min NUMERIC,
             max NUMERIC,
             count NUMERIC,
             stage NUMERIC,
             binIdx NUMERIC
              )''')
conn.commit()

c.execute(f'SELECT min(similarity) from {table}')
minSim = c.fetchone()[0]
c.execute(f'SELECT max(similarity) from {table}')
maxSim = c.fetchone()[0]
similarities = [math.floor(minSim) * 100, math.ceil(maxSim) * 100]

c.execute(f'SELECT min(targetFunctionBlockSize) from {table}')
minTargetBlockSize = c.fetchone()[0]
c.execute(f'SELECT max(targetFunctionBlockSize) from {table}')
maxTargetBlockSize = c.fetchone()[0]
c.execute(f'SELECT min(cloneFunctionBlockSize) from {table}')
minCloneBlockSize = c.fetchone()[0]
c.execute(f'SELECT max(cloneFunctionBlockSize) from {table}')
maxCloneBlockSize = c.fetchone()[0]
blockSizeRange = [min(minTargetBlockSize, minCloneBlockSize), max(maxTargetBlockSize, maxCloneBlockSize)]

c.execute(f'SELECT min(targetFunctionCodeSize) from {table}')
minTargetCodeSize = c.fetchone()[0]
c.execute(f'SELECT max(targetFunctionCodeSize) from {table}')
maxTargetCodeSize = c.fetchone()[0]
c.execute(f'SELECT min(cloneFunctionCodeSize) from {table}')
minCloneCodeSize = c.fetchone()[0]
c.execute(f'SELECT max(cloneFunctionCodeSize) from {table}')
maxCloneCodeSize = c.fetchone()[0]
codeSizeRange = [min(minTargetCodeSize, minCloneCodeSize), max(maxTargetCodeSize, maxCloneCodeSize)]

c.execute(f'''SELECT * from functions{table}''')
sizeArr = list(c.fetchall())

codeSizeArr = [f[1] for f in sizeArr]
codeSizeArr.sort()

blockSizeArr = [f[2] for f in sizeArr]
blockSizeArr.sort()

limit = 550

binCodeSize = max(math.floor(len(codeSizeArr) / limit), 5)
countsCodeSize, codeSizeRelt = ComputeBinsRange.calArr(codeSizeArr, binCodeSize)
codeSizeRelt[0][0] = codeSizeRange[0]
codeSizeRelt[-1][-1] = codeSizeRange[1]
createBins(c, codeSizeRelt, countsCodeSize, len(countsCodeSize), codeSizeArr, True, binsByCodeSize, f'functions{table}')

binBlockSize = max(math.floor(len(blockSizeArr) / limit), 5)
countsBlockSize, blockSizeRelt = ComputeBinsRange.calArr(blockSizeArr, binBlockSize)
blockSizeRelt[0][0] = blockSizeRange[0]
blockSizeRelt[-1][-1] = blockSizeRange[1]
createBins(c, blockSizeRelt, countsBlockSize, len(countsBlockSize), blockSizeArr, False, binsByBlockSize, f'functions{table}')

CreateTreemapData.setTreemapData(codeSizeRelt, table, True, similarities, codeSizeTreemapName, c)
CreateTreemapData.setTreemapData(blockSizeRelt, table, False, similarities, blockSizeTreemapName, c)
# CreateTreemapData.setTreemapDataByBinary(extractFile.binaries, table, similarities, binaryTreemapName, c)


conn.commit()
conn.close()