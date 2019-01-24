import DataExtract
import os
import pymongo
import math
import CreateTreemapData
import sqlite3
import configparser
import CreateBinsData


limit = 550

def calArr(sizeArr, binSize):
    arr = []
    l = len(sizeArr)

    start = 0
    step = int(math.ceil(l / binSize))
    stop = step
    while start < l:
        if stop >= l - 1:
            arr.append(sizeArr[start:l])
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

    # x = []
    # y = []
    counts = []
    relt = []
    for a in arr:
        # x.append("[%d,%d]" % (a[0], a[-1]))
        # y.append(len(a))
        counts.append((len(a)))
        relt.append([a[0], a[-1]])
        print(len(a), a[0], a[-1])
    print(len(sizeArr), sum([len(a) for a in arr]))

    return counts, relt

extractFile = DataExtract.Extract()
dataDir = './DataSteven/'
file_paths = os.listdir(dataDir)

config = configparser.ConfigParser()
config.read('config.ini')

# extractFile.isMongoDB=False

if (extractFile.isMongoDB):
    client = pymongo.MongoClient()

    db = client[config.get('mongodb', 'database')]
    colName = config.get('mongodb', 'collection')

    rawDataName = colName
    col = db[rawDataName]

    extractFile.col = col

    for file_path in file_paths:
        if not file_path.endswith('.json'):
            continue
        extractFile.extract(dataDir + file_path)

    col.create_index([("targetBinaryId", pymongo.ASCENDING), ("cloneBinaryId", pymongo.ASCENDING),
                      ("targetFunctionBlockSize", pymongo.ASCENDING), ("targetFunctionCodeSize", pymongo.ASCENDING),
                      ("cloneFunctionBlockSize", pymongo.ASCENDING), ("cloneFunctionCodeSize", pymongo.ASCENDING),
                      ("similarity", pymongo.ASCENDING)], name="index")

    codeSizeTreemapName = "%sCodeSizeTreemap" % colName
    blockSizeTreemapName = "%sBlockSizeTreemap" % colName
    binaryTreemapName = "%sBinaryTreemap" % colName
    binsByCodeSize = "%sBinsByCodeSize" % colName
    binsByBlockSize = "%sBinsByBlockSize" % colName

    # col_description = db[colName]
    # col_description.insert({
    #     "rawdata": rawDataName,
    #     "Code Size": codeSizeTreemapName,
    #     "Block Size": blockSizeTreemapName,
    #     "Binaries": binaryTreemapName,
    #     "binsByCodeSize": binsByCodeSize,
    #     "binsByBlockSize": binsByBlockSize
    # })

    codeSizeArr = list(extractFile.codeSizeMap.values())
    codeSizeArr.sort()
    blockSizeArr = list(extractFile.blockSizeMap.values())
    blockSizeArr.sort()
    similarities = [math.floor(extractFile.similarityRange[0] * 100), math.ceil(extractFile.similarityRange[1] * 100)]

    binSize = max(math.floor(len(codeSizeArr) / limit), 5)

    countsCodeSize, codeSizeRelt = calArr(codeSizeArr, binSize)
    CreateTreemapData.setTreemapData(codeSizeRelt, col, True, similarities, db[codeSizeTreemapName])

    countsBlockSize, blockSizeRelt = calArr(blockSizeArr, binSize)
    CreateTreemapData.setTreemapData(blockSizeRelt, col, False, similarities, db[blockSizeTreemapName])

    CreateTreemapData.setTreemapDataByBinary(extractFile.binaries, col, similarities, db[binaryTreemapName])

    CreateBinsData.createBins(db[binsByCodeSize], codeSizeRelt, countsCodeSize, binSize, codeSizeArr)
    CreateBinsData.createBins(db[binsByBlockSize], blockSizeRelt, countsBlockSize, binSize, blockSizeArr)

else:
    db = config.get('sqlite3', 'database')
    table = config.get('sqlite3', 'table')

    conn = sqlite3.connect('%s.db'%db)
    extractFile.conn = conn
    c = conn.cursor()
    c.execute('''CREATE TABLE if not exists %s
                 (_id NUMERIC,
                  targetFunctionId TEXT,
                  cloneFunctionId TEXT,
                  targetBinaryId TEXT,
                  cloneBinaryId TEXT,
                  targetFunctionBlockSize NUMERIC,
                  targetFunctionCodeSize NUMERIC,
                  targetFunctionName TEXT,
                  cloneFunctionName TEXT,
                  targetBinaryName TEXT,
                  cloneBinaryName TEXT,
                  cloneFunctionBlockSize NUMERIC,
                  cloneFunctionCodeSize NUMERIC,
                  similarity NUMERIC
                  )''' % table)

    for file_path in file_paths:
        extractFile.extract(dataDir + file_path)

    conn.commit()
    conn.close()
