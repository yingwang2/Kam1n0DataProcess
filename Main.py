import DataExtract
import os
import pymongo
import math
import CreateTreemapData
import sqlite3
import configparser
import CreateBinsData
import logging
import ComputeBinsRange

limit = 550

logging.basicConfig(filename='Kam1n0DataProcess.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.info('Data process start')

def getPaths(section):
    dataDir = config.get(section, 'path')
    logger.info('Data Directory: ' + dataDir)
    file_paths = os.listdir(dataDir)
    return dataDir, file_paths

config = configparser.ConfigParser()
config.read('config.ini')

extractFile = DataExtract.Extract()
# extractFile.isMongoDB=False

if (extractFile.isMongoDB):
    dataDir, file_paths = getPaths('mongodb')
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

    # col.create_index([("similarity", pymongo.ASCENDING),("targetBinaryId", pymongo.ASCENDING),
    #                   ("cloneBinaryId", pymongo.ASCENDING),], name="binaryIdx")
    # col.create_index([("similarity", pymongo.ASCENDING), ("targetFunctionCodeSize", pymongo.ASCENDING),
    #                   ("cloneFunctionCodeSize", pymongo.ASCENDING)], name="codeIdx")
    # col.create_index([("similarity", pymongo.ASCENDING), ("targetFunctionBlockSize", pymongo.ASCENDING),
    #                   ("cloneFunctionBlockSize", pymongo.ASCENDING), ], name="blkIdx")

    logger.info("Indexing Start")
    col.create_index("similarity")
    col.create_index("targetFunctionCodeSize")
    col.create_index("cloneFunctionCodeSize")
    col.create_index("targetFunctionBlockSize")
    col.create_index("cloneFunctionBlockSize")
    logger.info("Indexing End")

    codeSizeTreemapName = "%sCodeSizeTreemap" % colName
    blockSizeTreemapName = "%sBlockSizeTreemap" % colName
    binaryTreemapName = "%sBinaryTreemap" % colName
    binsByCodeSize = "%sBinsByCodeSize" % colName
    binsByBlockSize = "%sBinsByBlockSize" % colName

    codeSizeArr = list(extractFile.codeSizeMap.values())
    codeSizeArr.sort()
    blockSizeArr = list(extractFile.blockSizeMap.values())
    blockSizeArr.sort()
    similarities = [math.floor(extractFile.similarityRange[0] * 100), math.ceil(extractFile.similarityRange[1] * 100)]

    binSize = max(math.floor(len(codeSizeArr) / limit), 5)

    countsCodeSize, codeSizeRelt = ComputeBinsRange.calArr(codeSizeArr, binSize)
    countsBlockSize, blockSizeRelt = ComputeBinsRange.calArr(blockSizeArr, binSize)

    CreateBinsData.createBins(db[binsByCodeSize], codeSizeRelt, countsCodeSize, binSize, codeSizeArr, True)
    CreateBinsData.createBins(db[binsByBlockSize], blockSizeRelt, countsBlockSize, binSize, blockSizeArr, False)

    CreateTreemapData.setTreemapData(codeSizeRelt, col, True, similarities, db[codeSizeTreemapName])
    CreateTreemapData.setTreemapData(blockSizeRelt, col, False, similarities, db[blockSizeTreemapName])
    CreateTreemapData.setTreemapDataByBinary(extractFile.binaries, col, similarities, db[binaryTreemapName])

else:
    dataDir, file_paths = getPaths('sqlite3')
    def ifTableExist(name):
        if c.fetchone()[0] != 1:
            exit()
            str = "Table %s has already existed, please modify the table name in config.ini" % name
            logger.error(str)
            print(str)

    db = config.get('sqlite3', 'database')
    table = config.get('sqlite3', 'table')

    codeSizeTreemapName = "%sCodeSizeTreemap" % table
    blockSizeTreemapName = "%sBlockSizeTreemap" % table
    binaryTreemapName = "%sBinaryTreemap" % table
    binsByCodeSize = "%sBinsByCodeSize" % table
    binsByBlockSize = "%sBinsByBlockSize" % table

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
    ifTableExist(table)

    c.execute('''CREATE TABLE if not exists %s
                 (_id NUMERIC,
                 min NUMERIC,
                 max NUMERIC,
                 count NUMERIC,
                 binIdx NUMERIC,
                 stage NUMERIC
                  )''' % binsByCodeSize)
    ifTableExist(binsByCodeSize)

    c.execute('''CREATE TABLE if not exists %s
                 (_id NUMERIC,
                 min NUMERIC,
                 max NUMERIC,
                 count NUMERIC,
                 binIdx NUMERIC,
                 stage NUMERIC
                  )''' % binsByBlockSize)
    ifTableExist(binsByBlockSize)

    c.execute('''CREATE TABLE if not exists %s
                 (name TEXT,
                 row NUMERIC,
                 col NUMERIC,
                 targetMin NUMERIC,
                 targetMax NUMERIC,
                 cloneMin NUMERIC,
                 cloneMax NUMERIC,
                 simMin NUMERIC,
                 simMax NUMERIC,
                 isRoot BLOB
                  )''' % codeSizeTreemapName)
    ifTableExist(codeSizeTreemapName)

    c.execute('''CREATE TABLE if not exists %s
                 (name TEXT,
                 row NUMERIC,
                 col NUMERIC,
                 targetMin NUMERIC,
                 targetMax NUMERIC,
                 cloneMin NUMERIC,
                 cloneMax NUMERIC,
                 simMin NUMERIC,
                 simMax NUMERIC,
                 isRoot BLOB
                  )''' % blockSizeTreemapName)
    ifTableExist(blockSizeTreemapName)

    c.execute('''CREATE TABLE if not exists %s
                 (name TEXT,
                 row NUMERIC,
                 col NUMERIC,
                 targetBinaryId TEXT,
                 cloneBinaryId TEXT,
                 targetBinaryName TEXT,
                 cloneBinaryName TEXT,
                 simMin NUMERIC,
                 simMax NUMERIC,
                 isRoot BLOB
                  )''' % binaryTreemapName)
    ifTableExist(binaryTreemapName)

    for file_path in file_paths:
        extractFile.extract(dataDir + file_path)

    conn.commit()
    conn.close()

logger.info('Data Process End')