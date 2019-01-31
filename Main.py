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

config = configparser.ConfigParser()
config.read('config.ini')

dataDir = config.get('setting', 'path')
logger.info('Data Directory: ' + dataDir)
file_paths = os.listdir(dataDir)

database = config.get('setting', 'database')
table = config.get('setting', 'table')

extractFile = DataExtract.Extract()
extractFile.isMongoDB = config.get('setting', 'type') == 'mongodb'

if (extractFile.isMongoDB):
    client = pymongo.MongoClient()
    db = client[database]
    colName = table

    rawDataName = colName
    col = db[rawDataName]
    colFunc = db[f'{rawDataName}Functions']

    extractFile.col = col
    extractFile.colFunctions = colFunc

    for file_path in file_paths:
        if not file_path.endswith('.json'):
            continue
        extractFile.extract(dataDir + file_path)

    logger.info("Indexing similarity start")
    col.create_index("similarity")

    logger.info("Indexing targetFunctionCodeSize start")
    col.create_index("targetFunctionCodeSize")

    logger.info("Indexing cloneFunctionCodeSize start")
    col.create_index("cloneFunctionCodeSize")

    logger.info("Indexing targetFunctionBlockSize start")
    col.create_index("targetFunctionBlockSize")

    logger.info("Indexing cloneFunctionBlockSize start")
    col.create_index("cloneFunctionBlockSize")

    logger.info("Indexing codeSize start")
    colFunc.create_index("codeSize")

    logger.info("Indexing blockSize start")
    colFunc.create_index("blockSize")
    logger.info("Indexing End")

    codeSizeTreemapName = f"{colName}CodeSizeTreemap"
    blockSizeTreemapName = f"{colName}BlockSizeTreemap"
    binaryTreemapName = f"{colName}BinaryTreemap"
    binsByCodeSize = f"{colName}BinsByCodeSize"
    binsByBlockSize = f"{colName}BinsByBlockSize"

    codeSizeArr = list(extractFile.codeSizeMap.values())
    codeSizeArr.sort()
    blockSizeArr = list(extractFile.blockSizeMap.values())
    blockSizeArr.sort()
    similarities = [math.floor(extractFile.similarityRange[0] * 100), math.ceil(extractFile.similarityRange[1] * 100)]

    binSize = max(math.floor(len(codeSizeArr) / limit), 5)

    countsCodeSize, codeSizeRelt = ComputeBinsRange.calArr(colFunc, "codeSize")
    countsBlockSize, blockSizeRelt = ComputeBinsRange.calArr(colFunc, "blockSize")

    CreateBinsData.createBins(db[binsByCodeSize], codeSizeRelt, countsCodeSize, binSize, codeSizeArr, True)
    CreateBinsData.createBins(db[binsByBlockSize], blockSizeRelt, countsBlockSize, binSize, blockSizeArr, False)

    CreateTreemapData.setTreemapData(codeSizeRelt, col, True, similarities, db[codeSizeTreemapName])
    CreateTreemapData.setTreemapData(blockSizeRelt, col, False, similarities, db[blockSizeTreemapName])
    CreateTreemapData.setTreemapDataByBinary(extractFile.binaries, col, similarities, db[binaryTreemapName])

else:
    def ifTableExist(name, c):
        c.execute(f"SELECT name FROM sqlite_master WHERE type='table' and name='{name}'")
        tableName = c.fetchone()
        if tableName:
            str = f"Table {name} has already existed in database {database}, please modify the table name in config.ini"
            logger.error(str)
            print(str)
            exit()

    codeSizeTreemapName = f"{table}CodeSizeTreemap"
    blockSizeTreemapName = f"{table}BlockSizeTreemap"
    binaryTreemapName = f"{table}BinaryTreemap"
    binsByCodeSize = f"{table}BinsByCodeSize"
    binsByBlockSize = f"{table}BinsByBlockSize"

    conn = sqlite3.connect(f'{database}.db')
    c = conn.cursor()
    extractFile.cur = c
    extractFile.table = table

    ifTableExist(table, c)
    ifTableExist(binsByCodeSize, c)
    ifTableExist(binsByBlockSize, c)
    ifTableExist(codeSizeTreemapName, c)
    ifTableExist(binaryTreemapName, c)
    ifTableExist(blockSizeTreemapName, c)

    c.execute(f'''CREATE TABLE if not exists {table}
                 (_id TEXT UNIQUE,
                  targetFunctionId TEXT,
                  cloneFunctionId TEXT,
                  targetBinaryId TEXT,
                  cloneBinaryId TEXT,
                  targetFunctionBlockSize INTEGER,
                  targetFunctionCodeSize INTEGER,
                  targetFunctionName TEXT,
                  cloneFunctionName TEXT,
                  targetBinaryName TEXT,
                  cloneBinaryName TEXT,
                  cloneFunctionBlockSize INTEGER,
                  cloneFunctionCodeSize INTEGER,
                  similarity NUMERIC
                  )''')

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

    c.execute(f'''CREATE TABLE if not exists {codeSizeTreemapName}
                 (_id TEXT UNIQUE,                 
                 name TEXT,
                 row NUMERIC,
                 col NUMERIC,
                 targetMin NUMERIC,
                 targetMax NUMERIC,
                 cloneMin NUMERIC,
                 cloneMax NUMERIC,
                 simMin NUMERIC,
                 simMax NUMERIC,
                 count INTEGER,
                 isRoot INTEGER
                  )''')

    c.execute(f'''CREATE TABLE if not exists {blockSizeTreemapName}
                 (_id TEXT UNIQUE,
                 name TEXT,
                 row NUMERIC,
                 col NUMERIC,
                 targetMin NUMERIC,
                 targetMax NUMERIC,
                 cloneMin NUMERIC,
                 cloneMax NUMERIC,
                 simMin NUMERIC,
                 simMax NUMERIC,
                 count INTEGER,
                 isRoot INTEGER
                  )''')

    c.execute(f'''CREATE TABLE if not exists {binaryTreemapName}
                 (_id TEXT UNIQUE,
                 name TEXT,
                 row NUMERIC,
                 col NUMERIC,
                 targetBinaryId TEXT,
                 cloneBinaryId TEXT,
                 targetBinaryName TEXT,
                 cloneBinaryName TEXT,
                 simMin NUMERIC,
                 simMax NUMERIC,
                 count INTEGER,
                 isRoot INTEGER
                  )''')


    for file_path in file_paths:
        if not file_path.endswith('.json'):
            continue
        extractFile.extract(dataDir + file_path)

    logger.info("Indexing Start")
    c.execute(f'CREATE INDEX similarity{table} ON {table} (similarity)')
    c.execute(f'CREATE INDEX targetFunctionCodeSize{table} ON {table} (targetFunctionCodeSize)')
    c.execute(f'CREATE INDEX cloneFunctionCodeSize{table} ON {table} (cloneFunctionCodeSize)')
    c.execute(f'CREATE INDEX targetFunctionBlockSize{table} ON {table} (targetFunctionBlockSize)')
    c.execute(f'CREATE INDEX cloneFunctionBlockSize{table} ON {table} (cloneFunctionBlockSize)')
    logger.info("Indexing End")

    codeSizeArr = list(extractFile.codeSizeMap.values())
    codeSizeArr.sort()
    blockSizeArr = list(extractFile.blockSizeMap.values())
    blockSizeArr.sort()
    similarities = [math.floor(extractFile.similarityRange[0] * 100), math.ceil(extractFile.similarityRange[1] * 100)]

    binSize = max(math.floor(len(codeSizeArr) / limit), 5)

    countsCodeSize, codeSizeRelt = ComputeBinsRange.calArr(codeSizeArr, binSize)
    countsBlockSize, blockSizeRelt = ComputeBinsRange.calArr(blockSizeArr, binSize)

    CreateBinsData.createBins(c, codeSizeRelt, countsCodeSize, binSize, codeSizeArr, True, binsByCodeSize)
    CreateBinsData.createBins(c, blockSizeRelt, countsBlockSize, binSize, blockSizeArr, False, binsByBlockSize)

    CreateTreemapData.setTreemapData(codeSizeRelt, table, True, similarities, codeSizeTreemapName, c)
    CreateTreemapData.setTreemapData(blockSizeRelt, table, False, similarities, blockSizeTreemapName, c)
    CreateTreemapData.setTreemapDataByBinary(extractFile.binaries, table, similarities, binaryTreemapName, c)

    conn.commit()
    conn.close()

logger.info('Data Process End')