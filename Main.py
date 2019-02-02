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

    extractFile.col = col

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
    logger.info(f"Similarity range: {similarities}")

    binCodeSize = max(math.floor(len(codeSizeArr) / limit), 5)
    binBlockSize = max(math.floor(len(blockSizeArr) / limit), 5)

    countsCodeSize, codeSizeRelt, binCodeSize, codeSizeArr = ComputeBinsRange.calArr(codeSizeArr, binCodeSize)
    countsBlockSize, blockSizeRelt, binBlockSize, blockSizeArr = ComputeBinsRange.calArr(blockSizeArr, binBlockSize)

    CreateBinsData.createBins(db[binsByCodeSize], codeSizeRelt, countsCodeSize, binCodeSize, codeSizeArr, True)
    CreateBinsData.createBins(db[binsByBlockSize], blockSizeRelt, countsBlockSize, binBlockSize, blockSizeArr, False)

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
    functionsTableName = f"{table}Functions"

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
    ifTableExist(functionsTableName, c)

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
    conn.commit()
    logger.info("Indexing Start")
    c.execute(f'CREATE INDEX similarity{table} ON {table} (similarity)')
    c.execute(f'CREATE INDEX targetFunctionCodeSize{table} ON {table} (targetFunctionCodeSize)')
    c.execute(f'CREATE INDEX cloneFunctionCodeSize{table} ON {table} (cloneFunctionCodeSize)')
    c.execute(f'CREATE INDEX targetFunctionBlockSize{table} ON {table} (targetFunctionBlockSize)')
    c.execute(f'CREATE INDEX cloneFunctionBlockSize{table} ON {table} (cloneFunctionBlockSize)')
    logger.info("Indexing End")
    conn.commit()
    # codeSizeArr = list(extractFile.codeSizeMap.values())
    # codeSizeArr.sort()
    # blockSizeArr = list(extractFile.blockSizeMap.values())
    # blockSizeArr.sort()
    # similarities = [math.floor(extractFile.similarityRange[0] * 100), math.ceil(extractFile.similarityRange[1] * 100)]

    c.execute(f'SELECT min(similarity) from {table}')
    minSim = c.fetchone()[0]
    c.execute(f'SELECT max(similarity) from {table}')
    maxSim = c.fetchone()[0]
    similarities = [math.floor(minSim) * 100, math.ceil(maxSim) * 100]
    # binSize = max(math.floor(len(codeSizeArr) / limit), 5)
    logger.info(f'Similarity Range: {similarities}')

    c.execute(f'SELECT min(targetFunctionCodeSize) from {table}')
    minTargetCodeSize = c.fetchone()[0]
    c.execute(f'SELECT max(targetFunctionCodeSize) from {table}')
    maxTargetCodeSize = c.fetchone()[0]
    c.execute(f'SELECT min(cloneFunctionCodeSize) from {table}')
    minCloneCodeSize = c.fetchone()[0]
    c.execute(f'SELECT max(cloneFunctionCodeSize) from {table}')
    maxCloneCodeSize = c.fetchone()[0]
    codeSizeRange = [min(minTargetCodeSize, minCloneCodeSize), max(maxTargetCodeSize, maxCloneCodeSize)]
    logger.info(f'CodeSizeRange: {codeSizeRange}')

    c.execute(f'SELECT min(targetFunctionBlockSize) from {table}')
    minTargetBlockSize = c.fetchone()[0]
    c.execute(f'SELECT max(targetFunctionBlockSize) from {table}')
    maxTargetBlockSize = c.fetchone()[0]
    c.execute(f'SELECT min(cloneFunctionBlockSize) from {table}')
    minCloneBlockSize = c.fetchone()[0]
    c.execute(f'SELECT max(cloneFunctionBlockSize) from {table}')
    maxCloneBlockSize = c.fetchone()[0]
    blockSizeRange = [min(minTargetBlockSize, minCloneBlockSize), max(maxTargetBlockSize, maxCloneBlockSize)]
    logger.info(f'BlockSizeRange: {blockSizeRange}')

    q = f'''CREATE TABLE {functionsTableName} AS
        SELECT t.targetFunctionId as functionId, t.targetFunctionCodeSize as codeSize, t.targetFunctionBlockSize as blockSize 
        FROM {table} AS t 
        WHERE _id IN (SELECT _id FROM {table} ORDER BY RANDOM() LIMIT 100000)
        union
        SELECT c.cloneFunctionId as functionId, c.cloneFunctionCodeSize as codeSize, c.cloneFunctionBlockSize as blockSize 
        FROM {table} AS c 
        WHERE _id IN (SELECT _id FROM {table} ORDER BY RANDOM() LIMIT 100000)'''
    c.execute(q)
    conn.commit()
    logger.info('Sample function table created')
    c.execute(f'''SELECT * from {functionsTableName}''')
    sizeArr = list(c.fetchall())
    codeSizeArr = [f[1] for f in sizeArr]
    codeSizeArr.sort()

    blockSizeArr = [f[2] for f in sizeArr]
    blockSizeArr.sort()

    binCodeSize = max(math.floor(len(codeSizeArr) / limit), 5)
    binBlockSize = max(math.floor(len(blockSizeArr) / limit), 5)

    countsCodeSize, codeSizeRelt = ComputeBinsRange.calArr(codeSizeArr, binCodeSize)
    codeSizeRelt[0][0] = codeSizeRange[0]
    codeSizeRelt[-1][-1] = codeSizeRange[1]
    logger.info(f'CodeSizeRelt:{codeSizeRelt}')

    countsBlockSize, blockSizeRelt = ComputeBinsRange.calArr(blockSizeArr, binBlockSize)
    blockSizeRelt[0][0] = blockSizeRange[0]
    blockSizeRelt[-1][-1] = blockSizeRange[1]
    logger.info(f'BlockSizeRelt: {blockSizeRelt}')

    countsCodeSize = []
    for sizeRange in codeSizeRelt:
        c.execute(f'''SELECT COUNT(*) FROM {functionsTableName} WHERE codeSize >= {sizeRange[0]} and 
            codeSize <= {sizeRange[1]}''')
        countsCodeSize.append(c.fetchone()[0])
    logger.info(f'CountsCodeSize: {countsCodeSize}')
    countsBlockSize = []
    for sizeRange in blockSizeRelt:
        c.execute(f'''SELECT COUNT(*) FROM {functionsTableName} WHERE blockSize >= {sizeRange[0]} and 
            blockSize <= {sizeRange[1]}''')
        countsBlockSize.append(c.fetchone()[0])
    logger.info(f'CountsBlockSize: {countsBlockSize}')

    CreateBinsData.createBins(c, codeSizeRelt, countsCodeSize, binCodeSize, codeSizeArr, True, binsByCodeSize, functionsTableName)
    CreateBinsData.createBins(c, blockSizeRelt, countsBlockSize, binBlockSize, blockSizeArr, False, binsByBlockSize, functionsTableName)

    CreateTreemapData.setTreemapData(codeSizeRelt, table, True, similarities, codeSizeTreemapName, c)
    CreateTreemapData.setTreemapData(blockSizeRelt, table, False, similarities, blockSizeTreemapName, c)
    CreateTreemapData.setTreemapDataByBinary(extractFile.binaries, table, similarities, binaryTreemapName, c)

    conn.commit()
    conn.close()

logger.info('Data Process End')