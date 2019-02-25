import processData
import logging
import configparser
import os
import sqlite3
import DataExtract


def createTables(table, functionsTableName, addVersion):
    def ifTableExist(name, cur):
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' and name='{name}'")
        tableName = cur.fetchone()
        if tableName:
            s = f"Table {name} has already existed in database {database}, " \
                f"please modify the database or the table name in config.ini"
            logger.error(s)
            print(s)
            exit()

    def createBinsTable(binsTable):
        cur.execute(f'''CREATE TABLE if not exists {binsTable}
                     (_id TEXT UNIQUE,
                     min NUMERIC,
                     max NUMERIC,
                     count NUMERIC,
                     stage NUMERIC,
                     binIdx NUMERIC
                      )''')

    binsByCodeSize = f"{table}BinsByCodeSize"
    binsByBlockSize = f"{table}BinsByBlockSize"
    codeSizeTreemapName = f"{table}CodeSizeTreemap"
    blockSizeTreemapName = f"{table}BlockSizeTreemap"
    binaryTreemapName = f"{table}BinaryTreemap"
    codeSizeRangeTableName = f'{table}CodeSizeRange'
    blockSizeRangeTableName = f'{table}BlockSizeRange'
    pairCodeSizeTemp = f'{table}PairCodeSizeTemp'
    pairBlockSizeTemp = f'{table}PairBlockSizeTemp'
    binaryIdTable = f'{table}BinaryIdTable'
    versionTable = f'{table}VersionTable'

    ifTableExist(table, cur)
    ifTableExist(binsByCodeSize, cur)
    ifTableExist(binsByBlockSize, cur)
    ifTableExist(codeSizeTreemapName, cur)
    ifTableExist(blockSizeTreemapName, cur)
    ifTableExist(binaryTreemapName, cur)
    ifTableExist(functionsTableName, cur)
    ifTableExist(codeSizeRangeTableName, cur)
    ifTableExist(blockSizeRangeTableName, cur)
    ifTableExist(pairCodeSizeTemp, cur)
    ifTableExist(pairBlockSizeTemp, cur)
    ifTableExist(binaryIdTable, cur)
    ifTableExist(versionTable, cur)

    qCreateTable = f'''CREATE TABLE if not exists {table}
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
                  '''

    if addVersion:
        qCreateTable = qCreateTable + ',targetVersion TEXT,cloneVersion TEXT'
    qCreateTable = qCreateTable + ')'

    cur.execute(qCreateTable)

    createBinsTable(binsByCodeSize)
    createBinsTable(binsByBlockSize)


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

shouldCheck = config.get('binaryInfo', 'checkBinary') == 'True'
pattern = config.get('binaryInfo', 'binaryPattern')
addVersion = config.get('binaryInfo', 'addVersion') == 'True'

conn = sqlite3.connect(f'{database}.db')
cur = conn.cursor()

extractFile = DataExtract.Extract(cur, table, logger, pattern, addVersion)

if shouldCheck:
    for file_path in file_paths:
        if not file_path.endswith('.json'):
            continue
        extractFile.check_binary_name(dataDir + file_path)
    binaries = extractFile.binaries
    if len(binaries) > 0:
        print('Please check if the following binaries and versions are expected')
        for b in binaries:
            print(b)
    else:
        print(f'No binary is found by the regex {pattern}')
    exit()

functionsTableName = f"Functions{table}"
createTables(table, functionsTableName, addVersion)

for file_path in file_paths:
    if not file_path.endswith('.json'):
        continue
    extractFile.extract(dataDir + file_path)
conn.commit()

processData.indexTable(conn, cur, table, logger)
processData.createFuncTable(conn, cur,functionsTableName, table, logger)

codeProcessData = processData.ProcessData(table, conn, cur, 'Code', logger)
codeProcessData.createBinsData()
codeProcessData.createTreemap()

blockProcessData = processData.ProcessData(table, conn, cur, 'Block', logger)
blockProcessData.createBinsData()
blockProcessData.createTreemap()

processData.createBinaryTreemap(conn, cur, table, logger)
processData.createVersionTreemap(conn, cur, table, logger)

conn.commit()
cur.close()
conn.close()
