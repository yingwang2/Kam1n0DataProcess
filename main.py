import processData
import logging
import configparser
import os
import sqlite3
import DataExtract
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler


def createTables(table, functionsTableName, addVersion):
    def ifTableExist(name, cur):
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' and name='{name}'")
        tableName = cur.fetchone()
        if tableName:
            s = f"Table {name} has already existed in database {database}, " \
                f"please modify the database or the table name in config.ini"
            logger.error(s)
            print(s)
            sys.exit(0)

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
                  similarity NUMERIC,
                  targetFunctionCallee TEXT
                  '''

    if addVersion:
        qCreateTable = qCreateTable + ',targetVersion TEXT,cloneVersion TEXT'
    qCreateTable = qCreateTable + ')'

    cur.execute(qCreateTable)

    createBinsTable(binsByCodeSize)
    createBinsTable(binsByBlockSize)

def printToConsole(s):
    now = datetime.now()
    t = now.strftime('%Y-%m-%d %H:%M:%S')
    print(f'{t} {s}')
logging.basicConfig(
                    level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[RotatingFileHandler('Kam1n0DataProcess.log',maxBytes=10*1024*1024,backupCount=10)])
logger = logging.getLogger()
logger.info('Data process start')
printToConsole('Data process start...')

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath('.'))+'/config.ini')

inputDataDir = config.get('setting', 'inputDataDir') + '/'
logger.info('Input data directory: ' + inputDataDir)
printToConsole('Input data directory: ' + inputDataDir)

outputDir = config.get('setting', 'outputDir') + '/'
if os.path.exists(outputDir):
    logger.info('Output directory: ' + outputDir)
    printToConsole('Output directory: ' + outputDir)
else:
    os.mkdir(outputDir)
    logger.info('Output directory: ' + outputDir)
    printToConsole('Output directory: ' + outputDir)

file_paths = os.listdir(inputDataDir)

database = config.get('setting', 'database')
table = config.get('setting', 'table')

shouldCheck = config.get('binaryInfo', 'checkBinaryVersion') == 'True'
pattern = config.get('binaryInfo', 'binaryPattern')
addVersion = config.get('binaryInfo', 'addVersion') == 'True'

conn = sqlite3.connect(f'{outputDir}/{database}.db')
cur = conn.cursor()

extractFile = DataExtract.Extract(cur, table, logger, pattern, addVersion)

if shouldCheck:
    for file_path in file_paths:
        if not file_path.endswith('.json'):
            continue
        extractFile.check_binary_name(inputDataDir + file_path)
    binaries = extractFile.binaries
    if len(binaries) > 0:
        print('\nPlease check if the following binaries and versions are expected. If so, you can change the "checkBinaryVersion" as False in config.ini')
        for b in binaries:
            print(b)
    else:
        print(f'No binary is found by the regex {pattern}')
    print('\n')
    sys.exit(0)

functionsTableName = f"Functions{table}"
createTables(table, functionsTableName, addVersion)

for file_path in file_paths:
    if not file_path.endswith('.json'):
        continue
    printToConsole('Start to extract data from ' + file_path)
    extractFile.extract(inputDataDir + file_path)
    printToConsole('End to extract data from ' + file_path)
conn.commit()

printToConsole('Compute data start...')
processData.indexTable(conn, cur, table, logger)
processData.createFuncTable(conn, cur,functionsTableName, table, logger)

codeProcessData = processData.ProcessData(table, conn, cur, 'Code', logger)
codeProcessData.createBinsData()
codeProcessData.createTreemap()

blockProcessData = processData.ProcessData(table, conn, cur, 'Block', logger)
blockProcessData.createBinsData()
blockProcessData.createTreemap()

processData.createBinaryTreemap(conn, cur, table, logger)
if addVersion:
    processData.createVersionTreemap(conn, cur, table, logger)

conn.commit()
cur.close()
conn.close()
printToConsole('Compute data end')
printToConsole('Data process end')
