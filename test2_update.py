import sqlite3
import configparser
import logging
import os
import math
import ComputeBinsRange
import CreateBinsData
import DataExtract

def getBinsCount(cur, funcTable, tmpTable, binTable, field):
    cur.execute(f'''
        create table {tmpTable} as
        select functionId, {field}, 
        B._id as binId, B.min as min, B.max as max
        from {funcTable}
        join {binTable} as B on {field} >= B.min and {field} <=B.max;
    ''')
    cur.execute(f'''
        select min, max, count(functionId) as count from {tmpTable} group by binId order by binId
    ''')
    bins = list(cur.fetchall())
    initialBins = []
    for bin in bins:
        initialBins.append({
            "min": bin[0],
            "max": bin[1],
            "count": bin[2]
        })
    return initialBins

def createBins(conn, cur, tableSizeRange, sizeArr, bin, isCodeSize, functionsTableName, binsTable):
    type = 'CodeSize' if isCodeSize else 'BlockSize'
    field = 'codeSize' if isCodeSize else 'blockSize'
    cur.execute(f'''CREATE TABLE if not exists {tableSizeRange}
                 (_id NUMERIC UNIQUE,
                 min NUMERIC,
                 max NUMERIC
                  )''')

    sizeRelt = ComputeBinsRange.calArr(sizeArr, bin)

    cur.executemany(f"insert into {tableSizeRange} values ({'?, '* 2}?)", sizeRelt)
    logger.info(f'{type} Relt:{sizeRelt}')
    logger.info(f'Inserted into {tableSizeRange}')

    cur.execute(f'''
        update {tableSizeRange} set min=
        (select min(min(targetFunction{type}), min(cloneFunction{type})) from {table})
        where _id=0
    ''')
    logger.info(f'Updated min in {tableSizeRange}')
    cur.execute(f'''
        update {tableSizeRange} set max=
        (select max(max(targetFunction{type}), max(cloneFunction{type})) from {table})
        where _id=(select max(_id) from {tableSizeRange})
    ''')
    logger.info(f'Updated max in {tableSizeRange}')
    conn.commit()

    bins = getBinsCount(cur, functionsTableName, f'func{type}Tmp', tableSizeRange, field)
    CreateBinsData.createBins(cur, sizeRelt, isCodeSize, binsTable, bins)
    conn.commit()

def createBinsTable(binsTable):
    cur.execute(f'''CREATE TABLE if not exists {binsTable}
                 (_id TEXT UNIQUE,
                 min NUMERIC,
                 max NUMERIC,
                 count NUMERIC,
                 stage NUMERIC,
                 binIdx NUMERIC
                  )''')

def createTreemap(conn, cur, pairSizeTemp, table, sizeRangeTableName, isCodeSize, treemapTableName):
    type = 'CodeSize' if isCodeSize else 'BlockSize'

    logging.info('Start to prepare treemap data')
    cur.execute(f'''
        create table {pairSizeTemp} as
        select A._id as pairId, cast(similarity * 100/ 5 as int) * 5 as simMin,
        B._id as row, B.min as targetMin, B.max as targetMax,
        C._id as col, C.min as cloneMin, C.max as cloneMax
        from {table} as A 
        join {sizeRangeTableName} as B on A.targetFunction{type} >= B.min and A.targetFunction{type} <=B.max
        join {sizeRangeTableName} as C on A.cloneFunction{type} >= C.min and A.cloneFunction{type} <=C.max;
    ''')
    conn.commit()
    logging.info(f'Created {pairSizeTemp}')

    cur.execute(f'''
        create table {treemapTableName} as
        select row, col, simMin,
        count(pairId) as count,
        B.min as targetMin, B.max as targetMax,
        C.min as cloneMin, C.max as cloneMax
        from {pairSizeTemp}
        join {sizeRangeTableName} as B on row=B._id
        join {sizeRangeTableName} as C on col=C._id
        group by
        row, col, simMin;
    ''')
    conn.commit()
    logging.info(f'Created table {treemapTableName}')
    cur.execute(f'alter table {treemapTableName} add column _id numeric')
    cur.execute(f'update {treemapTableName} set _id=rowId-1;')
    conn.commit()
    logging.info(f'Updated {treemapTableName}')

def createBinaryTreemap(conn, cur, binaryIdTable, table, binaryTreemapTable):
    cur.execute(f'''
        create table {binaryIdTable} as
        select targetBinaryId as binaryId, targetBinaryName as binaryName from {table}
        union 
        select cloneBinaryId as binaryId, cloneBinaryName as binaryName from {table}
        order by binaryName
    ''')
    conn.commit()
    logger.info(f'Created table {binaryIdTable}')

    cur.execute(f'alter table {binaryIdTable} add column _id;')
    cur.execute(f'update {binaryIdTable} set _id=rowId-1;')
    conn.commit()
    logger.info(f'Updated {binaryIdTable}')

    cur.execute(f'''
        create table {binaryTreemapTable} as
        select A._id as row, B._id as col,
        targetBinaryName, cloneBinaryName, targetBinaryId, cloneBinaryId, 
        cast(similarity * 100/ 5 as int) * 5 as simMin, count(C._id) as count 
        from {table} as C
        join {binaryIdTable} as A on A.binaryId=targetBinaryId
        join {binaryIdTable} as B on B.binaryId=cloneBinaryId
        group by targetBinaryId, cloneBinaryId, simMin;
    ''')
    conn.commit()
    logger.info(f'Created table {binaryTreemapTable}')

    cur.execute(f'alter table {binaryTreemapTable} add column _id numeric')
    cur.execute(f'update {binaryTreemapTable} set _id=rowId-1;')
    conn.commit()
    logging.info(f'Updated {binaryTreemapTable}')

def ifTableExist(name, c):
    c.execute(f"SELECT name FROM sqlite_master WHERE type='table' and name='{name}'")
    tableName = c.fetchone()
    if tableName:
        str = f"Table {name} has already existed in database {database}, please modify the table name in config.ini"
        logger.error(str)
        print(str)
        exit()

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

binsByCodeSize = f"{table}BinsByCodeSize"
binsByBlockSize = f"{table}BinsByBlockSize"
codeSizeTreemapName = f"{table}CodeSizeTreemap"
blockSizeTreemapName = f"{table}BlockSizeTreemap"
binaryTreemapName = f"{table}BinaryTreemap"
functionsTableName = f"Functions{table}"
codeSizeRangeTableName = f'{table}CodeSizeRange'
blockSizeRangeTableName = f'{table}BlockSizeRange'
pairCodeSizeTemp = f'{table}PairCodeSizeTemp'
pairBlockSizeTemp = f'{table}PairBlockSizeTemp'
binaryIdTable = f'{table}BinaryIdTable'

conn = sqlite3.connect(f'{database}.db')
cur = conn.cursor()

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

cur.execute(f'''CREATE TABLE if not exists {table}
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

createBinsTable(binsByCodeSize)
createBinsTable(binsByBlockSize)

extractFile = DataExtract.Extract()
extractFile.isMongoDB = config.get('setting', 'type') == 'mongodb'
extractFile.cur = cur
extractFile.table = table

for file_path in file_paths:
    if not file_path.endswith('.json'):
        continue
    extractFile.extract(dataDir + file_path)
conn.commit()

logger.info("Indexing Start")
cur.execute(f'CREATE INDEX targetFunctionCodeSize{table} ON {table} (targetFunctionCodeSize)')
cur.execute(f'CREATE INDEX targetFunctionBlockSize{table} ON {table} (targetFunctionBlockSize)')
logger.info("Indexing End")
conn.commit()

cur.execute(f'''CREATE TABLE {functionsTableName} AS
    SELECT t.targetFunctionId as functionId, t.targetFunctionCodeSize as codeSize, t.targetFunctionBlockSize as blockSize 
    FROM {table} AS t 
    WHERE _id IN (SELECT _id FROM {table} ORDER BY RANDOM() LIMIT 100000)
    union
    SELECT c.cloneFunctionId as functionId, c.cloneFunctionCodeSize as codeSize, c.cloneFunctionBlockSize as blockSize 
    FROM {table} AS c 
    WHERE _id IN (SELECT _id FROM {table} ORDER BY RANDOM() LIMIT 100000)''')
conn.commit()
logger.info('Sample function table created')

cur.execute(f'SELECT codeSize, blockSize from {functionsTableName}')
sizeArr = list(cur.fetchall())

# create code size bins table
codeSizeArr = [f[0] for f in sizeArr]
codeSizeArr.sort()
binCodeSize = max(math.floor(len(codeSizeArr) / limit), 5)
createBins(conn, cur, codeSizeRangeTableName, codeSizeArr, binCodeSize, True, functionsTableName, binsByCodeSize)

cur.execute(f'alter table {table} add column simMin')
cur.execute(f'alter table {table} add targetCodeSizeRangeId')
cur.execute(f'alter table {table} add cloneCodeSizeRangeId')

cur.execute(f'''
update {table}
set targetCodeSizeRangeId=
(select _id  from {codeSizeRangeTableName} where targetFunctionCodeSize >= min and targetFunctionCodeSize <=max),
cloneCodeSizeRangeId=
(select _id  from {codeSizeRangeTableName} where cloneFunctionCodeSize >= min and cloneFunctionCodeSize <=max),
simMin=cast(similarity * 100/ 5 as int) * 5
''')
conn.commit()
logger.info('Updated range ids')

cur.execute(f'''
create table {codeSizeTreemapName} as
select targetCodeSizeRangeId as row, cloneCodeSizeRangeId as col, simMin, count(A._id) as count,
B.min as targetMin, B.max as targetMax,
C.min as cloneMin, C.max as cloneMax
from {table} as A
join {codeSizeRangeTableName} as B on targetCodeSizeRangeId=B._id
join {codeSizeRangeTableName} as C on cloneCodeSizeRangeId=C._id
group by 
targetCodeSizeRangeId, cloneCodeSizeRangeId, simMin
''')
conn.commit()
logger.info(f'Created {codeSizeTreemapName}')

logging.info(f'Created table {codeSizeTreemapName}')
cur.execute(f'alter table {codeSizeTreemapName} add column _id numeric')
cur.execute(f'update {codeSizeTreemapName} set _id=rowId-1;')
conn.commit()
logging.info(f'Updated {codeSizeTreemapName}')

logger.info('end')

cur.close()
conn.close()