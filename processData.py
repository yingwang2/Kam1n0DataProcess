import copy
import math

class ProcessData():
    def __init__(self, table, conn, cur, type, logger):
        self.table = table
        self.conn = conn
        self.cur = cur
        self.limit = 550
        self.logger = logger

        self.type = type
        self.field = 'codeSize' if self.type == 'Code' else 'blockSize'

        self.binsBySize = f"{table}BinsBy{type}Size"

        self.sizeTreemapName = f"{table}{type}SizeTreemap"

        self.functionsTableName = f"Functions{table}"
        self.functionsSizeTmp = f'{table}Functions{type}SizeTmp'

        self.sizeRangeTableName = f'{table}{type}SizeRange'

        self.pairSizeTemp = f'{table}Pair{type}SizeTemp'

    def getBinsCount(self):
        self.cur.execute(f'''
            create table {self.functionsSizeTmp} as
            select functionId, {self.field}, 
            B._id as binId, B.min as min, B.max as max
            from {self.functionsTableName}
            join {self.sizeRangeTableName} as B on {self.field} >= B.min and {self.field} <=B.max;
        ''')
        self.cur.execute(f'''
            select min, max, count(functionId) as count from {self.functionsSizeTmp} group by binId order by binId
        ''')
        bins = list(self.cur.fetchall())
        initialBins = []
        for bin in bins:
            initialBins.append({
                "min": bin[0],
                "max": bin[1],
                "count": bin[2]
            })
        return initialBins

    def calArr(self):
        self.logger.info("Computing bins start")
        self.cur.execute(f'SELECT {self.field} from {self.functionsTableName}')
        sizeArr = list(self.cur.fetchall())

        sizeArr = [f[0] for f in sizeArr]
        sizeArr.sort()

        l = len(sizeArr)
        binSize = max(math.floor(len(sizeArr) / self.limit), 5)

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
        self.logger.info("Start to push results")

        relt = []
        eachSizeArrStart = arr[0][0]

        i = 0
        for a in arr:
            relt.append((i, min(a[0], eachSizeArrStart), a[-1]))
            eachSizeArrStart = a[-1] + 1
            i += 1
        self.logger.info("Computing bins end")
        return relt

    def createBinsData(self):
        self.cur.execute(f'''CREATE TABLE if not exists {self.sizeRangeTableName}
                     (_id NUMERIC UNIQUE,
                     min NUMERIC,
                     max NUMERIC
                      )''')

        sizeRelt = self.calArr()

        self.cur.executemany(f"insert into {self.sizeRangeTableName} values ({'?, '* 2}?)", sizeRelt)
        self.logger.info(f'{self.type}Size Relt:{sizeRelt}')
        self.logger.info(f'Inserted into {self.sizeRangeTableName}')

        self.cur.execute(f'''
            update {self.sizeRangeTableName} set min=
            (select min(min(targetFunction{self.type}Size), min(cloneFunction{self.type}Size)) from {self.table})
            where _id=0
        ''')
        self.logger.info(f'Updated min in {self.sizeRangeTableName}')
        self.cur.execute(f'''
            update {self.sizeRangeTableName} set max=
            (select max(max(targetFunction{self.type}Size), max(cloneFunction{self.type}Size)) from {self.table})
            where _id=(select max(_id) from {self.sizeRangeTableName})
        ''')
        self.logger.info(f'Updated max in {self.sizeRangeTableName}')
        self.conn.commit()

        bins = self.getBinsCount()
        self.createBins(sizeRelt, bins)
        self.conn.commit()

    def createBins(self, sizeRelt, initialBins):
        countsSize = [i['count'] for i in initialBins]
        binSize = len(countsSize)
        id = 0
        self.logger.info(f'Bin Chart for {self.field} Size Computing Start')
        relt = []
        relt.append(initialBins)
        bins = initialBins

        i = 0

        while i < binSize - 1:
            newBins = copy.deepcopy(bins)

            diff = bins[i]["count"] - countsSize[i]

            newCurrCount = countsSize[i]
            newCurrRange = sizeRelt[i]
            newNexCount = bins[i + 1]["count"] + diff
            newNextRange = [newCurrRange[1] + 1, bins[i + 1]["max"]]

            newBins[i] = {
                "min": sizeRelt[i][0],
                "max": sizeRelt[i][1],
                "count": newCurrCount
            }
            newBins[i + 1] = {
                "min": newNextRange[0],
                "max": newNextRange[1],
                "count": newNexCount
            }
            relt.append(newBins)

            bins = copy.deepcopy(newBins)

            i += 1

        for i, stage in enumerate(relt):
            for j, bins in enumerate(stage):
                if not self.binsBySize:
                    self.cur.insert({
                        "min": bins["min"],
                        "max": bins["max"],
                        "count": bins["count"],
                        "stage": i,
                        "binIdx": j
                    })
                else:
                    self.cur.execute(f'''INSERT INTO {self.binsBySize} VALUES ('{id}', '{bins["min"]}',
                        {bins["max"]}, {bins["count"]}, {i}, {j})''')
                    id += 1

        self.logger.info(f'Bin Chart for {self.field} Size Computing End')


    def createTreemap(self):
        self.logger.info('Start to prepare treemap data')
        self.cur.execute(f'''
            create table {self.pairSizeTemp} as
            select A._id as pairId, cast(similarity * 100/ 5 as int) * 5 as simMin,
            B._id as row, B.min as targetMin, B.max as targetMax,
            C._id as col, C.min as cloneMin, C.max as cloneMax
            from {self.table} as A 
            join {self.sizeRangeTableName} as B on A.targetFunction{self.type}Size >= B.min and A.targetFunction{self.type}Size <=B.max
            join {self.sizeRangeTableName} as C on A.cloneFunction{self.type}Size >= C.min and A.cloneFunction{self.type}Size <=C.max;
        ''')
        self.conn.commit()
        self.logger.info(f'Created {self.pairSizeTemp}')

        self.cur.execute(f'''
            create table {self.sizeTreemapName} as
            select row, col, simMin,
            count(pairId) as count,
            B.min as targetMin, B.max as targetMax,
            C.min as cloneMin, C.max as cloneMax
            from {self.pairSizeTemp}
            join {self.sizeRangeTableName} as B on row=B._id
            join {self.sizeRangeTableName} as C on col=C._id
            group by
            row, col, simMin;
        ''')
        self.conn.commit()
        self.logger.info(f'Created table {self.sizeTreemapName}')
        self.cur.execute(f'alter table {self.sizeTreemapName} add column _id numeric')
        self.cur.execute(f'update {self.sizeTreemapName} set _id=rowId-1;')
        self.conn.commit()
        self.logger.info(f'Updated {self.sizeTreemapName}')

def createBinaryTreemap(conn, cur, table, logger):
    binaryIdTable = f'{table}BinaryIdTable'
    binaryTreemapTable = f"{table}BinaryTreemap"
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
    logger.info(f'Updated {binaryTreemapTable}')


def createVersionTreemap(conn, cur, table, logger):
    cur.execute(f'''
        select targetVersion as version from {table}
        union 
        select cloneVersion as version from {table}''')
    versions = list(cur.fetchall())
    versions.sort(key=lambda s: list(map(int, s[0].split('.'))))

    rows = [(i, v[0]) for i, v in enumerate(versions)]

    versionTable = f'{table}VersionTable'

    cur.execute(f'''
        create table {versionTable} (_id NUMERIC UNIQUE, version TEXT)
    ''')
    conn.commit()
    logger.info(f'Created {versionTable}')

    cur.executemany(f'insert into {versionTable} values  (?, ?)', rows)

    versionTreemapTable = f"{table}VersionTreemap"
    # cur.execute(f'''
    #     create table {versionTable} as
    #     select targetVersion as version from {table}
    #     union
    #     select cloneVersion as version from {table}
    #     order by version
    # ''')
    # conn.commit()
    # logger.info(f'Created table {versionTable}')
    #
    # cur.execute(f'alter table {versionTable} add column _id;')
    # cur.execute(f'update {versionTable} set _id=rowId-1;')
    conn.commit()
    logger.info(f'Updated {versionTable}')

    cur.execute(f'''
        create table {versionTreemapTable} as
        select A._id as row, B._id as col,
        targetVersion, cloneVersion,
        cast(similarity * 100/ 5 as int) * 5 as simMin, count(C._id) as count 
        from {table} as C
        join {versionTable} as A on A.version=targetVersion
        join {versionTable} as B on B.version=cloneVersion
        group by targetVersion, cloneVersion, simMin;
    ''')
    conn.commit()
    logger.info(f'Created table {versionTreemapTable}')

    cur.execute(f'alter table {versionTreemapTable} add column _id numeric')
    cur.execute(f'update {versionTreemapTable} set _id=rowId-1;')
    conn.commit()
    logger.info(f'Updated {versionTreemapTable}')


def indexTable(conn, cur, table, logger):
    logger.info("Indexing Start")
    cur.execute(f'CREATE INDEX targetFunctionCodeSize{table} ON {table} (targetFunctionCodeSize)')
    cur.execute(f'CREATE INDEX targetFunctionBlockSize{table} ON {table} (targetFunctionBlockSize)')
    logger.info("Indexing End")
    conn.commit()

def createFuncTable(conn, cur,functionsTableName, table, logger):
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