import json
import traceback
import re


class Extract:
    def __init__(self, cur, table, logger, pattern, addVersion):
        self.cur = cur
        self.table = table
        self.logger = logger
        self.id = 0
        self.binaries = set()
        self.pattern = pattern
        self.addVersion = addVersion

    def extract(self, file_path):
        self.logger.info('File Path: ' + file_path)
        self.logger.info('Data Extract Start')
        f = open(file_path)
        try:
            insertLimitIndex = 0
            insertArr = []
            hasExecuted = False
            for line in f:
                item = json.loads(line)
                function = item['function']
                sourceId = function['functionId']
                sourceName = function['functionName']
                sourceBinaryId = function['binaryId']
                sourceBinaryName = function['binaryName']
                sourceBlockSize = function['blockSize']if 'blockSize' in function else 0
                sourceCodeSize = function['codeSize'] if 'codeSize' in function else 0
                targetFunctionId = "%s%s" % (sourceBinaryId, sourceId)
                sourceVersion = ''
                targetVersion = ''

                if self.addVersion:
                    m = re.search(self.pattern, sourceBinaryName)
                    if m:
                        sourceVersion = m.group(1)

                for clone in item['clones']:
                    targetId = clone['functionId']
                    targetName = clone['functionName']
                    targetBinaryId = clone['binaryId']
                    targetBinaryName = clone['binaryName']
                    similarity = clone['similarity']
                    targetBlockSize = clone['numBbs'] if 'numBbs' in clone else 0
                    targetCodeSize = clone['codeSize'] if 'codeSize' in clone else 0
                    cloneFunctionId = "%s%s" % (targetBinaryId, targetId)

                    row = (self.id,
                        targetFunctionId,
                        cloneFunctionId,
                        sourceBinaryId,
                        targetBinaryId,
                        sourceBlockSize,
                        sourceCodeSize,
                        sourceName,
                        targetName,
                        sourceBinaryName,
                        targetBinaryName,
                        targetBlockSize,
                        targetCodeSize,
                        similarity)

                    numCol = 13
                    if self.addVersion:
                        m = re.search(self.pattern, targetBinaryName)
                        if m:
                            targetVersion = m.group(1)
                        row = row + (sourceVersion, targetVersion, )
                        numCol = 15

                    insertArr.append(row)
                    insertLimitIndex += 1
                    if insertLimitIndex >= 10000:
                        self.cur.executemany(f"insert into {self.table} values ({'?, ' * numCol}?)", insertArr)
                        self.logger.info(f'Inserted {insertLimitIndex} rows')
                        insertArr = []
                        hasExecuted = True
                        insertLimitIndex = 0
                    self.id += 1
            if not hasExecuted or len(insertArr) > 0:
                self.cur.executemany(f"insert into {self.table} values ({'?, '* numCol}?)", insertArr)
                self.logger.info(f'Inserted {insertLimitIndex} left rows')
            self.logger.info(f'Totol rows: {self.id}')
        except Exception:
            self.logger.error('Data Extract Error: ' + traceback.format_exc())
            print('Data Extract Error: ' + traceback.format_exc())
            exit()
        finally:
            f.close()
        self.logger.info('Data Extract End')

    def check_binary_name(self, file_path):
        regex = re.compile('"binaryName":' + '"(' + self.pattern + ')"')
        f = open(file_path)
        try:
            for line in f:
                for match in regex.finditer(line):
                    self.binaries.add(f'binary: {match.group(1)}, version: {match.group(2)}')
        except Exception:
            self.logger.error('Data Extract Error: ' + traceback.format_exc())
            print('Data Extract Error: ' + traceback.format_exc())
            exit()
        finally:
            f.close()
