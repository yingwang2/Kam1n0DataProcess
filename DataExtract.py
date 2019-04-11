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
        print('Start to extract data from ' + file_path)
        f = open(file_path)
        numCol = 14
        if self.addVersion:
            numCol += 2
        try:
            insertLimitIndex = 0
            insertArr = []
            hasExecuted = False
            lineHasRead = None
            while True:
                if lineHasRead:
                    print('contains line that has been read: ', lineHasRead)
                    line = lineHasRead
                else:
                    line = f.readline()
                if not line:
                    break

                item = json.loads(line)
                function = item['function']
                sourceId = function['functionId']
                sourceName = function['functionName']
                sourceBinaryId = function['binaryId']
                sourceBinaryName = function['binaryName']
                sourceBlockSize = function['blockSize']if 'blockSize' in function else 0
                sourceCodeSize = function['codeSize'] if 'codeSize' in function else 0
                targetFunctionId = sourceId
                sourceVersion = ''
                targetVersion = ''

                line2 = f.readline()  # read line of function calls
                item2 = json.loads(line2)
                if line2 and sourceId == str(item2['functionId']):
                    callee = str(item2['callingFunctionIds'])
                else:
                    callee = ''
                    lineHasRead = line2
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
                    cloneFunctionId = targetId

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
                        similarity,
                        callee)

                    if self.addVersion:
                        m = re.search(self.pattern, targetBinaryName)
                        if m:
                            targetVersion = m.group(1)
                        row = row + (sourceVersion, targetVersion, )

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
        print('End to extract data from ' + file_path)

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
