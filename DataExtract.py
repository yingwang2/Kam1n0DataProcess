import json
import traceback
import math
import logging

class Extract():
    isMongoDB = True
    binaryIndexMap = {}
    binaries = dict()
    codeSizeMap = {}
    blockSizeMap = {}
    similarityRange = [math.inf, -math.inf]
    col = None
    cur = None
    table = None
    logger = logging.getLogger()
    id = 0
    colFunctions = None


    def setCodeSize(self, id, codeSize):
        if not id in self.codeSizeMap:
            self.codeSizeMap[id] = codeSize

    def setBlockSize(self, id, blockSize):
        if not id in self.blockSizeMap:
            self.blockSizeMap[id] = blockSize

    def setSimilarityRange(self, similarity):
        self.similarityRange[0] = min(self.similarityRange[0], similarity)
        self.similarityRange[1] = max(self.similarityRange[1], similarity)

    def addBinary(self, binaryId, binaryName):
        if (binaryId not in self.binaries.keys()):
            self.binaries[binaryId] = ({"name": binaryName, "id": binaryId})


    def extract(self, file_path):
        self.logger.info('File Path: ' + file_path)
        self.logger.info('Data Extract Start')
        f = open(file_path)
        try:
            i = 0
            for line in f:
                if i == 3:
                    return
                i = i + 1
                item = json.loads(line)
                function = item['function']
                sourceId = function['functionId']
                sourceName = function['functionName']
                sourceBinaryId = function['binaryId']
                sourceBinaryName = function['binaryName']
                sourceBlockSize = function['blockSize']if 'blockSize' in function else 0
                sourceCodeSize = function['codeSize'] if 'codeSize' in function else 0
                targetFunctionId = "%s%s" % (sourceBinaryId, sourceId)

                self.addBinary(sourceBinaryId, sourceBinaryName)
                self.setCodeSize(targetFunctionId, sourceCodeSize)
                self.setBlockSize(targetFunctionId, sourceBlockSize)

                for clone in item['clones']:
                    targetId = clone['functionId']
                    targetName = clone['functionName']
                    targetBinaryId = clone['binaryId']
                    targetBinaryName = clone['binaryName']
                    similarity = clone['similarity']
                    targetBlockSize = clone['numBbs'] if 'numBbs' in clone else 0
                    targetCodeSize = clone['codeSize'] if 'codeSize' in clone else 0
                    cloneFunctionId = "%s%s" % (targetBinaryId, targetId)

                    self.addBinary(targetBinaryId, targetBinaryName)
                    self.setCodeSize(cloneFunctionId, targetCodeSize)
                    self.setBlockSize(cloneFunctionId, targetBlockSize)
                    self.setSimilarityRange(similarity)

                    if self.isMongoDB:
                        self.col.insert({"targetFunctionId": targetFunctionId,
                                    "cloneFunctionId": cloneFunctionId,
                                    "targetBinaryId": sourceBinaryId,
                                    "cloneBinaryId": targetBinaryId,
                                    "targetFunctionBlockSize": sourceBlockSize,
                                    "targetFunctionCodeSize": sourceCodeSize,
                                    "targetFunctionName": sourceName,
                                    "cloneFunctionName": targetName,
                                    "targetBinaryName": sourceBinaryName,
                                    "cloneBinaryName": targetBinaryName,
                                    "cloneFunctionBlockSize": targetBlockSize,
                                    "cloneFunctionCodeSize": targetCodeSize,
                                    "similarity": similarity
                                    })
                        self.colFunctions.update({"_id": targetFunctionId},
                                                 {"codeSize": sourceCodeSize, "blockSize": sourceBlockSize},
                                                 upsert=True)
                        self.colFunctions.update({'_id': cloneFunctionId},
                                                {"codeSize": targetCodeSize, "blockSize": targetBlockSize},
                                                 upsert=True)
                    else:
                        self.cur.execute('''
                            INSERT INTO %s VALUES 
                            ('%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')
                        ''' % (self.table,
                               self.id,
                               ("%s%s" % (sourceBinaryId, sourceId)),
                               "%s%s" % (targetBinaryId, targetId),
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
                               similarity
                               ))
                        self.id += 1

        except Exception:
            self.logger.error('Data Extract Error: ' + traceback.format_exc())
            exit()
        finally:
            f.close()
        self.logger.info('Data Extract End')
