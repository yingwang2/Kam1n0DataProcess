import json
import traceback
import math

class Extract():
    isMongoDB = True
    id = 0
    binaryIndexMap = {}
    binaries = dict()
    codeSizeMap = {}
    blockSizeMap = {}
    similarityRange = [math.inf, -math.inf]
    col = None
    conn = None

    # def setBinaryIndex(self, binaryId):
    #     if  not binaryId in self.binaryIndexMap:
    #         index = len(self.binaryIndexMap)
    #         self.binaryIndexMap[binaryId] = index

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
        f = open(file_path)
        try:
            # i = 0
            for line in f:
                # if i == 10:
                #     return
                # i = i + 1
                item = json.loads(line)
                function = item['function']
                sourceId = function['functionId']
                sourceName = function['functionName']
                sourceBinaryId = function['binaryId']
                sourceBinaryName = function['binaryName']
                sourceBlockSize = function['blockSize']if 'blockSize' in function else 0
                sourceCodeSize = function['codeSize'] if 'codeSize' in function else 0
                targetFunctionId = "%s%s" % (sourceBinaryId, sourceId)

                # self.setBinaryIndex(sourceBinaryId)
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
                    # self.setBinaryIndex(targetBinaryId)
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
                    else:
                        self.conn.execute('''
                            INSERT INTO dataFromSteven VALUES 
                            ('%d', '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')
                        ''' % (self.id,
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
            print(traceback.format_exc())
        finally:
            f.close()

