import logging

def setTreemapData(sizeRanges, col, isCodeSize, similarities, colRelt):
    logger = logging.getLogger()
    name = "FunctionCodeSize" if isCodeSize else "FunctionBlockSize"
    logger.info('Code Treemap Computing Start') if isCodeSize else logger.info('Block Treemap Computing Start')
    logger.info('Size Range: %d' % len(sizeRanges))
    for i, targetSizeRange in enumerate(sizeRanges):
        targetMin = targetSizeRange[0]
        targetMax = targetSizeRange[1]
        for j, cloneSizeRange in enumerate(sizeRanges):
            logger.info("Computing index: [%d, %d]" % (i, j))
            cloneMin = cloneSizeRange[0]
            cloneMax = cloneSizeRange[1]
            results = []

            for s in range(similarities[0], similarities[1]):
                if s >= similarities[1]:
                    break

                sMin = s / 100
                sMax = (s + 1) / 100
                sStr = "$lte" if sMax == similarities[1] else "$lt"

                size = col.count_documents(
                    {
                        "$and": [
                            {
                                "target%s" % name: {
                                    "$gte": targetMin
                                }
                            },
                            {
                                "target%s" % name: {
                                    "$lte": targetMax
                                }
                            },
                            {
                                "clone%s" % name: {
                                    "$gte": cloneMin
                                }
                            },
                            {
                                "clone%s" % name: {
                                    "$lte": cloneMax
                                }
                            },
                            {
                                "similarity": {
                                    "$gte": sMin
                                }
                            },
                            {
                                "similarity": {
                                    sStr: sMax
                                }
                            }
                        ]
                    }
                )
                results.append({
                    "name": "Similarity: %d%% - %d%%" % (sMin*100, sMax*100),
                    "row": i,
                    "col": j,
                    "targetMin": targetMin,
                    "targetMax": targetMax,
                    "cloneMin": cloneMin,
                    "cloneMax": cloneMax,
                    "simMin":sMin,
                    "simMax": sMax,
                    "count": size,
                    "isRoot": False
                })

            results.append({
                "name": "Size Range: [%d, %d]" % (i, j),
                "row": i,
                "col": j,
                "targetMin": targetMin,
                "targetMax": targetMax,
                "cloneMin": cloneMin,
                "cloneMax": cloneMax,
                "simMin": similarities[0],
                "simMax": similarities[1],
                "isRoot": True
            })
            colRelt.insert_many(results)
    logger.info('Code Treemap Computing End') if isCodeSize else logger.info('Block Treemap Computing End')

def setTreemapDataByBinary(binaries, col, similarities, colRelt):
    logger = logging.getLogger()
    logger.info('Binary Treemap Computing Start')
    for i, targetBinary in enumerate(binaries.values()):
        for j, cloneBinary in enumerate(binaries.values()):
            logger.info("Computing index: [%d, %d]" % (i, j))
            results = []
            for s in range(similarities[0], similarities[1]):
                if s >= similarities[1]:
                    break

                sMin = s / 100
                sMax = (s + 1) / 100
                sStr = "$lte" if sMax == similarities[1] else "$lt"

                x = col.find(
                    {
                        "$and": [
                            {
                                "targetBinaryId": targetBinary["id"]
                            },
                            {
                                "cloneBinaryId": cloneBinary["id"]
                            },
                            {
                                "similarity": {
                                    "$gte": sMin
                                }
                            },
                            {
                                "similarity": {
                                    sStr: sMax
                                }
                            }
                        ]
                    },
                    {
                        "_id": 1.0
                    }
                )
                size = x.count()
                results.append({
                    "name": "Similarity: %d%% - %d%%" % (sMin * 100, sMax * 100),
                    "row": i,
                    "col": j,
                    "targetBinaryId": targetBinary["id"],
                    "cloneBinaryId": cloneBinary["id"],
                    "targetBinaryName": targetBinary["name"],
                    "cloneBinaryName": cloneBinary["name"],
                    "simMin": sMin,
                    "simMax": sMax,
                    "count": size,
                    "isRoot": False
                })
            results.append({
                "name": "BinaryIds: [%s, %s]" % (targetBinary["id"], cloneBinary["id"]),
                "row": i,
                "col": j,
                "targetBinaryId": targetBinary["id"],
                "cloneBinaryId": cloneBinary["id"],
                "targetBinaryName": targetBinary["name"],
                "cloneBinaryName": cloneBinary["name"],
                "simMin": similarities[0] / 100,
                "simMax": similarities[1] / 100,
                "isRoot": True
            })
            colRelt.insert_many(results)
    logger.info('Binary Treemap Computing End')
