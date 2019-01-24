def setTreemapData(sizeRanges, col, isCodeSize, similarities, colRelt):
    name = "FunctionCodeSize" if isCodeSize else "FunctionBlockSize"
    for i, cloneSizeRange in enumerate(sizeRanges):
        cloneStr = "$lte" if i == len(sizeRanges) else "$lt"
        cloneMin = cloneSizeRange[0]
        cloneMax = cloneSizeRange[1]
        for j, targetSizeRange in enumerate(sizeRanges):
            targetStr = "$lte" if i == len(sizeRanges) else "$lt"
            targetMin = targetSizeRange[0]
            targetMax = targetSizeRange[1]

            children = []
            count = 0

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
                                "target%s" % name: {
                                    "$gte": targetMin
                                }
                            },
                            {
                                "clone%s" % name: {
                                    "$gte": cloneMin
                                }
                            },
                            {
                                "target%s" % name: {
                                    targetStr: targetMax
                                }
                            },
                            {
                                "clone%s" % name: {
                                    cloneStr: cloneMax
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
                    },
                    {
                        "_id": 1.0
                    }
                )

                size = x.count()
                children.append({
                    "name": "Similarity: %d%% - %d%%" % (sMin*100, sMax*100),
                    "similarityRange": [sMin, sMax],
                    "count": size
                })
                count += size

            colRelt.insert({"name": "Size Range: [%d, %d]" % (i, j),
                            "rangeClone": [cloneMin, cloneMax],
                            "rangeTarget": [targetMin, targetMax],
                            "position": [i, j],
                            "count": count,
                            "similarityRange": similarities,
                            "children": children})

def setTreemapDataByBinary(binaries, col, similarities, colRelt):
    for i, cloneBinary in enumerate(binaries.values()):
        for j, targetBinary in enumerate(binaries.values()):
            children = []
            count = 0

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
                children.append({
                    "name": "Similarity: %d%% - %d%%" % (sMin * 100, sMax * 100),
                    "similarityRange": [sMin, sMax],
                    "count": size
                })
                count += size

            colRelt.insert({"name": "BinaryIds: [%s, %s]" % (targetBinary["id"], cloneBinary["id"]),
                            "targetBinary": targetBinary,
                            "cloneBinary": cloneBinary,
                            "position": [i, j],
                            "count": count,
                            "similarityRange": similarities,
                            "children": children})