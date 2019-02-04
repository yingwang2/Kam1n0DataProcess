import logging

def setTreemapData(sizeRanges, col, isCodeSize, similarities, colRelt, cur=None):
    id = 0
    logger = logging.getLogger()
    name = "FunctionCodeSize" if isCodeSize else "FunctionBlockSize"
    indexName = "codeSize" if isCodeSize else "blockSize"
    logger.info('Code Treemap Computing Start') if isCodeSize else logger.info('Block Treemap Computing Start')
    logger.info('Size Range: %d' % len(sizeRanges))
    insertLimitIndex = 0
    insertArr = []
    hasExecuted = False
    simStep = 5
    for i, targetSizeRange in enumerate(sizeRanges):
        targetMin = targetSizeRange[0]
        targetMax = targetSizeRange[1]
        for j, cloneSizeRange in enumerate(sizeRanges):
            logger.info("Computing index: [%d, %d]" % (i, j))
            cloneMin = cloneSizeRange[0]
            cloneMax = cloneSizeRange[1]
            results = []

            for s in range(similarities[0], similarities[1] + simStep, simStep):
                logger.info(f'At step {i, j, s}')
                if s >= similarities[1]:
                    break

                sMin = s / 100
                sMax = (s + simStep) / 100


                if not cur:
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
                else:
                    sStr = "<=" if sMax == similarities[1] else "<"
                    sTarget = f'target{name} >= {targetMin} and target{name} <= {targetMax} and '
                    if targetMin == targetMax:
                        sTarget = f'target{name} = {targetMin} and'
                    sClone = f'clone{name} >= {cloneMin} and clone{name} <= {cloneMax} and'
                    if cloneMin == cloneMax:
                        sClone = f'clone{name} = {cloneMin} and'
                    cur.execute(f'''
                        SELECT COUNT(*) FROM {col} INDEXED BY {indexName}{col} WHERE 
                        {sTarget}
                        {sClone}
                        similarity >= {sMin} and 
                        similarity {sStr} {sMax}
                    ''')
                    size = cur.fetchone()[0]
                    insertArr.append(
                        (id,
                         f'Similarity: {sMin*100}% - {sMax*100}%',
                         i,
                         j,
                         targetMin,
                         targetMax,
                         cloneMin,
                         cloneMax,
                         sMin,
                         sMax,
                         size,
                         0)
                    )
                    insertLimitIndex += 1
                    if insertLimitIndex >= 10000 or len(insertArr) > 0:
                        cur.executemany(f"insert into {colRelt} values ({'?, '* 11}?)", insertArr)
                        insertArr = []
                        hasExecuted = True
                        insertLimitIndex = 0
                    # cur.execute(f'''
                    #     INSERT INTO {colRelt} VALUES ('{id}', 'Similarity: {sMin*100}% - {sMax*100}%', '{i}', '{j}',
                    #     '{targetMin}', '{targetMax}', '{cloneMin}', '{cloneMax}', '{sMin}', '{sMax}', '{size}', '{0}')
                    # ''')
                    id += 1
            if not cur:
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
            else:
                insertArr.append(
                    (id,
                     f'Size Range: [{i}, {j}]',
                     i,
                     j,
                     targetMin,
                     targetMax,
                     cloneMin,
                     cloneMax,
                     similarities[0],
                     similarities[1],
                     '',
                     1)
                )
                # cur.execute(f'''
                #     INSERT INTO {colRelt} VALUES ('{id}', 'Size Range: [{i}, {j}]', '{i}', '{j}',
                #     '{targetMin}', '{targetMax}', '{cloneMin}', '{cloneMax}', '{similarities[0]}', '{similarities[1]}', '', '{1}')
                # ''')
                insertLimitIndex += 1
                id += 1
    if not hasExecuted or len(insertArr) > 0:
        cur.executemany(f"insert into {colRelt} values ({'?, '* 11}?)", insertArr)
    logger.info('Code Treemap Computing End') if isCodeSize else logger.info('Block Treemap Computing End')

def setTreemapDataByBinary(binaries, col, similarities, colRelt, cur=None):
    id = 0
    logger = logging.getLogger()
    logger.info('Binary Treemap Computing Start')
    insertLimitIndex = 0
    insertArr = []
    hasExecuted = False
    for i, targetBinary in enumerate(binaries.values()):
        for j, cloneBinary in enumerate(binaries.values()):
            logger.info("Computing index: [%d, %d]" % (i, j))
            results = []
            for s in range(similarities[0], similarities[1]):
                if s >= similarities[1]:
                    break

                sMin = s / 100
                sMax = (s + 1) / 100
                if not cur:
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
                else:
                    sStr = "<=" if sMax == similarities[1] else "<"
                    cur.execute(f'''
                        SELECT COUNT(*) FROM {col} INDEXED BY binary{col} WHERE 
                        targetBinaryId = {targetBinary["id"]} and 
                        cloneBinaryId = {cloneBinary["id"]} and 
                        similarity >= {sMin} and 
                        similarity {sStr} {sMax}
                    ''')
                    size = cur.fetchone()[0]
                    insertArr.append(
                        (id,
                         f'Similarity: {sMin*100}% - {sMax*100}%',
                         i,
                         j,
                         targetBinary["id"],
                         cloneBinary["id"],
                         targetBinary["name"],
                         cloneBinary["name"],
                         sMin,
                         sMax,
                         size,
                         0)
                    )
                    insertLimitIndex += 1
                    if insertLimitIndex >= 10000 or len(insertArr) > 0:
                        cur.executemany(f"insert into {colRelt} values ({'?, '* 11}?)", insertArr)
                        insertArr = []
                        hasExecuted = True
                        insertLimitIndex = 0
                    # cur.execute(f'''
                    #     INSERT INTO {colRelt} VALUES ('{id}', 'Similarity: {sMin*100}% - {sMax*100}%', '{i}', '{j}',
                    #     '{targetBinary["id"]}', '{cloneBinary["id"]}', '{targetBinary["name"]}', '{cloneBinary["name"]}',
                    #     '{sMin}', '{sMax}', '{size}', '{0}')
                    #                     ''')
                    id += 1
            if not cur:
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
            else:
                insertArr.append(
                    (id,
                     f'BinaryIds: [{targetBinary["id"]}, {cloneBinary["id"]}]',
                     i,
                     j,
                     targetBinary["id"],
                     cloneBinary["id"],
                     targetBinary["name"],
                     cloneBinary["name"],
                     similarities[0] / 100,
                     similarities[1] / 100,
                     '',
                     1)
                )
                # cur.execute(f'''
                #     INSERT INTO {colRelt} VALUES ('{id}', 'BinaryIds: [{targetBinary["id"]}, {cloneBinary["id"]}]', '{i}', '{j}',
                #         '{targetBinary["id"]}', '{cloneBinary["id"]}', '{targetBinary["name"]}', '{cloneBinary["name"]}',
                #          '{similarities[0] / 100}', '{similarities[1] / 100}', '', '{1}')
                #     ''')
                insertLimitIndex += 1
                id += 1
    if not hasExecuted or len(insertArr) > 0:
        cur.executemany(f"insert into {colRelt} values ({'?, '* 11}?)", insertArr)
    logger.info('Binary Treemap Computing End')