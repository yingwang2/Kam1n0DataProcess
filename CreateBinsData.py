import math
import copy
import logging

def createBins(dbBinsBySize, sizeRelt, countsSize, binSize, sizeArr, isCodeSize):
    logger = logging.getLogger()
    logger.info('Bin Chart for Code Size Computing Start') if isCodeSize else logger.info('Bin Chart for Block Size Computing Start')
    initialBins = []
    min = sizeRelt[0][0]
    max = sizeRelt[-1][-1]
    step = int(math.ceil((max - min) / binSize))
    relt = []

    for start in range(min, max, step):
        stop = start + step if start + step <= max else max

        if stop == max:
            count = len([x for x in sizeArr if (x >= start and x <= stop)])
            end = stop
        else:
            count = len([x for x in sizeArr if (x >= start and x < stop)])
            end = stop - 1

        initialBins.append({
            "min": start,
            "max": end,
            "count": count
        })

    relt.append(initialBins)

    bins = initialBins
    i = 0
    while i < binSize - 1:
        newBins = copy.deepcopy(bins)

        diff = bins[i]["count"] - countsSize[i]

        newCurrCount = countsSize[i]
        newCurrRange = sizeRelt[i]
        newNexCount = bins[i+1]["count"] + diff
        newNextRange = [newCurrRange[1] + 1, bins[i+1]["max"]]

        newBins[i] = {
            "min": sizeRelt[i][0],
            "max": sizeRelt[i][1],
            "count": newCurrCount
        }
        newBins[i+1] = {
            "min": newNextRange[0],
            "max": newNextRange[1],
            "count": newNexCount
        }
        relt.append(newBins)


        bins = copy.deepcopy(newBins)

        i += 1
    for i, stage in enumerate(relt):
        for j, bins in enumerate(stage):
            dbBinsBySize.insert({
                "min": bins["min"],
                "max": bins["max"],
                "count": bins["count"],
                "stage": i,
                "binIdx": j
            })

    # dbBinsBySize.insert({"0": relt})
        logger.info('Bin Chart for Code Size Computing End') if isCodeSize else logger.info(
            'Bin Chart for Block Size Computing End')