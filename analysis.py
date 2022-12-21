import os
import redis
import json

try:
    import config
    REDIS_URL = config.REDIS_URL
    r = redis.from_url(REDIS_URL, ssl_cert_reqs=None, decode_responses=True)
except:
    REDIS_URL = os.getenv('CELERY_BROKER_URL')
    r = redis.from_url(REDIS_URL, decode_responses=True)


def createCandle():

    newCandle = {
        'time' : 0,
        'timestamp' : '',
        'time_delta' : 0,
        'close' : 0,
        'open' : 0,
        'price_delta' : 0,
        'high' : 0,
        'low' : 0,
        'buys' : 0,
        'sells' : 0,
        'delta' : 0,
        'delta_cumulative' : 0,
        'total' : 0,
        'oi_cumulative': 0,
        'oi_delta': 0,
        'vol_cumulative' : 0,
        'vol_delta': 0,
    }

    return newCandle


def getBlocks(size, blocksString):
    blocks = json.loads(blocksString)

    print(len(blocks))

    newCandle = {}

    firstUnit = {}

    ## new list of blocks
    newList = []

    count = 1

    for unit in blocks:
        print(count)

        ## ADD fist unit
        if count == 1:
            newCandle = {}
            for y in unit:
                # print(unit, y)
                newCandle[y] = unit[y]
                firstUnit[y] = unit[y]

            count += 1

        ## add more units
        elif count < size:
            if unit['low'] < newCandle['low']:
                newCandle['low'] = unit['low']

            if unit['high'] > newCandle['high']:
                newCandle['high'] = unit['high']

            newCandle['buys'] += unit['buys']
            newCandle['sells'] += unit['sells']
            newCandle['delta'] += unit['buys'] - unit['sells']
            newCandle['total'] += unit['total']
            newCandle['vol_delta'] += unit['vol_delta']

            count += 1

        ## add last unit
        elif count == size:
            ## also update price and delta
            if unit['low'] < newCandle['low']:
                newCandle['low'] = unit['low']

            if unit['high'] > newCandle['high']:
                newCandle['high'] = unit['high']

            newCandle['buys'] += unit['buys']
            newCandle['sells'] += unit['sells']
            newCandle['delta'] += unit['buys'] - unit['sells']
            newCandle['total'] += unit['total']
            newCandle['vol_delta'] += unit['vol_delta']

            newCandle['close'] = unit['close']
            newCandle['price_delta'] = newCandle['close'] - newCandle['open']

            if len(newList) > 0:
                lastUnit = newList[len(newList)-1]
                previousDelta = lastUnit['delta_cumulative']
                previousOI = lastUnit['oi_cumulative']
                previousVol = lastUnit['vol_cumulative']

                lastUnit['time_delta'] = newCandle['time'] - lastUnit['time']

                newCandle['delta_cumulative'] = previousDelta + newCandle['delta']
                newCandle['oi_cumulative'] = previousOI + newCandle['oi_delta']
                newCandle['vol_cumulative'] = previousVol + newCandle['total']

            newList.append(newCandle)

            count = 1


    print(len(newList))
    return json.dumps(newList)

# getVolumeBlock()
