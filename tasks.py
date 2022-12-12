import os, json, math
from celery import Celery
from celery.utils.log import get_task_logger
from time import sleep
from pybit import inverse_perpetual
# from message import sendMessage
from datetime import datetime

import redis
LOCAL = False

try:
    import config
    LOCAL = True
    REDIS_URL = config.REDIS_URL
    r = redis.from_url(REDIS_URL, ssl_cert_reqs=None, decode_responses=True)
except:
    REDIS_URL = os.getenv('CELERY_BROKER_URL')
    r = redis.from_url(REDIS_URL, decode_responses=True)

print('URL', REDIS_URL)
print('REDIS', r)

app = Celery('tasks', broker=os.getenv("CELERY_BROKER_URL"))
logger = get_task_logger(__name__)


def addBlock(units, blocks):

    print('UNITS', len(units), len(blocks))
    previousOI = 0
    previousVol = 0
    previousDelta = 0
    previousTime = 0
    newOpen = 0

    lastIndex = len(blocks) - 1
    if len(blocks) > 0:
        last = list(blocks.keys())[lastIndex]
        lastUnit = blocks[last]
        print(lastUnit)

        newOpen = lastUnit['close']
        previousOI = lastUnit['oi_cumulative']
        previousTime = lastUnit['time']
        previousDelta = lastUnit['delta']
        previousVol = lastUnit['vol_cumulative']

    stream = json.loads(r.get('stream'))

    # print('Flow New Block', block)

    time = stream['lastTime']
    price = stream['lastPrice']
    oi = stream['lastOI']
    vol = stream['lastVol']

    volDelta = vol - previousVol
    timeDelta = time - previousTime
    oiDelta = oi - previousOI

    buyCount = 0
    sellCount = 0
    highPrice = 0
    lowPrice = 0

    count = 0

    for d in units:
        # print('BLOCK LOOP', d)
        if d['side'] == 'Buy':
            buyCount += d['size']
        else:
            sellCount += d['size']
        price = d['price']

        if count == 0:
            highPrice = price
            lowPrice = price
        else:
            if price > highPrice:
                highPrice = price
            if price < lowPrice:
                lowPrice = price

        count += 1

    delta = buyCount - sellCount

    newCandle = {
        'time' : time,
        'time_delta' : timeDelta,
        'close' : price,
        'open' : newOpen,
        'high' : highPrice,
        'low' : lowPrice,
        'buys' : buyCount,
        'sells' : sellCount,
        'delta' : delta,
        'delta_cumulative' : previousDelta + delta,
        'total' : buyCount + sellCount,
        'oi_cumulative': oi,
        'oi_delta': oiDelta,
        'vol_cumulative' : vol,
        'vol_delta': volDelta,
    }

    print('NEW CANDLE', newCandle)

    return newCandle

def logTimeCandle(unit):
    print('ADD TIME FLOW')

    # add a new unit which is msg from handle_message

    timeflow =  json.loads(r.get('timeflow')) # []
    timeblocks = json.loads(r.get('timeblocks')) # {}

    newUnit = { 'side' : unit['side'] , 'size' : unit['size'] , 'time' : unit['trade_time_ms'], 'timestamp' : unit['timestamp'], 'price' : unit['price']}

    print('TIME REDIS', len(timeflow), len(timeblocks))

    if len(timeflow) == 0:
        print('TIME 0')
        timeflow.append(newUnit)
        r.set('timeflow', json.dumps(timeflow))
    else:
        blockStart = timeflow[0]['time']
        interval = (60000*5) # 5Min
        blockFinish = blockStart + interval

        print('TIME 1', blockStart, blockFinish, len(timeflow))
        if unit['trade_time_ms'] >= blockFinish: # start a new Candle
            print('ADD TIME CANDLE')
            timestamp = timeflow[0]['timestamp']
            example = '2022-12-09T08:20:22.000Z'
            t = datetime.strptime(timestamp.split('.')[0], "%Y-%m-%dT%H:%M:%S")
            print('STRP TIME', t)
            newCandle = addBlock(timeflow, timeblocks)

            timeblocks[str(t)] = newCandle
            r.set('timeblocks', json.dumps(timeblocks))

            timeflow = []
            newUnit['time'] = blockFinish
            timeflow.append(newUnit)
            print('TIME FLOW RESET', len(timeflow))

            r.set('timeflow', json.dumps(timeflow))

        else: # add the unit to the time flow
            print('ADD TIME DATA')
            timeflow.append(newUnit)
            r.set('timeflow', json.dumps(timeflow))


def handle_trade_message(msg):
    current_time = datetime.now()
    print(current_time.hour, current_time.minute)
    if current_time.hour == 9 and current_time.minute == 0 and len(json.loads(r.get('timeblocks'))) > 3:
        r.set('tradeList', json.dumps([]) )  # this the flow of message data for volume candles
        r.set('blockflow', json.dumps({}) )  #  this is the store of volume based candles
        r.set('timeflow', json.dumps([]) )  # this the flow of message data to create next candle
        r.set('timeblocks', json.dumps({}) ) # this is the store of new time based candles

    print('handle_trade_message')
    # print(msg['data'])
    block = 1000000

    tradeList = json.loads(r.get('tradeList')) ## reset after each volume block

    tradeListTotal = 0
    for t in tradeList:
        tradeListTotal += t['size']


    for x in msg['data']:
        print('msg', x)

        # send message to time candle log
        logTimeCandle(x)

        if tradeListTotal + x['size'] <= block:
            # Normal addition of Trade
            print(tradeListTotal, '< Block')
            tradeList.append( { 'side' : x['side'] , 'size' : x['size'] , 'time' : x['trade_time_ms'], 'price' : x['price'], 'blocktrade' : x['is_block_trade']} )
            tradeListTotal += x['size']
        else:
            # Need to add a new block
            print('carryOver')
            lefttoFill = block - tradeListTotal
            carryOver = x['size'] - lefttoFill
            tradeList.append({ 'side' : x['side'] , 'size' : lefttoFill, 'time' : x['trade_time_ms'], 'price' : x['price'], 'blocktrade' : x['is_block_trade']})

            blockflow = json.loads(r.get('blockflow'))
            newCandle = addBlock(tradeList, blockflow)

            blockflow[len(blockflow) + 1] = newCandle
            r.set('blockflow', json.dumps(blockflow))


            # Need to add multiple blocks
            for y in range(carryOver//block):
                blockflow = json.loads(r.get('blockflow'))
                fullTradeList =  [{ 'side' : x['side'] , 'size' : block, 'time' : x['trade_time_ms'], 'price' : x['price'], 'blocktrade' : x['is_block_trade']}]
                newCandle = addBlock(fullTradeList, blockflow)

                blockflow[len(blockflow) + 1] = newCandle
                r.set('blockflow', json.dumps(blockflow))

                print('Add Block', y)

            # Reset Current Block
            tradeList = [{ 'side' : x['side'] , 'size' : carryOver%block, 'time' : x['trade_time_ms'], 'price' : x['price'], 'blocktrade' : x['is_block_trade']}]
            tradeListTotal = carryOver%block


    r.set('tradeList', json.dumps(tradeList))


def handle_info_message(msg):
    # print('handle_info_message')
    vol = msg['data']['total_volume']
    oi = msg['data']['open_interest']
    price = msg['data']['last_price']
    time = msg['timestamp_e6']

    stream = json.loads(r.get('stream'))
    stream['lastTime'] = time
    stream['lastPrice'] = price
    stream['lastOI'] = oi
    stream['lastVol'] = vol
    # print(stream)
    r.set('stream', json.dumps(stream) )



def webSockets():
    print('WEB_SOCKETS')

    # openWhile =  r.get('while')
    ws_inverseP = inverse_perpetual.WebSocket(
        test=False,
        ping_interval=30,  # the default is 30
        ping_timeout=10,  # the default is 10
        domain="bybit"  # the default is "bybit"
    )

    ws_inverseP.trade_stream(
        handle_trade_message, "BTCUSD"
    )

    ws_inverseP.instrument_info_stream(
        handle_info_message, "BTCUSD"
    )
    while True:
        sleep(0.1)


@app.task
def runStream():
    print('RUN_STREAM')
    rDict = {
        # 'volRec' : 0,   ### volume check at last package
        # 'volCount' : 0,  ### cumulative volume from stream start
        # 'blockCount' : 0,
        # 'OIRec' : 0,
        # 'OICount' : 0,
        'lastPrice' : 0,
        'lastTime' : 0,
        'lastOI' : 0,
        'lastVol' : 0,
    }


    r.set('stream', json.dumps(rDict) )
    r.set('tradeList', json.dumps([]) )  # this the flow of message data for volume candles
    r.set('blockflow', json.dumps({}) )  #  this is the store of volume based candles
    r.set('timeflow', json.dumps([]) )  # this the flow of message data to create next candle
    r.set('timeblocks', json.dumps({}) ) # this is the store of new time based candles

    # sendMessage('started')

    webSockets()

# if LOCAL:
#     runStream()





