import os, json, math
from celery import Celery
from celery.utils.log import get_task_logger
from time import sleep
from pybit import inverse_perpetual
# from message import sendMessage
import datetime as dt
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

app = Celery('tasks', broker=REDIS_URL, backend=REDIS_URL)
logger = get_task_logger(__name__)


def addBlock(units, blocks):

    ts = str(units[0]['timestamp'])

    # print('UNITS', len(units), len(blocks))
    previousOI = 0
    previousVol = 0
    previousDelta = 0
    previousTime = 0
    newOpen = 0

    lastIndex = len(blocks) - 1
    if len(blocks) > 0:
        lastUnit = blocks[lastIndex - 1] # ignore last unit which is the current one

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
        'timestamp' : ts,
        'time_delta' : timeDelta,
        'close' : price,
        'open' : newOpen,
        'price_delta' : price - newOpen,
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

    print('NEW CANDLE', newCandle['timestamp'])

    return newCandle

def logTimeUnit(unit, ts):
    print('ADD TIME FLOW')

    # add a new unit which is msg from handle_message

    timeflow =  json.loads(r.get('timeflow')) # []
    timeblocks = json.loads(r.get('timeblocks')) # []

    newUnit = { 'side' : unit['side'] , 'size' : unit['size'] , 'time' : unit['trade_time_ms'], 'timestamp' : ts, 'price' : unit['price']}

    print('TIME REDIS', len(timeflow), len(timeblocks))

    if len(timeflow) == 0:
        print('TIME 0')

        ## start new time flow and initial current candle
        timeflow.append(newUnit)
        currentCandle = addBlock(timeflow, timeblocks)
        timeblocks.append(currentCandle)

        r.set('timeblocks', json.dumps(timeblocks))
        r.set('timeflow', json.dumps(timeflow))
    else:
        blockStart = timeflow[0]['time']
        interval = (60000*5) # 5Min
        blockFinish = blockStart + interval

        print('TIME 1', blockStart, blockFinish, unit['trade_time_ms'], unit['trade_time_ms'] >= blockFinish, len(timeflow))
        if unit['trade_time_ms'] >= blockFinish: # store current candle and start a new Candle
            print('ADD TIME CANDLE')

            # replace current candle with completed candle
            newCandle = addBlock(timeflow, timeblocks)
            LastIndex = len(timeblocks) -1
            timeblocks[LastIndex] = newCandle

            # reset timeflow and add new unit
            timeflow = []
            newUnit['time'] = blockFinish
            timeflow.append(newUnit)

            # add fresh current candle to timeblock
            currentCandle = addBlock(timeflow, timeblocks)
            timeblocks.append(currentCandle)
            print('TIME FLOW RESET', len(timeflow), len(timeblocks))
            r.set('timeblocks', json.dumps(timeblocks))
            r.set('timeflow', json.dumps(timeflow))

        else: # add the unit to the time flow

            print('ADD TIME UNIT')
            timeflow.append(newUnit)

            # update current candle with new unit data
            currentCandle = addBlock(timeflow, timeblocks)
            LastIndex = len(timeblocks) -1
            timeblocks[LastIndex] = currentCandle
            r.set('timeblocks', json.dumps(timeblocks))
            r.set('timeflow', json.dumps(timeflow))


def handle_trade_message(msg):
    current_time = dt.datetime.utcnow()
    print('Current Time UTC', current_time, current_time.hour, current_time.minute)

    if current_time.hour == 23 and current_time.minute == 59 and len(json.loads(r.get('timeblocks'))) > 3:
        dt_string = current_time.strftime("%d/%m/%Y")
        print('REDIS STORE')
        vf = r.get('volumeflow')  # this the flow of message data for volume candles
        vb = r.get('volumeblocks')  #  this is the store of volume based candles
        tf = r.get('timeflow' )  # this the flow of message data to create next candle
        tb = r.get('timeblocks')
        history = json.loads(r.get('history'))
        history.append({
            'date' : dt_string,
            'volumeflow' : vf,
            'volumeblocks' : vb,
            'timeflow' : tf,
            'timeblocks' : tb
        })

    if current_time.hour == 0 and current_time.minute == 0: #and len(json.loads(r.get('timeblocks'))) > 3:

        print('REDIS RESET')
        r.set('volumeflow', json.dumps([]) )  # this the flow of message data for volume candles
        r.set('volumeblocks', json.dumps([]) )  #  this is the store of volume based candles
        r.set('timeflow', json.dumps([]) )  # this the flow of message data to create next candle
        r.set('timeblocks', json.dumps([]) ) # this is the store of new time based candles

    print('handle_trade_message')
    # print(msg['data'])
    block = 1000000

    volumeflow = json.loads(r.get('volumeflow')) ## reset after each volume block

    volumeflowTotal = 0
    for t in volumeflow:
        volumeflowTotal += t['size']


    for x in msg['data']:
        print('msg', x)

        timestamp = x['timestamp']
        ts = str(datetime.strptime(timestamp.split('.')[0], "%Y-%m-%dT%H:%M:%S"))
        # print('msg Ts', ts)

        # send message to time candle log
        logTimeUnit(x, ts)


        if volumeflowTotal + x['size'] <= block:
            # Normal addition of trade to volume flow
            # print(volumeflowTotal, '< Block')

            volumeflow.append( { 'side' : x['side'] , 'size' : x['size'] , 'time' : x['trade_time_ms'], 'timestamp' : ts, 'price' : x['price'], 'blocktrade' : x['is_block_trade']} )
            volumeflowTotal += x['size']

            volumeblocks = json.loads(r.get('volumeblocks'))
            currentCandle = addBlock(volumeflow, volumeblocks)

            ''' need to standardize this code logic '''

            LastIndex = len(volumeblocks) -1
            if LastIndex < 0:
                volumeblocks.append(currentCandle)
            else:
                volumeblocks[LastIndex] = currentCandle

            r.set('volumeblocks', json.dumps(volumeblocks))
        else:
            # Need to add a new block
            # print('carryOver')
            lefttoFill = block - volumeflowTotal
            carryOver = x['size'] - lefttoFill
            volumeflow.append({ 'side' : x['side'] , 'size' : lefttoFill, 'time' : x['trade_time_ms'], 'timestamp' : ts, 'price' : x['price'], 'blocktrade' : x['is_block_trade']})

            volumeblocks = json.loads(r.get('volumeblocks'))
            LastIndex = len(volumeblocks) -1

            newCandle = addBlock(volumeflow, volumeblocks)
            volumeblocks[LastIndex] = newCandle  # replace last candle (current) with completed
            r.set('volumeblocks', json.dumps(volumeblocks))


            # Need to add multiple blocks if there are any
            for y in range(carryOver//block):

                fullTradeList =  [{ 'side' : x['side'] , 'size' : block, 'time' : x['trade_time_ms'], 'timestamp' : ts, 'price' : x['price'], 'blocktrade' : x['is_block_trade']}]

                volumeblocks = json.loads(r.get('volumeblocks'))
                newCandle = addBlock(fullTradeList, volumeblocks)

                volumeblocks = json.loads(r.get('volumeblocks'))
                newCandle = addBlock(volumeflow, volumeblocks)
                volumeblocks.append(newCandle)
                r.set('volumeblocks', json.dumps(volumeblocks))

                print('Add Block', y)

            # Reset Current Block

            volumeflow = [{ 'side' : x['side'] , 'size' : carryOver%block, 'time' : x['trade_time_ms'], 'timestamp' : ts, 'price' : x['price'], 'blocktrade' : x['is_block_trade']}]

            volumeblocks = json.loads(r.get('volumeblocks'))
            currentCandle = addBlock(volumeflow, volumeblocks)
            volumeblocks.append(currentCandle)
            r.set('volumeblocks', json.dumps(volumeblocks))

            volumeflowTotal = carryOver%block


    r.set('volumeflow', json.dumps(volumeflow))


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



@app.task() #bind=True, base=AbortableTask  // (self)
def runStream():

    print('RUN_STREAM')
    rDict = {
        'lastPrice' : 0,
        'lastTime' : 0,
        'lastOI' : 0,
        'lastVol' : 0,
    }

    r.set('stream', json.dumps(rDict) )
    # r.set('history', json.dumps([]) )
    r.set('volumeflow', json.dumps([]) )  # this the flow of message data for volume candles
    r.set('volumeblocks', json.dumps([]) )  #  this is the store of volume based candles
    r.set('timeflow', json.dumps([]) )  # this the flow of message data to create next candle
    r.set('timeblocks', json.dumps([]) ) # this is the store of new time based candles

    # sendMessage('started')

    print('WEB_SOCKETS')

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

    return print('Task Closed')


if LOCAL:
    runStream()





