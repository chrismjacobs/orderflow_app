import os, json, math
from celery import Celery
from celery.utils.log import get_task_logger
from time import sleep
from pybit import inverse_perpetual
# from message import sendMessage
import datetime as dt
from datetime import datetime
import redis
import discord
import time
from discord.ext import tasks, commands


session = inverse_perpetual.HTTP(
    endpoint='https://api.bybit.com'
)

LOCAL = False

try:
    import config
    LOCAL = True
    REDIS_URL = config.REDIS_URL
    DISCORD_CHANNEL = config.DISCORD_CHANNEL
    DISCORD_TOKEN = config.DISCORD_TOKEN
    DISCORD_USER = config.DISCORD_USER
    r = redis.from_url(REDIS_URL, ssl_cert_reqs=None, decode_responses=True)
except:
    REDIS_URL = os.getenv('CELERY_BROKER_URL')
    DISCORD_CHANNEL = os.getenv('DISCORD_CHANNEL')
    DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
    DISCORD_USER = os.getenv('DISCORD_USER')
    r = redis.from_url(REDIS_URL, decode_responses=True)

print('URL', REDIS_URL)
print('REDIS', r)

app = Celery('tasks', broker=REDIS_URL, backend=REDIS_URL)
logger = get_task_logger(__name__)

def getHiLow(blocks):

    minutes = 60

    now = dt.datetime.utcnow()
    timestamp = int(dt.datetime.timestamp(now)) - int(minutes)*60

    data = session.query_kline(symbol="BTCUSD", interval="1", from_time=str(timestamp))['result']

    print('GET HI LOW ', len(data))

    hAry = []
    lAry = []

    for i in range(0, len(data)):

        hAry.append(round(  float(data[i]['high'])  *2)/2)  # this formula rounds to the nearest 0.5
        lAry.append(round(  float(data[i]['low'])  *2)/2)

    mHi = max(hAry)
    mLow = min(lAry)

    highInfo = {
        'price' : mHi,
        'index' : 0,
        'delta' : 0,
        'local' : False
    }

    lowInfo = {
        'price' : mLow,
        'index' : 0,
        'delta' : 0,
        'local' : False
    }

    count = 0

    tbs = len(blocks) - 1

    for tb in blocks:
        if tb['high'] == mHi:
            highInfo['index'] = tbs - count
            highInfo['delta'] = tb['delta_cumulative']
            if count == tbs - 1:
                highInfo['local'] = True
        if tb['low'] == mLow:
            lowInfo['index'] = tbs - count
            lowInfo['delta'] = tb['delta_cumulative']
            if count == tbs - 1:
                highInfo['local'] = True

        if highInfo['index'] != 0 and highInfo['local'] == False:

            cvdExcessHi = tb['delta_cumulative'] > highInfo['delta']
            cvdIntactHi = tb['high'] < mHi

            if tb['delta_cumulative'] > highInfo['delta'] and count < tbs:
                highInfo['delta'] = tb['delta_cumulative']


            ## last candle
            if count == tbs:
                if cvdExcessHi and cvdIntactHi:
                    if r.get('discord_filter') == 'off':
                        r.set('discord', 'CVD bear divergence: ' + json.dumps(highInfo))

        if lowInfo['index'] != 0 and lowInfo['local'] == False:
            cvdExcessLo = tb['delta_cumulative'] < lowInfo['delta']
            cvdIntactLo = tb['low'] > mLow

            if tb['delta_cumulative'] < lowInfo['delta'] and count < tbs:
                lowInfo['delta'] = tb['delta_cumulative']

            if count == tbs - 1 or count == tbs - 2:
                lowInfo['local'] = True
            ## last candle
            if count == tbs:
                if cvdExcessLo and cvdIntactLo:
                    if r.get('discord_filter') == 'off':
                        r.set('discord', 'CVD bull divergence: ' + json.dumps(lowInfo))

        count += 1

    return {'highInfo' : highInfo , 'lowInfo' : lowInfo }


def addBlockBlock(blocks, newCandle, timeNow):

    if len(blocks) > 1:
        lastCandle = blocks[-2]
        previousDeltaCum = lastCandle['delta_cumulative']
        previousOICum = lastCandle['oi_cumulative']
        previousTime = lastCandle['time']
    else:
        previousDeltaCum = 0
        previousOICum = 0
        previousTime = 0

    currentCandle = blocks[-1]

    if newCandle['low'] < currentCandle['low']:
        currentCandle['low'] = newCandle['low']
    if newCandle['high'] < currentCandle['high']:
        currentCandle['high'] = newCandle['high']

    currentCandle['buys'] += newCandle['buys']
    currentCandle['sells'] += newCandle['sells']
    currentCandle['delta'] = currentCandle['buys'] - currentCandle['sells']
    currentCandle['total'] = currentCandle['buys'] + currentCandle['sells']

    currentCandle['close'] = newCandle['close']
    currentCandle['price_delta'] = currentCandle['close'] - currentCandle['open']

    currentCandle['delta_cumulative'] =  previousDeltaCum + currentCandle['delta']
    currentCandle['oi_cumulative'] = currentCandle['oi_cumulative'] + newCandle['oi_delta']
    currentCandle['oi_delta'] = currentCandle['oi_cumulative'] - previousOICum
    currentCandle['time_delta'] = timeNow - previousTime

    if r.get('discord_filter') == 'off':
        if currentCandle['delta'] < 0 and currentCandle['price_delta'] > 0:
            r.set('discord', 'possible 2m bull div candle')
        if currentCandle['delta'] > 0 and currentCandle['price_delta'] < 0:
            r.set('discord', 'possible 2m bear div candle')


def addBlock(units, blocks, mode):

    ts = str(units[0]['timestamp'])

    # print('UNITS', len(units), len(blocks))

    ''' STREAM DATA '''
    previousOI = 0
    previousDeltaCum = 0
    previousTime = 0
    newOpen = 0

    stream = json.loads(r.get('stream'))

    sess = session.latest_information_for_symbol(symbol="BTCUSD")
    # print(sess)
    timeNow = round(float(sess['time_now']), 1)
    stream['lastTime'] = timeNow

    price = sess['result'][0]['last_price']
    stream['lastPrice'] = price

    oi = sess['result'][0]['open_interest']
    stream['lastOI'] = oi
    # vol = sess['lastVol']
    print('ADD BLOCK: ' + mode)

    if mode == 'time' or mode == 'timeblock':

        if len(stream['1mOI']) < 2:
            print('INITIAL')
            stream['1mOI'] = [timeNow, oi]
        elif timeNow - stream['1mOI'][0] >= 60:


            deltaOI =  oi - stream['1mOI'][1]
            if deltaOI > stream['oiMarker']:
                r.set('discord', 'sudden OI change: ' + json.dumps({'delta oi' : deltaOI}))

            stream['1mOI'] = [timeNow, oi]

        else:
            stream['delta'] = [timeNow - stream['1mOI'][0], oi - stream['1mOI'][1] ]

    if mode == 'timeblock':
        stream['Divs'] = getHiLow(blocks)

    # print(stream)
    r.set('stream', json.dumps(stream) )

    ''' BLOCK DATA '''

    lastCandle = None

    if len(blocks) > 1:

        lastIndex = len(blocks) - 1

        if mode == 'carry':
            lastCandle = blocks[lastIndex] # when carrying there is no current candle
        else:
            lastCandle = blocks[lastIndex - 1] # ignore last unit which is the current one
        newOpen = lastCandle['close']
        previousOI = lastCandle['oi_cumulative']
        previousTime = lastCandle['time']
        previousDeltaCum = lastCandle['delta_cumulative']
        timeDelta = timeNow - previousTime
        oiDelta = oi - previousOI
    else:
        # volDelta = 0
        timeDelta = 0
        oiDelta = 0
        previousDeltaCum = 0

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
        'time' : timeNow,
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
        'delta_cumulative' : previousDeltaCum + delta,
        'total' : buyCount + sellCount,
        'oi_cumulative': oi,
        'oi_delta': oiDelta,
        'pva_status': {}
    }

    print('NEW CANDLE: ' + mode, newCandle['timestamp'])

    if mode == 'volblock' or mode == 'carry':
        try:
            blockSize = 1000000
            if LOCAL:
                blockSize = 100000

            blocks2m = json.loads(r.get('volumeblocks2m'))
            if len(blocks2m) == 0:
                blocks2m.append(newCandle)
            elif blocks2m[-1]['total'] == blockSize:
                addBlockBlock(blocks2m, newCandle, timeNow)
            elif blocks2m[-1]['total'] == blockSize * 2:
                blocks2m.append(newCandle)

            r.set('volumeblocks2m', json.dumps(blocks2m))
        except:
            print('VOLBLOCKS ERROR')

    return newCandle

def getPVAstatus(timeblocks):
    last11blocks = []
    if len(timeblocks) < 11:
        history = json.loads(r.get('history'))
        try:
            if len(history) > 0:
                lastHistory = history[-1]['timeblocks']
                howManyOldTimeblocks = (11-len(timeblocks))
                last11blocks = lastHistory[-howManyOldTimeblocks:] + timeblocks
                # print('LASTBLOCKS HISTORY', last11blocks)
                ## if one time block - get last 10 from history
                ## if 4 time blocks - get last 7 from history
            else:
                return {}
        except:
            # r.set('discord', 'History PVA error')
            print('PVA HISTORY ERROR')
            return {}
    else:
        if len(timeblocks) >= 11:
            try:
                last11blocks = timeblocks[-11:]
            except:
                return {}

        else:
            return {}

    print('PVA Calculate')

    sumVolume = 0
    lastVolume = 0
    lastDelta = 0
    lastPriceDelta = 0

    try:
        count = 1
        for x in last11blocks:
            if count < 11:
                sumVolume += x['total']
                count += 1
            else:
                lastVolume = x['total']
                lastDelta = x['delta']
                lastPriceDelta = x['price_delta']
                lastPriceDelta = x['price_delta']
                lastOIDelta = x['oi_delta']

        pva = False
        divergence = False
        flatOI = False

        percentage = round((lastVolume/(sumVolume/10)), 2)
        deltapercentage = round((lastDelta/lastVolume)*100, 2)

        if percentage > 1.5:
            pva = True
            if lastOIDelta < 100000  and lastOIDelta > - 100000:
                flatOI = True

        if lastDelta > 0 and lastPriceDelta < 0:
            divergence = True
        elif lastDelta < 0 and lastPriceDelta > 0:
            divergence = True

        returnPVA = {'pva' : pva, 'vol': lastVolume, 'percentage' : percentage, 'deltapercentge' : deltapercentage, 'divergence' : divergence, 'flatOI' : flatOI}

        print('RETURN PVA')

        if pva and flatOI:
            r.set('discord', 'changing hands: ' + json.dumps(returnPVA))
        elif pva and divergence:
            r.set('discord', 'divergence: ' + json.dumps(returnPVA))

        return returnPVA

    except:
        return {}


def logTimeUnit(unit, ts):
    print('ADD TIME FLOW')

    # add a new unit which is msg from handle_message

    timeflow =  json.loads(r.get('timeflow')) # []
    timeblocks = json.loads(r.get('timeblocks')) # []

    newUnit = { 'side' : unit['side'] , 'size' : unit['size'] , 'time' : unit['trade_time_ms'], 'timestamp' : ts, 'price' : unit['price']}

    print('TIME REDIS', len(timeflow), len(timeblocks))

    if len(timeflow) == 0:
        print('TIME 0')

        ## start the initial time flow and initial current candle
        timeflow.append(newUnit)
        currentCandle = addBlock(timeflow, timeblocks, 'time')
        timeblocks.append(currentCandle)

        r.set('timeblocks', json.dumps(timeblocks))
        r.set('timeflow', json.dumps(timeflow))
    else:
        blockStart = timeflow[0]['time']
        if LOCAL:
            interval = (60000*1) # 1Min
        else:
            interval = (60000*5) # 5Min
        blockFinish = blockStart + interval

        print('TIME 1', blockStart, blockFinish)
        if unit['trade_time_ms'] >= blockFinish: # store current candle and start a new Candle
            print('ADD TIME CANDLE')

            # replace current candle with completed candle
            newCandle = addBlock(timeflow, timeblocks, 'timeblock')
            LastIndex = len(timeblocks) - 1
            timeblocks[LastIndex] = newCandle

            timeblocks[LastIndex]['pva_status'] = getPVAstatus(timeblocks)

            # reset timeflow and add new unit
            timeflow = []
            newUnit['time'] = blockFinish
            timeflow.append(newUnit)

            # add fresh current candle to timeblock
            currentCandle = addBlock(timeflow, timeblocks, 'time')
            timeblocks.append(currentCandle)
            print('TIME FLOW RESET', len(timeflow), len(timeblocks))
            r.set('timeblocks', json.dumps(timeblocks))
            r.set('timeflow', json.dumps(timeflow))

        else: # add the unit to the time flow

            print('ADD TIME UNIT')
            timeflow.append(newUnit)

            # update current candle with new unit data
            currentCandle = addBlock(timeflow, timeblocks, 'time')
            LastIndex = len(timeblocks) - 1
            timeblocks[LastIndex] = currentCandle
            r.set('timeblocks', json.dumps(timeblocks))
            r.set('timeflow', json.dumps(timeflow))



def handle_trade_message(msg):
    current_time = dt.datetime.utcnow()
    print('Current Time UTC', current_time, current_time.hour, current_time.minute)
    dt_string = current_time.strftime("%d/%m/%Y")

    if current_time.hour == 23 and current_time.minute == 59:
        history = json.loads(r.get('history'))
        if len(history) > 0:
            lastHistory = json.loads(r.get('history'))[len(history)-1]

            if lastHistory['date'] != dt_string:
                print('REDIS STORE', dt_string)
                vb = json.loads(r.get('volumeblocks'))
                tb = json.loads(r.get('timeblocks'))
                history.append({
                    'date' : dt_string,
                    'volumeblocks' : vb,
                    'timeblocks' : tb
                })
                r.set('history', json.dumps(history))
                r.set('discord', 'history log')
        else:
            print('REDIS STORE INITIAL')
            vb = json.loads(r.get('volumeblocks'))
            tb = json.loads(r.get('timeblocks'))
            history.append({
                'date' : current_time.strftime("%d/%m/%Y"),
                'volumeblocks' : vb,
                'timeblocks' : tb
            })
            r.set('history', json.dumps(history))

    if current_time.hour == 0 and current_time.minute == 0:
        print('REDIS RESET', current_time)
        if r.get('newDay') != dt_string:
            print('REDIS RESET')
            r.set('volumeflow', json.dumps([]) )  # this the flow of message data for volume candles
            r.set('volumeblocks', json.dumps([]) )  #  this is the store of volume based candles
            r.set('volumeblocks2m', json.dumps([]) )  #  this is the store of volume based candles
            r.set('volumeblocks5m', json.dumps([]) )  #  this is the store of volume based candles
            r.set('timeflow', json.dumps([]) )  # this the flow of message data to create next candle
            r.set('timeblocks', json.dumps([]) ) # this is the store of new time based candles
            r.set('newDay', dt_string)
            r.set('discord', 'new day')


    print('handle_trade_message')
    # print(msg['data'])

    if LOCAL:
        block = 100000
    else:
        block = 1000000

    ## load vol flow
    volumeflow = json.loads(r.get('volumeflow')) ## reset after each volume block

    ## calculate current candle size
    volumeflowTotal = 0
    for t in volumeflow:
        volumeflowTotal += t['size']

    ## run through data
    for x in msg['data']:
        if x['size'] > 100:
            print('msg', x['side'], x['size'])

        ## look for big blocks
        if x['size'] > block:
            r.set('discord', '1000000')

        timestamp = x['timestamp']
        ts = str(datetime.strptime(timestamp.split('.')[0], "%Y-%m-%dT%H:%M:%S"))

        # send message to time candle log
        logTimeUnit(x, ts)


        if volumeflowTotal + x['size'] <= block:
            # Normal addition of trade to volume flow
            # print(volumeflowTotal, '< Block')

            volumeflow.append( { 'side' : x['side'] , 'size' : x['size'] , 'time' : x['trade_time_ms'], 'timestamp' : ts, 'price' : x['price'], 'blocktrade' : x['is_block_trade']} )
            volumeflowTotal += x['size']

            volumeblocks = json.loads(r.get('volumeblocks'))
            currentCandle = addBlock(volumeflow, volumeblocks, 'vol')

            LastIndex = len(volumeblocks) - 1
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
            LastIndex = len(volumeblocks) - 1
            newCandle = addBlock(volumeflow, volumeblocks, 'volblock')
            volumeblocks[LastIndex] = newCandle  # replace last candle (current) with completed

            r.set('volumeblocks', json.dumps(volumeblocks))

            ## volume flow has been added as  full candle and should be reset
            volumeflow = []
            volumeflowTotal = 0
            ## Note: volumeblock does not have a current candle at thsi point

            # if r.get('discord_filter') == 'off':
            #     blockList = []
            #     for m in msg['data']:
            #         blockList.append(m['size'])

            #     r.set('discord', 'VOL BLOCK: ' + str(x['size']) + ':' + str(carryOver) + '\n' + str(carryOver//block) + '\n' + json.dumps(blockList))


            # if r.get('discord_filter') == 'off':
            #     r.set('discord', 'Carry Over: ' + str(carryOver) + ' / ' + str(carryOver//block))

            # Need to add multiple blocks if there are any
            for y in range(carryOver//block):

                r.set('discord', 'Carry Over: ' + str(carryOver//block))

                ## this is volume flow list - just one block
                fullTradeList =  [{ 'side' : x['side'] , 'size' : block, 'time' : x['trade_time_ms'], 'timestamp' : ts, 'price' : x['price'], 'blocktrade' : 'CARRY OVER'}]

                ## keep appending large blocks
                volumeblocks = json.loads(r.get('volumeblocks'))
                newCandle = addBlock(fullTradeList, volumeblocks, 'carry')
                volumeblocks.append(newCandle)
                r.set('volumeblocks', json.dumps(volumeblocks))

                print('Add Block', y)

            # Creat new flow block with left over contracts
            volumeflow = [{ 'side' : x['side'] , 'size' : carryOver%block, 'time' : x['trade_time_ms'], 'timestamp' : ts, 'price' : x['price'], 'blocktrade' : x['is_block_trade']}]

            volumeblocks = json.loads(r.get('volumeblocks'))
            currentCandle = addBlock(volumeflow, volumeblocks, 'vol')
            volumeblocks.append(currentCandle)
            r.set('volumeblocks', json.dumps(volumeblocks))

            volumeflowTotal = carryOver%block


    r.set('volumeflow', json.dumps(volumeflow))



def startDiscord():
    ## intents controls what the bot can do; in this case read message content
    intents = discord.Intents.default()
    intents.message_content = True
    intents.members = True
    bot = commands.Bot(command_prefix="!", intents=discord.Intents().all())

    @bot.event
    async def on_ready():
        print(f'{bot.user} is now running!')
        user = bot.get_user(int(DISCORD_USER))
        print('DISCORD_GET USER', DISCORD_USER, 'user=', user)
        await user.send('Running')
        checkRedis.start(user)

    @tasks.loop(seconds=10)
    async def checkRedis(user):
        print('DISCORD REDIS CHECK')

        if r.get('discord') != 'blank':
            await user.send(r.get('discord'))
            r.set('discord', 'blank')

    @bot.event
    async def on_message(msg):
        user = bot.get_user(int(DISCORD_USER))
        print('MESSAGE DDDDDDDDD', msg.content)
        if msg.author == user:
            await user.send('ho')


    bot.run(DISCORD_TOKEN)


@app.task() #bind=True, base=AbortableTask  // (self)
def runStream():

    print('RUN_STREAM')
    # rDict = {
    #     'lastPrice' : 0,
    #     'lastTime' : 0,
    #     'lastOI' : 0,
    #     'lastVol' : 0,
    #     '1mOI' : []
    # }

    rDict = {
        'lastPrice' : 0,
        'lastTime' : 0,
        'lastOI' : 0,
        '1mOI' : [],
        'oiMarker' : 1000000
    }

    r.set('discord_filter',  'off')
    r.set('stream', json.dumps(rDict) )
    # r.set('history', json.dumps([]) )
    r.set('volumeflow', json.dumps([]) )  # this the flow of message data for volume candles
    r.set('volumeblocks2m', json.dumps([]) )  #  this is the store of volume based candles
    r.set('volumeblocks5m', json.dumps([]) )  #  this is the store of volume based candles
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

    # ws_inverseP.instrument_info_stream(
    #     handle_info_message, "BTCUSD"
    # )

    startDiscord()

    while True:
        sleep(0.1)

    return print('Task Closed')


if LOCAL:
    runStream()





