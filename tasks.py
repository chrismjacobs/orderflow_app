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
from pythonping import ping


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

def getHiLow(timeblocks):

    tbRev = timeblocks[::-1] ## creates a new list  .reverse() change the original list

    ## last block is not completed but does have current HLOC

    LH2h = tbRev[0]['high']
    LL2h = tbRev[0]['low']
    LH2h_index = 0
    LL2h_index = 0
    LH2h_cvd = tbRev[0]['delta_cumulative']
    LL2h_cvd = tbRev[0]['delta_cumulative']


    '''Set locals for the last 2 Hours'''
    count = 0

    for block in tbRev:
        if count <= 23:
            if block['high'] > LH2h:
                LH2h = block['high']
                LH2h_index = count
                LH2h_cvd = block['delta_cumulative']
            if block['low'] < LL2h:
                LL2h = block['low']
                LL2h_index = count
                LL2h_cvd = block['delta_cumulative']
        count += 1

    ''' check if previous candle has an exceeeding cvd'''
    try:
        if tbRev[LH2h_index + 1]['delta_cumulative'] > LH2h_cvd:
            LH2h_cvd = tbRev[LH2h_index + 1]['delta_cumulative']

        if tbRev[LL2h_index + 1]['delta_cumulative'] < LL2h_cvd:
            LL2h_cvd = tbRev[LL2h_index + 1]['delta_cumulative']
    except:
        print('LOCAL CVD CHECK FAIL')

    '''Look for areas where the CVD has already exceeded '''
    recount = 0

    for block in tbRev:
        if recount <= 23 and recount > 1: # discount the first two blocks
            if block['delta_cumulative'] > LH2h_cvd:
                LH2h_cvd = block['delta_cumulative']
            if block['delta_cumulative'] < LL2h_cvd:
                LL2h_cvd = block['delta_cumulative']

        recount += 1

    highInfo = {
        'price' : LH2h,
        'index' : LH2h_index,
        'delta' : LH2h_cvd,
        'div' : False
    }

    lowInfo = {
        'price' : LL2h,
        'index' : LL2h_index,
        'delta' : LL2h_cvd,
        'div' : False
    }

    if LH2h_index >= 2:
        # current timeblock nor the previous is not the highest/lowest
        if tbRev[0]['delta_cumulative'] > LH2h_cvd:
            # Divergence Triggered
            highInfo['div'] = True
            r.set('discord', 'CVD BEAR div: ' + json.dumps(highInfo))

    if LL2h_index >= 2:
        if tbRev[0]['delta_cumulative'] < LL2h_cvd:
            # Divergence Triggered
            lowInfo['div'] = True
            r.set('discord', 'CVD BULL div: ' + json.dumps(lowInfo))


    return {'highInfo' : highInfo , 'lowInfo' : lowInfo}


def addBlockBlock(blocks, newCandle, timeNow, size):

    print('BLOCK BLOCK 1')

    if len(blocks) > 1:
        lastCandle = blocks[-2]
        previousDeltaCum = lastCandle['delta_cumulative']
        previousOICum = lastCandle['oi_cumulative']
        previousTime = lastCandle['trade_time_ms']
    # if len(blocks) == 2:
    #     '''### readjust first block'''
    #     firstCandle = blocks[0]
    #     firstCandle['trade_time_ms'] = firstCandle['trade_time_ms'] - timeNow
    #     firstCandle['oi_delta'] = firstCandle['oi_delta'] - newCandle['oi_delta']
    else:
        previousDeltaCum = 0
        previousOICum = 0
        previousTime = 0

    print('BLOCK BLOCK 2')
    currentCandle = blocks[-1]

    if newCandle['low'] < currentCandle['low']:
        currentCandle['low'] = newCandle['low']
    if newCandle['high'] > currentCandle['high']:
        currentCandle['high'] = newCandle['high']

    print('BLOCK BLOCK 3')
    currentCandle['buys'] += newCandle['buys']
    currentCandle['sells'] += newCandle['sells']
    currentCandle['delta'] = currentCandle['buys'] - currentCandle['sells']
    currentCandle['total'] = currentCandle['buys'] + currentCandle['sells']

    currentCandle['close'] = newCandle['close']
    currentCandle['price_delta'] = currentCandle['close'] - currentCandle['open']

    print('BLOCK BLOCK 4')

    currentCandle['delta_cumulative'] =  previousDeltaCum + currentCandle['delta']
    currentCandle['oi_cumulative'] = currentCandle['oi_cumulative'] + newCandle['oi_delta']
    currentCandle['oi_delta'] = currentCandle['oi_cumulative'] - previousOICum
    currentCandle['time_delta'] = timeNow - previousTime


    volDivBull2M = False
    volDivBull5M = False

    volDivBear2M = False
    volDivBear5M = False

    print('BLOCK BLOCK 5')

    deltaPercent = round( (  currentCandle['delta']  /  (size*1_000_000)  ) * 100  )

    if r.get('discord_filter') == 'off':
        if abs(deltaPercent) > 20:
            if currentCandle['delta'] < 0 and currentCandle['price_delta'] > 0:
                if currentCandle['total'] >= 2_000_000 and size == 2:
                    volDivBull2M = True
                    r.set('discord', '2M possible BULL div candle: ' + str(deltaPercent))
                if currentCandle['total'] >= 4_000_000:
                    deltaPercent = round((currentCandle['delta']/5_000_000)*100)
                    volDivBull5M = True
                    r.set('discord', '5M possible BULL div candle: ' + str(deltaPercent))

            print('BLOCK BLOCK BREAK')

            if currentCandle['delta'] > 0 and currentCandle['price_delta'] < 0:
                if currentCandle['total'] == 2_000_000 and size == 2:
                    volDivBear2M = True
                    r.set('discord', '2M possible BEAR div candle: ' + str(deltaPercent))
                if currentCandle['total'] >= 4_000_000:
                    volDivBear5M = True
                    r.set('discord', '5M possible BEAR div candle: ' + str(deltaPercent))


    if size == 5:
        return { 'Bull': volDivBull5M, 'Bear': volDivBear5M }

    if size == 2:
        return { 'Bull': volDivBull2M, 'Bear': volDivBear2M }



def manageStream(streamTime, streamPrice, streamOI):

    timeblocks = json.loads(r.get('timeblocks'))
    currentBuys = 0
    currentSells = 0
    if len(timeblocks) > 0:
        currentBuys = timeblocks[-1]['buys']
        currentSells = timeblocks[-1]['sells']

    print('Manage Stream')
    stream = json.loads(r.get('stream'))
    stream['lastTime'] = streamTime
    stream['lastPrice'] = streamPrice
    stream['lastOI'] = streamOI

    if len(stream['1mOI']) < 2:
        print('INITIAL')
        stream['1mOI'] = [streamTime, streamOI]
    elif streamTime - stream['1mOI'][0] >= 90:

        deltaOI =  streamOI - stream['1mOI'][1]
        deltaOIstr = str(round(deltaOI/100_000)/10) + 'm '
        deltaBuyStr = str(round(currentBuys/1_000)) + 'k '
        deltaSellStr = str(- round(currentSells/1_000)) + 'k '

        if deltaOI > stream['oiMarker']:
            r.set('discord', 'Sudden OI INC: ' + deltaOIstr + ' Buys:' + deltaBuyStr + ' Sells: ' + deltaSellStr)

        if deltaOI < - stream['oiMarker']:
            r.set('discord', 'Sudden OI DEC: ' + deltaOIstr + ' Buys ' + deltaBuyStr + ' Sells: ' + deltaSellStr)

        stream['1mOI'] = [streamTime, streamOI]

    else:
        stream['oi delta'] = [round(streamTime - stream['1mOI'][0]), streamOI - stream['1mOI'][1], '(secs/oi)' ]

    # print(stream)
    r.set('stream', json.dumps(stream) )

    return True


def addBlock(units, blocks, mode):

    CVDdivergence = {}
    tradeCount = 0

    if mode == 'timeblock':
        CVDdivergence = getHiLow(blocks)
        stream = json.loads(r.get('stream'))
        stream['Divs'] = CVDdivergence
        r.set('stream', json.dumps(stream) )

    # print('UNITS', len(units), len(blocks))

    switch = False

    if mode == 'deltablock':

        try:

            fastCandles = 0

            switchUp = False
            switchDown = False

            if len(blocks) > 2:
                if blocks[-1]['delta'] > 0 and blocks[-2]['delta'] < 0:
                    switchUp = True
                if blocks[-1]['delta'] < 0 and blocks[-2]['delta'] > 0:
                    switchDown = True

            lastElements = [-2, -3, -4, -5, -6]
            timeElements = []

            if len(blocks) >= 7:
                for t in lastElements:
                    timeDelta = blocks[t]['time_delta']/1000
                    timeElements.append(timeDelta)
                    if timeDelta < 30:
                        fastCandles += 1
                        timeElements.append(timeDelta)

            if fastCandles >= 3:
                if switchUp:
                    switch = True
                    r.set('discord', 'Delta Switch Up:' + json.dumps(timeElements) )
                if switchDown:
                    switch = True
                    r.set('discord', 'Delta Switch Down:' + json.dumps(timeElements) )

            if r.get('discord_delta') == 'on':
                r.set('discord', 'Delta Switch Alert')

        except:

            r.set('discord', 'delta switch fail')

    ''' BLOCK DATA '''

    print('BLOCK DATA: ' + mode)
    previousOI = units[0]['streamOI']
    previousTime = units[0]['trade_time_ms']
    newOpen = units[0]['price']
    price = units[-1]['price']
    previousDeltaCum = 0

    newStart  = units[0]['trade_time_ms']
    newClose = units[-1]['trade_time_ms']
    timeDelta = newClose - newStart
    timeDelta2 = newClose - previousTime

    #print('BLOCK DATA 2')
    if len(blocks) > 1:
        if mode == 'carry':
            lastBlock = blocks[-1] # when carrying there is no current candle
        else:
            lastBlock = blocks[-2] # ignore last unit which is the current one

        previousOI = lastBlock['oi_cumulative']
        previousDeltaCum = lastBlock['delta_cumulative']
        previousTime = lastBlock['trade_time_ms']
        newOpen = lastBlock['close']

    buyCount = 0
    sellCount = 0
    highPrice = 0
    lowPrice = 0

    OIopen = 0
    OIclose = 0
    OIhigh = 0
    OIlow = 0

    tradecount = 0
    blocktradecount = {}
    onedollarcount = 0

    for d in units:
        # print('BLOCK LOOP', d)

        if d['blocktrade'] == "true":
            blocktradecount[d['trade_time_ms']] = d['size']

        if d['size'] == 1:
            onedollarcount +=1

        if d['side'] == 'Buy':
            buyCount += d['size']
        else:
            sellCount += d['size']

        dPrice = d['price']

        if tradecount == 0:
            highPrice = dPrice
            lowPrice = dPrice
            OIopen = d['streamOI']
            OIhigh = d['streamOI']
            OIlow = d['streamOI']
        else:
            if dPrice > highPrice:
                highPrice = dPrice
            if dPrice < lowPrice:
                lowPrice = dPrice

            if d['streamOI'] > OIhigh:
                OIhigh = d['streamOI']
            if d['streamOI'] < OIlow:
                OIlow = d['streamOI']

            OIclose = d['streamOI']

        tradecount += 1

    delta = buyCount - sellCount
    OIdelta =  OIclose - previousOI

    newCandle = {
        'trade_time_ms' : newClose,
        'timestamp' : str(units[0]['timestamp']),
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
        'oi_delta': OIdelta,
        'oi_high': OIhigh,
        'oi_low': OIlow,
        'oi_open': OIopen,
        'oi_cumulative': OIclose,
        'divergence' : CVDdivergence,
        'switch' : switch,
        'volcandle_two' : {},
        'volcandle_five' : {},
        'pva_status': {},
        'tradecount': tradecount,
        'blocktradecount': blocktradecount,
        'onedollar' : onedollarcount
    }

    if 'block' in mode:
        print('NEW CANDLE: ' + mode + ' ' newCandle['timestamp'])

    if mode == 'volblock' or mode == 'carry':
        try:
            blockSize = 1_000_000
            if LOCAL:
                blockSize = 100_000

            blocks2m = json.loads(r.get('volumeblocks2m'))
            if len(blocks2m) == 0:
                blocks2m.append(newCandle)
            elif blocks2m[-1]['total'] < blockSize * 2:
                newCandle['volcandle_two'] = addBlockBlock(blocks2m, newCandle, newClose, 2)
            elif blocks2m[-1]['total'] == blockSize * 2:
                blocks2m.append(newCandle)

            r.set('volumeblocks2m', json.dumps(blocks2m))

            blocks5m = json.loads(r.get('volumeblocks5m'))
            if len(blocks5m) == 0:
                blocks5m.append(newCandle)
            elif blocks5m[-1]['total'] < blockSize * 5:
                newCandle['volcandle_five'] = addBlockBlock(blocks5m, newCandle, newClose, 5)
            elif blocks5m[-1]['total'] == blockSize * 5:
                blocks5m.append(newCandle)

            r.set('volumeblocks5m', json.dumps(blocks5m))

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

        pva150 = False
        pva200 = False
        divergenceBull = False
        divergenceBear = False
        flatOI = False

        percentage = round((lastVolume/(sumVolume/10)), 2)
        deltapercentage = round((lastDelta/lastVolume)*100, 2)

        if percentage > 2:
            pva200 = True
            if lastOIDelta < 100000  and lastOIDelta > - 100000:
                flatOI = True
        elif percentage > 1.5:
            pva150 = True

        if lastDelta > 0 and lastPriceDelta < 0:
            divergenceBear = True
        elif lastDelta < 0 and lastPriceDelta > 0:
            divergenceBull = True

        returnPVA = {
            'pva150' : pva150,
            'pva200' : pva200,
            'vol': lastVolume,
            'percentage' : percentage,
            'deltapercentge' : deltapercentage,
            'PVAbearDIV' : divergenceBear,
            'PVAbullDIV' : divergenceBull,
            'flatOI' : flatOI
            }

        print('RETURN PVA')

        if pva200 and flatOI and lastVolume > 1_000_000:
            r.set('discord', 'PVA flatOI: ' + str(returnPVA['vol']) + ' ' + str(returnPVA['percentage']*100) + '%')
        elif pva200 and divergenceBear and lastVolume > 1_000_000:
            r.set('discord', 'PVA divergence Bear: ' +  str(returnPVA['vol']) + ' ' + str(returnPVA['percentage']))
        elif pva200 and divergenceBull and lastVolume > 1_000_000:
            r.set('discord', 'PVA divergence Bull: ' +  str(returnPVA['vol']) + ' ' + str(returnPVA['percentage']))

        return returnPVA

    except:
        return {}


def logTimeUnit(newUnit):
    # print('ADD TIME FLOW')

    # add a new unit which is msg from handle_message

    timeflow =  json.loads(r.get('timeflow')) # []
    timeblocks = json.loads(r.get('timeblocks')) # []

    # print('TIME REDIS', len(timeflow), len(timeblocks))

    if len(timeflow) == 0:
        print('TIME 0')

        ## start the initial time flow and initial current candle
        timeflow.append(newUnit)
        currentCandle = addBlock(timeflow, timeblocks, 'timemode')
        timeblocks.append(currentCandle)

        r.set('timeblocks', json.dumps(timeblocks))
        r.set('timeflow', json.dumps(timeflow))
    else:
        blockStart = timeflow[0]['trade_time_ms']
        if LOCAL:
            interval = (60000*1) # 1Min
        else:
            interval = (60000*5) # 5Min
        blockFinish = blockStart + interval

        # print('TIME 1')
        if newUnit['trade_time_ms'] >= blockFinish: # store current candle and start a new Candle
            print('ADD TIME CANDLE')

            # replace current candle with completed candle
            newCandle = addBlock(timeflow, timeblocks, 'timeblock')
            LastIndex = len(timeblocks) - 1
            timeblocks[LastIndex] = newCandle

            timeblocks[LastIndex]['pva_status'] = getPVAstatus(timeblocks)

            # reset timeflow and add new unit
            timeflow = []
            newUnit['trade_time_ms'] = blockFinish
            timeflow.append(newUnit)

            # add fresh current candle to timeblock
            currentCandle = addBlock(timeflow, timeblocks, 'timemode')
            timeblocks.append(currentCandle)
            print('TIME FLOW RESET: ' + str(len(timeflow)) + ' ' + str(len(timeblocks)))
            r.set('timeblocks', json.dumps(timeblocks))
            r.set('timeflow', json.dumps(timeflow))

        else: # add the unit to the time flow

            # print('ADD TIME UNIT')
            timeflow.append(newUnit)

            # update current candle with new unit data
            currentCandle = addBlock(timeflow, timeblocks, 'timemode')
            LastIndex = len(timeblocks) - 1
            timeblocks[LastIndex] = currentCandle
            r.set('timeblocks', json.dumps(timeblocks))
            r.set('timeflow', json.dumps(timeflow))


def getDeltaStatus(deltaflow):
    print('GET DELTA STATUS')

    deltaBlock = 1_000_000

    if LOCAL:
        deltaBlock = 100_000


    totalBuys = 0
    totalSells = 0
    negDelta = False
    posDelta = False

    for d in deltaflow:
        if d['side'] == 'Buy':
            totalBuys += d['size']
        if d['side'] == 'Sell':
            totalSells += d['size']

    if totalBuys - totalSells < - deltaBlock:
        negDelta = True


    if totalBuys - totalSells > deltaBlock:
        posDelta = True

    return {
            'flowdelta' : totalBuys - totalSells,
            'negDelta' : negDelta,
            'posDelta' : posDelta
    }


def logDeltaUnit(newUnit):

    # add a new unit which is msg from handle_message

    deltaflow =  json.loads(r.get('deltaflow')) # []
    deltablocks = json.loads(r.get('deltablocks')) # []

    if LOCAL:
        print('DELTA REDIS', len(deltaflow), len(deltablocks))

    if len(deltaflow) == 0:
        print('DELTA 0')

        ## start the initial time flow and initial current candle
        deltaflow.append(newUnit)
        currentCandle = addBlock(deltaflow, deltablocks, 'deltamode')
        deltablocks.append(currentCandle)

        r.set('deltablocks', json.dumps(deltablocks))
        r.set('deltaflow', json.dumps(deltaflow))
    else:

        deltaflow.append(newUnit)

        deltaStatus = getDeltaStatus(deltaflow)

        print('DELTA 1')

        if deltaStatus['posDelta'] or deltaStatus['negDelta']:
            # store current candle and start a new Candle
            print('ADD DELTA CANDLE: ' + json.dumps(deltaStatus))
            if LOCAL:
                r.set('discord', 'NEW DELTA: ' +  json.dumps(deltaStatus))

            # replace current candle with completed candle
            newCandle = addBlock(deltaflow, deltablocks, 'deltablock')
            LastIndex = len(deltablocks) - 1
            deltablocks[LastIndex] = newCandle

            # reset deltaflow
            deltaflow = []

            # add fresh current candle to timeblock
            if LOCAL:
                print('DELTA FLOW RESET', len(deltaflow), len(deltablocks))
            r.set('deltablocks', json.dumps(deltablocks))
            r.set('deltaflow', json.dumps(deltaflow))

        else: # add the unit to the delta flow

            print('ADD DELTA UNIT')

            # update current candle with new unit data
            currentCandle = addBlock(deltaflow, deltablocks, 'deltamode')
            LastIndex = len(deltablocks) - 1
            deltablocks[LastIndex] = currentCandle
            r.set('deltablocks', json.dumps(deltablocks))
            r.set('deltaflow', json.dumps(deltaflow))


def getPreviousDay(blocks):

    try:
        dailyOpen = blocks[0]['open']
        dailyClose = blocks[-1]['close']
        dailyPriceDelta = dailyClose - dailyOpen
        dailyCVD = blocks[-1]['delta_cumulative']
        dailyDIV = False

        if dailyPriceDelta < 0 and dailyCVD > 0:
            dailyDIV = True
        elif dailyPriceDelta > 0 and dailyCVD < 0:
            dailyDIV = True

        dailyVolume = 0

        for b in blocks:
            dailyVolume += b['total']

        return json.dumps({'vol' : dailyVolume, 'cvd' : dailyCVD, 'price' : dailyPriceDelta })

    except:
        return 'getPreviousDay() fail'


def historyReset():
    current_time = dt.datetime.utcnow()
    print('Current Time UTC', current_time, current_time.hour, current_time.minute)
    dt_string = current_time.strftime("%d/%m/%Y")

    if current_time.hour == 23 and current_time.minute == 59:
        history = json.loads(r.get('history'))

        vb = json.loads(r.get('volumeblocks'))
        tb = json.loads(r.get('timeblocks'))
        db = json.loads(r.get('deltablocks'))

        pdDict = {
                    'date' : dt_string,
                    'volumeblocks' : vb,
                    'timeblocks' : tb,
                    'deltablocks' : db
                }

        if len(history) > 0:
            lastHistory = json.loads(r.get('history'))[len(history)-1]

            if lastHistory['date'] != dt_string:
                print('REDIS STORE', dt_string)

                history.append(pdDict)

                pd = getPreviousDay(tb)

                r.set('history', json.dumps(history))
                r.set('discord', 'history log: ' + pd)
        else:
            print('REDIS STORE INITIAL')

            history.append(pdDict)

            pd = getPreviousDay(tb)

            r.set('history', json.dumps(history))
            r.set('discord', 'history log: ' + pd)

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
            r.set('deltaflow', json.dumps([]) )  # this the flow of message data to create next candle
            r.set('deltablocks', json.dumps([]) ) # this is the store of new time based candles
            r.set('newDay', dt_string)
            r.set('discord', 'new day')

    return True


def handle_trade_message(msg):

    ### check time and reset
    historyReset()

    print('handle_trade_message')
    # print(msg['data'])

    sess = session.latest_information_for_symbol(symbol="BTCUSD")

    streamTime = round(float(sess['time_now']), 1)
    streamPrice = sess['result'][0]['last_price']
    streamOI = sess['result'][0]['open_interest']

    manageStream(streamTime, streamPrice, streamOI)

    if LOCAL:
        block = 100_000
    else:
        block = 1_000_000

    ## load vol flow
    volumeflow = json.loads(r.get('volumeflow')) ## reset after each volume block

    ## calculate current candle size
    volumeflowTotal = 0
    for t in volumeflow:
        volumeflowTotal += t['size']

    ## run through data
    for x in msg['data']:
        if x['size'] > 100_000:
            print('msg: ' + str(x['side']) + ' ' + str(x['size']) )

        ## look for big blocks
        if x['size'] > block/10 and not LOCAL:

            bString = x['side'] + ': ' + str(round(x['size']/1000)) + 'k'
            print('Large Trade: ' + bString)
            r.set('discord',  'Large Trade: ' + bString)

        timestamp = x['timestamp']
        ts = str(datetime.strptime(timestamp.split('.')[0], "%Y-%m-%dT%H:%M:%S"))
        price = round(float(x['price'])*2)/2

        newUnit = {
                    'side' : x['side'] ,
                    'size' : x['size'] ,
                    'trade_time_ms' : x['trade_time_ms'],
                    'timestamp' : ts,
                    'price' : price,
                    'blocktrade' : x['is_block_trade'],
                    'streamTime' : streamTime,
                    'streamPrice' : streamPrice,
                    'streamOI' : streamOI
                }

        # send message to time candle log
        logTimeUnit(newUnit)
        # logDeltaUnit(newUnit)

        if volumeflowTotal + x['size'] <= block:
            # Normal addition of trade to volume flow
            # print(volumeflowTotal, '< Block')

            volumeflow.append(newUnit)

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

            newUnit['size'] = lefttoFill

            volumeflow.append(newUnit)

            volumeblocks = json.loads(r.get('volumeblocks'))
            LastIndex = len(volumeblocks) - 1
            print('VOL BLOCK BREAK')
            newCandle = addBlock(volumeflow, volumeblocks, 'volblock')
            volumeblocks[LastIndex] = newCandle  # replace last candle (current) with completed

            r.set('volumeblocks', json.dumps(volumeblocks))

            ## volume flow has been added as  full candle and should be reset
            volumeflow = []
            volumeflowTotal = 0

            if carryOver > 200_000:
                r.set('discord', 'Carry Over: ' + str(carryOver))
            ## Note: volumeblock does not have a current candle at thsi point
            for y in range(carryOver//block):

                r.set('discord', 'Carry Over: ' + str(carryOver//block))

                ## this is volume flow list - just one block
                fullTradeList =  [{
                     'side' : x['side'] ,
                     'size' : block,
                     'trade_time_ms' : x['trade_time_ms'],
                     'timestamp' : ts,
                     'price' : price,
                     'blocktrade' : 'CARRY OVER',
                     'streamTime' : streamTime,
                     'streamPrice' : streamPrice,
                     'streamOI' : streamOI
                     }
                ]
                ## keep appending large blocks
                volumeblocks = json.loads(r.get('volumeblocks'))
                newCandle = addBlock(fullTradeList, volumeblocks, 'carry')
                volumeblocks.append(newCandle)
                r.set('volumeblocks', json.dumps(volumeblocks))

                print('Add Block: ' + str(y) )

            # Create new flow block with left over contracts
            volumeflow = [
                    { 'side' : x['side'] ,
                    'size' : carryOver%block,
                    'trade_time_ms' : x['trade_time_ms'],
                    'timestamp' : ts,
                    'price' : price,
                    'blocktrade' : x['is_block_trade'],
                    'streamTime' : streamTime,
                    'streamPrice' : streamPrice,
                    'streamOI' : streamOI
                    }
                ]

            volumeblocks = json.loads(r.get('volumeblocks'))
            currentCandle = addBlock(volumeflow, volumeblocks, 'vol')
            volumeblocks.append(currentCandle)
            r.set('volumeblocks', json.dumps(volumeblocks))

            volumeflowTotal = carryOver%block


    r.set('volumeflow', json.dumps(volumeflow))



def pingTest():
    hosts = ['rekt-app.onrender.com', 'rektbit.onrender.com']
    for h in hosts:
        ping(h, verbose=True)


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

    @tasks.loop(seconds=3)
    async def checkRedis(user):
        print('DISCORD REDIS CHECK')

        if r.get('discord') != 'blank':
            await user.send(r.get('discord'))
            r.set('discord', 'blank')

    @bot.event
    async def on_message(msg):
        user = bot.get_user(int(DISCORD_USER))
        print('MESSAGE DDDDDDDDD', msg.content)
        replyText = 'ho'

        if msg.content == 'delta on':
            r.set('discord_delta', 'on')
        if msg.content == 'delta off':
            r.set('discord_delta', 'off')

        if msg.author == user:
            await user.send(replyText)


    bot.run(DISCORD_TOKEN)


@app.task() #bind=True, base=AbortableTask  // (self)
def runStream():

    print('RUN_STREAM')

    rDict = {
        'lastPrice' : 0,
        'lastTime' : 0,
        'lastOI' : 0,
        '1mOI' : [],
        'oiMarker' : 1000000,
        'Divs' : {}
    }

    r.set('discord_filter',  'off')
    r.set('stream', json.dumps(rDict) )

    r.set('volumeflow', json.dumps([]) )  # this the flow of message data for volume candles
    r.set('volumeblocks2m', json.dumps([]) )  #  this is the store of volume based candles
    r.set('volumeblocks5m', json.dumps([]) )  #  this is the store of volume based candles
    r.set('volumeblocks', json.dumps([]) )  #  this is the store of volume based candles
    r.set('timeflow', json.dumps([]) )  # this the flow of message data to create next candle
    r.set('timeblocks', json.dumps([]) ) # this is the store of new time based candles
    r.set('deltaflow', json.dumps([]) )
    r.set('deltablocks', json.dumps([]) )

    # r.set('history', json.dumps([]) )


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





