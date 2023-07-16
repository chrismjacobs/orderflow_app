import os, json, math
from celery import Celery
from celery.utils.log import get_task_logger
from celery.contrib.abortable import AbortableTask
from time import sleep
from pybit import inverse_perpetual, usdt_perpetual
import datetime as dt
from datetime import datetime
import redis
import time

from math import trunc
from taskAux import actionBIT, actionDELTA, actionVOLUME, startDiscord, sendMessage, setCoinDict


session = inverse_perpetual.HTTP(
    endpoint='https://api.bybit.com'
)

LOCAL = False

try:
    import config
    LOCAL = True
    if LOCAL:
        REDIS_URL = config.REDIS_URL_TEST
    else:
        REDIS_URL = config.REDIS_URL
    # r = redis.from_url(REDIS_URL, ssl_cert_reqs=None, decode_responses=True)

    DISCORD_CHANNEL = config.DISCORD_CHANNEL
    DISCORD_TOKEN = config.DISCORD_TOKEN
    DISCORD_USER = config.DISCORD_USER
    REDIS_IP = config.REDIS_IP
    REDIS_PASS = config.REDIS_PASS

except:
    # REDIS_URL = os.getenv('CELERY_BROKER_URL')
    # rRender = redis.from_url(REDIS_URL, decode_responses=True)
    DISCORD_CHANNEL = os.getenv('DISCORD_CHANNEL')
    DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
    DISCORD_USER = os.getenv('DISCORD_USER')
    REDIS_IP = os.getenv('REDIS_IP')
    REDIS_PASS = os.getenv('REDIS_PASS')

r = redis.Redis(
    host=REDIS_IP,
    port=6379,
    password=REDIS_PASS,
    decode_responses=True
    )



print('REDIS', r)


app = Celery('tasks', broker=REDIS_IP, backend=REDIS_IP, password='HBeUHgPoBlbI')
logger = get_task_logger(__name__)

def getHiLow(timeblocks, coin):

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
        if count <= 23: ### looks at past two hours
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

    oih = 0
    oil = 0

    try:
        oih = tbRev[0]['oi_cumulative'] - tbRev[LH2h_index]['oi_cumulative']
        oih = tbRev[0]['oi_cumulative'] - tbRev[LL2h_index]['oi_cumulative']
    except:
        print('OI count FAIL')


    highInfo = {
        'price' : LH2h,
        'index' : LH2h_index,
        'delta' : LH2h_cvd,
        'oi' : oih,
        'div' : False
    }

    lowInfo = {
        'price' : LL2h,
        'index' : LL2h_index,
        'delta' : LL2h_cvd,
        'oi' : oil,
        'div' : False
    }

    if LH2h_index >= 2:
        # current timeblock nor the previous is not the highest/lowest
        if tbRev[0]['delta_cumulative'] > LH2h_cvd:
            # Divergence Triggered
            highInfo['div'] = True

            streamAlert('CVD Bear div: ' + json.dumps(highInfo), 'CVD Divergence', coin)
            if coin == 'BTC':
                r.set('discord_' + coin, coin + ' CVD BEAR div')  #: '  + json.dumps(highInfo))

    if LL2h_index >= 2:
        if tbRev[0]['delta_cumulative'] < LL2h_cvd:
            # Divergence Triggered
            lowInfo['div'] = True
            streamAlert('CVD Bull div: ' + json.dumps(lowInfo), 'CVD Divergence', coin)

            if coin == 'BTC':
                r.set('discord_' + coin, coin + ' CVD BULL div') # : ' + json.dumps(lowInfo))


    return {'highInfo' : highInfo , 'lowInfo' : lowInfo }


def getHistory(coin):
    # print('GET HISTORY ' + coin)

    historyBlocks = json.loads(r.get('history_' + coin))
    ## -->  each day
    ## a list of dictionaries

    if historyBlocks and len(historyBlocks) > 0:
        return historyBlocks[-1]
    else:
        return False


def streamAlert(message, mode, coin):
    # print('Alert Stream ' + mode + ' ' + coin)
    stream = json.loads(r.get('stream_' + coin))

    current_time = dt.datetime.utcnow()
    # print('Current Time UTC Alert : ' + str(current_time).split('.')[0])

    alertList = stream['alerts']
    alertMessage = [str(current_time), mode, message]

    alertList.insert(0, alertMessage)

    if len(alertList) > 5:
        alertList.pop()

    r.set('stream_' + coin, json.dumps(stream) )


    ''' alerts notes '''
    # sudden OI change - looks at current candle or infact previous candle if time just passed -
    # perhaps calculate the likely reason

def manageStream(streamTime, streamPrice, streamOI, coin):

    timeblocks = json.loads(r.get('timeblocks_' + coin))
    currentBuys = 0
    currentSells = 0
    if len(timeblocks) > 1:
        currentBuys = timeblocks[-1]['buys']
        currentSells = timeblocks[-1]['sells']
        currentBuys += timeblocks[-2]['buys']
        currentSells += timeblocks[-2]['sells']

    # print('Manage Stream')
    stream = json.loads(r.get('stream_' + coin))
    stream['lastTime'] = streamTime
    stream['lastPrice'] = streamPrice
    stream['lastOI'] = streamOI

    if len(stream['1mOI']) < 2:
        print('INITIAL')
        stream['1mOI'] = [streamTime, streamOI]
    elif streamTime - stream['1mOI'][0] >= 90:

        deltaOI =  streamOI - stream['1mOI'][1]
        deltaOIstr = str(round(deltaOI/100_000)/10) + 'm '
        deltaBuyStr = str(round(currentBuys/100_000)/10) + 'm '
        deltaSellStr = str(round(currentSells/100_000)/10) + 'm '

        if stream['oiMarkers'][0] > 0 and deltaOI > stream['oiMarkers'][0]:
            message = 'OI INC: ' + deltaOIstr + ' Buys:' + deltaBuyStr + ' Sells: ' + deltaSellStr + ' Price: ' + str(stream['lastPrice'])
            sendMessage(coin, message, '', 'blue')
            streamAlert(message, 'OI', coin)

        if stream['oiMarkers'][1] > 0 and deltaOI < - stream['oiMarkers'][1]:
            message = 'OI DEC: ' + deltaOIstr + ' Buys: ' + deltaBuyStr + ' Sells: ' + deltaSellStr  + ' Price: ' + str(stream['lastPrice'])
            sendMessage(coin, message, '', 'pink')
            streamAlert(message, 'OI', coin)


        stream['1mOI'] = [streamTime, streamOI]

    else:
        stream['oi delta'] = [round(streamTime - stream['1mOI'][0]), streamOI - stream['1mOI'][1], '(secs/oi)' ]

    # print(stream)
    r.set('stream_' + coin, json.dumps(stream) )

    return True

def addDeltaBIT(blocks, coin, coinDict):

    switch = False

    switchUp = False
    switchDown = False


    if len(blocks) > 3:
        if blocks[-2]['delta'] > 0 and blocks[-3]['delta'] > 0 and blocks[-4]['delta'] > 0:
            if blocks[-1]['delta'] < 0:
                switchDown = True
                actionBIT('Sell')
        if blocks[-2]['delta'] < 0 and blocks[-3]['delta'] < 0 and blocks[-4]['delta'] < 0:
            if blocks[-1]['delta'] > 0:
                switchUp = True
                actionBIT('Buy')

    return switch


def getImbalances(tickList, mode):
    if LOCAL:
        print('IMBALANCES')

    ticks = len(tickList)
    # 1 2 3

    ## Buys cannot be last
    ## Sells cannot be at the top

    for i in range(ticks):  # 0 1 2
        if i + 1 < ticks:
            BIbuys = tickList[i]['Buy']
            BIsells = tickList[i + 1]['Sell']

            if BIsells == 0:
                BIsells = 1

            BIpct = round((BIbuys / BIsells) * 100)
            if BIpct > 1000:
                BIpct = 1000

            tickList[i]['BuyPer'] = BIpct

            SIbuys = tickList[i]['Buy']
            SIsells = tickList[i + 1]['Sell']

            if SIbuys == 0:
                SIbuys = 1

            SIpct = round((SIsells / SIbuys) * 100)
            if SIpct > 1000:
                SIpct = 1000

            tickList[i + 1]['SellPer'] = SIpct

    stackBuys = 0
    stackSells = 0

    if 'block' in mode:

        for t in tickList:
            if t['SellPer'] > 369:
                stackSells += 1
            if t['SellPer'] < 369 and stackSells <= 2:
                stackSells = 0
            if t['BuyPer'] > 369:
                stackBuys += 1
            if t['BuyPer'] < 369 and stackBuys <= 2:
                stackBuys = 0


    return [tickList, stackBuys, stackSells]

def getVWAP(timeblocks, coin):

    volumeCum = 0
    vwapVolumeCum = 0

    for t in timeblocks:

        volumeCum += t['total']
        t['pivot'] = (t['high'] + t['low'] + t['close'])/3
        vwapVolume = t['pivot']*t['total']
        vwapVolumeCum += vwapVolume
        vwapPrice = vwapVolumeCum/volumeCum
        t['vwap'] = vwapPrice
        # if coin == 'BTC':
        #     t['vwap_task'] = str(trunc(vwapPrice/10)*10)
        # elif coin == 'ETH':
        #     t['vwap_task']  = math.floor(vwapPrice)

    return round(timeblocks[-1]['vwap'])

## addBlockk
def addBlock(units, blocks, mode, coin):

    TIME = False
    VOLUME = False
    BLOCK = False

    coinDict = json.loads(r.get('coinDict'))

    pause = coinDict[coin]['pause']
    getTickImbs = coinDict[coin]['imbalances']

    modeSplit = mode.split('_')
    mode = modeSplit[0]

    if 'time' in mode:
        TIME = True
    elif 'vol' in mode:
        VOLUME = True

    if 'block' in mode:
        BLOCK = True


    CVDdivergence = {}

    if TIME and BLOCK and pause == False:
        CVDdivergence = getHiLow(blocks, coin)
        stream = json.loads(r.get('stream_' + coin))
        stream['Divs'] = CVDdivergence
        r.set('stream_' + coin, json.dumps(stream) )


    ''' BLOCK DATA '''

    # print('BLOCK DATA: ' + mode + ' -- ' + coin)
    previousOICum = units[0]['streamOI']
    previousTime = units[0]['trade_time_ms']
    vwap_task = 0
    newOpen = units[0]['streamPrice']
    price = units[-1]['streamPrice']
    previousDeltaCum = 0

    ## if just one block than that is the current candle
    ## last block is the previous one
    ## but if its the start of the day then we need to get Historical last block

    if len(blocks) > 1:
        if mode == 'carry':
            lastCandle = blocks[-1] # when carrying a volume block there is no current candle
        else:
            lastCandle = blocks[-2] # ignore last unit which is the current one
        previousDeltaCum = lastCandle['delta_cumulative']
        vwap_task = lastCandle['vwap_task']
        previousOICum = lastCandle['oi_cumulative']
        previousTime = lastCandle['trade_time_ms']
        newOpen = lastCandle['close']
    elif TIME:
        h = getHistory(coin)
        if h:
            lastCandle = h['timeblocks_' + coin][-1]
            previousDeltaCum = lastCandle['delta_cumulative']
            previousOICum = lastCandle['oi_cumulative']
            if lastCandle['vwap_task']:
                vwap_task = lastCandle['vwap_task']


    newStart  = units[0]['trade_time_ms']
    newClose = units[-1]['trade_time_ms']

    # if LOCAL:
    #     print('TIME CHECK', previousTime, newClose, newStart, type(newClose), type(newStart))

    timeDelta = newClose - newStart
    timeDelta2 = newClose - previousTime

    buyCount = 0
    sellCount = 0
    highPrice = 0
    lowPrice = 0

    OIclose = 0
    OIhigh = 0
    OIlow = 0

    tradecount = 0

    tickDict = {}

    oiList = []

    priceList = []

    tickCoins = ['BTC']


    for d in units:
        # print('BLOCK LOOP', d)

        if d['side'] == 'Buy':
            buyCount += d['size']
        else:
            sellCount += d['size']

        for price in d['spread']:

            oiList.append(d['streamOI'])
            OIclose = d['streamOI']

            price = float(price)
            priceList.append(price)

            # print('CHECK SPREAD TICKS', price, type(price) )

            if coin in tickCoins:
                # print('TICKES', tickDict, tickPrice)
                if coin == 'BTC':
                    tickPrice = str(trunc(price/10)*10)

                elif coin == 'ETH':
                    ##  1159.56 --> 1159.25
                    tickPrice = math.floor(price)


                if tickPrice not in tickDict:

                    tickDict[tickPrice] = {
                        'tickPrice' : tickPrice,
                        'Sell'  : 0,
                        'Buy' : 0,
                        'SellPer' : 0,
                        'BuyPer' : 0
                    }

                tickDict[tickPrice][d['side']] += d['spread'][str(price)]
                ## the spread keys come back as strings

    highPrice = max(priceList)
    lowPrice = min(priceList)
    total = buyCount + sellCount

    oiList.sort()

    OIlow = oiList[0]
    OIhigh = oiList[-1]

    delta = buyCount - sellCount
    OIdelta =  OIclose - previousOICum

    priceDelta = price - newOpen

    if coin == 'ETH':
        priceDelta = round(priceDelta*100)/100
    if coin == 'GALA':
        priceDelta = round(priceDelta*10000)/10000

    tickList = []

    if coin in tickCoins:
        #print('TICKS SORT', mode, size)

        tickKeys = list(tickDict.keys())
        tickKeys.sort(reverse = True)

        # print('SORT DATA ' + str(priceList))

        for p in tickKeys:
            tickList.append(tickDict[p])

        stack = r.get('stack')
        if not stack:
            stack = 'off'
            r.set('stack', stack)

        if stack and total > 2_000_000 and OIdelta > 0:
            getIMBs = getImbalances(tickList, mode)
            tickList = getIMBs[0]
            stackBuys = getIMBs[1]
            stackSells = getIMBs[2]

            if stackBuys >= 3 and 'time' in mode:
                sendMessage(coin, 'Stack IMBS BUY', '', 'white')
            if stackSells >= 3 and 'time' in mode:
                sendMessage(coin, 'Stack IMBS SELL', '', 'white')





    if BLOCK:
        try:
            vwap_task = getVWAP(blocks, coin)
        except:
            print('vwap exception')


    newCandle = {
        'trade_time_ms' : newClose,
        'timestamp' : str(units[0]['timestamp']),
        'time_delta' : timeDelta,
        'close' : price,
        'open' : newOpen,
        'price_delta' : priceDelta,
        'high' : highPrice,
        'low' : lowPrice,
        'buys' : buyCount,
        'sells' : sellCount,
        'delta' : delta,
        'delta_cumulative' : int(previousDeltaCum + delta),
        'total' : total,
        'vwap_task' : vwap_task,
        'oi_delta': OIdelta,
        'oi_high': OIhigh,
        'oi_low': OIlow,
        'oi_open': previousOICum,
        'oi_range': OIhigh - OIlow,
        'oi_cumulative': OIclose,
        'divergence' : CVDdivergence,
        'tickList' : tickList,
        'pva_status': {},
        'volDiv' : False,
        'switch' : False
        #'tradecount': tradecount,
    }

    # if 'block' in mode:
    #     print('NEW CANDLE: ' + mode + ' ' + coin)

    bullDiv = False
    bearDiv = False

    volblockcandle = mode == 'volblock' or mode == 'carry'

    if coin == 'BTC' and volblockcandle:

        # print('VOL DIV CHECK')

        deltaPercent = round( (  newCandle['delta']  /  newCandle['total']  ) * 100  )

        if abs(deltaPercent) > 20:
            # print('VOL DIV CHECK 2')
            timeSecs = round(newCandle['time_delta']/1000)
            oiCheck = round(newCandle['oi_delta']/1000)
            tots = round(newCandle['total']/1_000_000)

            if newCandle['delta'] < 0 and newCandle['price_delta'] > 0 and newCandle['time_delta'] > 30000:
                newCandle['volDiv'] = True
                bullDiv = True
                msg = coin + ' BULL VOL ' + str(tots) + ' Delta ' + str(deltaPercent) + '% ' + str(newCandle['price_delta']) + '$ ' + str(timeSecs) + ' secs  OI: ' + str(oiCheck)
                if newCandle['total'] > 2_500_000:
                    sendMessage(coin, msg, '', 'green')

            # print('VOL DIV CHECK 3')
            if newCandle['delta'] > 0 and newCandle['price_delta'] < 0 and newCandle['time_delta'] > 30000:
                newCandle['volDiv'] = True
                bearDiv = True
                msg = coin + ' BEAR VOL ' + str(tots) + ' Delta ' + str(deltaPercent) + '% ' + str(newCandle['price_delta']) + '$ ' + str(timeSecs) + ' secs  OI: ' + str(oiCheck)
                if newCandle['total'] > 2_500_000:
                    sendMessage(coin, msg, '', 'red')

            # print('VOL DIV CHECK COMPLETE')

        try:
            newCandle['switch'] = actionVOLUME(blocks, coin, coinDict, bullDiv, bearDiv)
        except:
            print('ACTION VOLUME EXCEPTION')

    if TIME:
        if newCandle['total'] > 50_000_000 and pause == False:
            if coinDict['ETH']:
                coinDict['ETH']['active'] = False
            coinDict['BTC']['pause'] = True
            r.set('coinDict', json.dumps(coinDict))
        elif pause == True and newCandle['total'] < 50_000_000:
            coinDict['BTC']['pause'] = False
            r.set('coinDict', json.dumps(coinDict))

    return newCandle

def addDeltaBlock(units, blocks, deltaCount, coin):

    # units == flow

    coinDict = json.loads(r.get('coinDict'))


    ''' BLOCK DATA '''

    #  print('BLOCK DATA: ' + mode + ' -- ' + coin)

    previousTime = units[0]['trade_time_ms']
    newOpen = units[0]['streamPrice']
    price = units[-1]['streamPrice']

    ## if just one block than that is the current candle
    ## if last candle is not filled then get previous candle

    lastCandleisBlock = True
    lastOI = 0

    if len(blocks) > 1:
        lastCandle = blocks[-1]
        lastCandleisBlock = lastCandle['delta'] == deltaCount or lastCandle['delta'] == -deltaCount
        lastOI = lastCandle['oi_close']

        if not lastCandleisBlock:
            lastCandle = blocks[-2] # ignore last unit which is the current one
            lastOI = lastCandle['oi_close']

        previousTime = lastCandle['trade_time_ms']
        newOpen = lastCandle['close']


    newStart  = units[0]['trade_time_ms']
    newClose = units[-1]['trade_time_ms']

    # if LOCAL:
    #     print('TIME CHECK', previousTime, newClose, newStart, type(newClose), type(newStart))

    timeDelta = newClose - newStart

    buyCount = 0
    sellCount = 0
    highPrice = 0
    lowPrice = 0

    priceList = []

    tradecount = 0

    for d in units:
        # print('BLOCK LOOP', d)

        tradecount += d['tradecount']

        if d['side'] == 'Buy':
            buyCount += d['size']
        else:
            sellCount += d['size']

        for price in d['spread']:

            price = float(price)
            priceList.append(price)


    highPrice = max(priceList)
    lowPrice = min(priceList)

    delta = buyCount - sellCount
    priceDelta = price - newOpen

    switch = False

    sess = session.latest_information_for_symbol(symbol='BTCUSD')
    streamOI = sess['result'][0]['open_interest']
    deltaOI = streamOI - lastOI



    newDeltaCandle = {
        'trade_time_ms' : newClose,
        'timestamp' : str(units[0]['timestamp']),
        'time_delta' : timeDelta,
        'close' : price,
        'open' : newOpen,
        'price_delta' : priceDelta,
        'high' : highPrice,
        'low' : lowPrice,
        'buys' : buyCount,
        'sells' : sellCount,
        'delta' : delta,
        'total' : buyCount + sellCount,
        'switch' : switch,
        'tradecount' : tradecount,
        'oi_delta': deltaOI,
        'oi_close': streamOI,
    }

    newCandleisBlock = delta == deltaCount or delta == -deltaCount

    if coin == 'BTC' and newCandleisBlock:
        newDeltaCandle['switch'] = actionDELTA(blocks, newDeltaCandle, coin, coinDict, lastCandleisBlock)

    return newDeltaCandle


def getPVAstatus(timeblocks, coin):
    if LOCAL:
        print('GET PVA')
    last11blocks = []
    if len(timeblocks) < 11:
        history = json.loads(r.get('history_' + coin))
        try:
            if len(history) > 0:
                lastHistory = history[-1]['timeblocks_' + coin]
                howManyOldTimeblocks = (11-len(timeblocks))
                last11blocks = lastHistory[-howManyOldTimeblocks:] + timeblocks
                # print('LASTBLOCKS HISTORY', last11blocks)
                ## if one time block - get last 10 from history
                ## if 4 time blocks - get last 7 from history
            else:
                return {}
        except:
            # r.set('discord_' + coin, 'History PVA error')
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

    # print('PVA Calculate')

    sumVolume = 1
    lastVolume = 1
    lastDelta = 0
    lastPriceDelta = 0
    lastOIDelta = 0
    lastOIRange = 0

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
                lastOIDelta = x['oi_delta']
                lastOIRange = round((x['oi_high'] - x['oi_low'])/100_000)/10

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
            'deltapercentage' : deltapercentage,
            'PVAbearDIV' : divergenceBear,
            'PVAbullDIV' : divergenceBull,
            'rangeOI' : lastOIRange,
            'flatOI' : flatOI
            }

        if LOCAL:
            print('RETURN PVA')

        volString = str(round(returnPVA['vol']/100_000)/10)

        if pva200 and flatOI and lastVolume > 10_000_000:
            msg = coin + ' PVA flatOI  Vol:' + volString  + ' ' + str(returnPVA['percentage']*100) + '%   OI Range: ' + str(returnPVA['rangeOI']) + 'm'
            sendMessage(coin, msg, '', 'yellow')
            streamAlert('PVA candle with flat OI', 'PVA', coin)
        elif pva200 and divergenceBear and lastVolume > 4_000_000:
            msg = coin + ' PVA divergence Bear: ' + volString  + ' ' + str(returnPVA['percentage'])
            sendMessage(coin, msg, '', 'red')
        elif pva200 and divergenceBull and lastVolume > 4_000_000:
            msg = coin + ' PVA divergence Bull: ' +  volString  + ' ' + str(returnPVA['percentage'])
            sendMessage(coin, msg, '', 'cyan')

        return returnPVA

    except:
        print('PVA ERROR')
        return {}


def logTimeUnit(buyUnit, sellUnit, coin):

    # if not r.get('timeflow_' + coin):
    #     r.set('timeflow_' + coin, json.dumps([]))
    #     r.set('timeblocks_' + coin, json.dumps([]))

    timeflow =  json.loads(r.get('timeflow_' + coin)) # []
    timeblocks = json.loads(r.get('timeblocks_' + coin)) # []

    # print('TIME REDIS', len(timeflow), len(timeblocks))

    if len(timeflow) == 0:
        print('TIME 0 ' + coin)

        ## start the initial time flow and initial current candle
        if buyUnit['size'] > 0:
            timeflow.append(buyUnit)
        if sellUnit['size'] > 0:
            timeflow.append(sellUnit)

        currentCandle = addBlock(timeflow, timeblocks, 'timemode', coin)
        timeblocks.append(currentCandle)

        r.set('timeblocks_' + coin, json.dumps(timeblocks))
        r.set('timeflow_' + coin, json.dumps(timeflow))
    else:
        blockStart = timeflow[0]['trade_time_ms']
        if LOCAL:
            interval = (60000*1) # 1Min
        else:
            interval = (60000*5) # 5Min
        blockFinish = blockStart + interval


        # print('TIME 1')
        if buyUnit['trade_time_ms'] >= blockFinish: # store current candle and start a new Candle
            # print('ADD TIME CANDLE ' + coin)

            # replace current candle with completed candle
            newCandle = addBlock(timeflow, timeblocks, 'timeblock', coin)
            LastIndex = len(timeblocks) - 1
            timeblocks[LastIndex] = newCandle

            timeblocks[LastIndex]['pva_status'] = getPVAstatus(timeblocks, coin)

            # reset timeflow and add new unit
            timeflow = []
            buyUnit['trade_time_ms'] = blockFinish
            sellUnit['trade_time_ms'] = blockFinish
            if buyUnit['size'] > 0:
                timeflow.append(buyUnit)
            if sellUnit['size'] > 0:
                timeflow.append(sellUnit)

            # add fresh current candle to timeblock
            currentCandle = addBlock(timeflow, timeblocks, 'timemode', coin)
            timeblocks.append(currentCandle)
            # print('TIME FLOW RESET: ' + str(len(timeflow)) + ' ' + str(len(timeblocks)))
            r.set('timeblocks_' + coin, json.dumps(timeblocks))
            r.set('timeflow_' + coin, json.dumps(timeflow))

        else: # add the unit to the time flow

            # print('ADD TIME UNIT')
            timeflow.append(buyUnit)
            timeflow.append(sellUnit)

            # update current candle with new unit data
            currentCandle = addBlock(timeflow, timeblocks, 'timemode', coin)
            LastIndex = len(timeblocks) - 1
            timeblocks[LastIndex] = currentCandle
            r.set('timeblocks_' + coin, json.dumps(timeblocks))
            r.set('timeflow_' + coin, json.dumps(timeflow))


def getDeltaStatus(deltaflow, deltaCount):

    # if LOCAL:
    #     print('GET DELTA STATUS', len(deltaflow))

    newDeltaflowList = []

    totalBuys = 0
    totalSells = 0

    blockfill = False
    fillMarker = False # toggle when unfilled block is added at the end


    deltaflowList = [[]]

    for d in deltaflow:

        size = d['size']

        if d['side'] == 'Buy':
            totalBuys += size

        elif d['side'] == 'Sell':
            totalSells += size

        ## 4k Buys
        ## 13K Sells
        ## delta -9K

        # newDeltaflowList.append([
        #     {
        #         "side": d['side'],
        #         "size": d['size'],
        #         'totalBS_ABS' : [str(totalBuys) , str(totalSells), str(abs((totalBuys - totalSells)))]
        #     }
        # ])

        excess = 0

        if totalBuys - totalSells <  - deltaCount or totalBuys - totalSells > deltaCount:
            blockfill = True
            fillMarker = True
            excess = abs(totalBuys - totalSells) - deltaCount


        if blockfill and fillMarker: #posDelta or negDelta:
            ## complete delta flow

            completeUnit = d.copy()

            completeUnit['size'] -= excess
            deltaflowList[-1].append(completeUnit)

            # Excess UNIT SIZE 1084 10352 0 10352 352
            # Excess UNIT SIZE 2 10352 2 10350 350

            while excess >= deltaCount:
                adjustUnit = d.copy()
                adjustUnit['size'] = deltaCount
                excess -= deltaCount
                deltaflowList.append([adjustUnit])
                # print('excess unit added ' + str(excess))


            if excess == 0:
                excess = 1


            finalUnit = d.copy()
            finalUnit['size'] = excess
            deltaflowList.append([finalUnit])
            # print('surplus unit added ' + str(excess))


            # printDict = {
            #     'UNIT SIZE': completeUnit['size'],
            #     'SIDE' : completeUnit['side'],
            #     'LENGTH' : len(deltaflow),
            #     'totalBS' : [totalBuys , totalSells],
            #     'abs' : abs((totalBuys - totalSells)),
            #     'excess' : excess,
            #     'counted unit' : newDeltaflowList,
            #     'currentnewList' : deltaflowList
            # }
            # print('EXCESS ' + json.dumps(printDict))

            fillMarker = False
            totalBuys = 0
            totalSells = 0

        else:
            deltaflowList[-1].append(d)


    return {
            'flowdelta' : totalBuys - totalSells,
            'blockfill' : blockfill,
            'deltaflowList' : deltaflowList
    }


def logDeltaUnit(buyUnit, sellUnit, coin, deltaCount):

    # add a new unit which is msg from handle_message

    dFlow = 'deltaflow_' + coin
    dBlocks = 'deltablocks_' + coin

    if not r.get(dFlow):
        r.set(dFlow, json.dumps([]))
        r.set(dBlocks, json.dumps([]))

    deltaflow =  json.loads(r.get('deltaflow_' + coin)) # []
    deltablocks = json.loads(r.get('deltablocks_' + coin)) # []

    if len(deltaflow) == 0:
        print('DELTA 0')

        ## start the initial delta flow and initial current candle
        if buyUnit['size'] > 1:
            deltaflow.append(buyUnit)
        if sellUnit['size'] > 1:
            deltaflow.append(sellUnit)

        currentCandle = addDeltaBlock(deltaflow, deltablocks, deltaCount, coin)
        deltablocks.append(currentCandle)

        r.set('deltablocks_' + coin, json.dumps(deltablocks))
        r.set('deltaflow_' + coin, json.dumps(deltaflow))
    else:
        if buyUnit['size'] > 1:
            deltaflow.append(buyUnit)
        if sellUnit['size'] > 1:
            deltaflow.append(sellUnit)

        deltaStatus = getDeltaStatus(deltaflow, deltaCount)


        if LOCAL:
            print('DELTA STATUS', len(deltablocks), len(deltaflow), len(deltaStatus['deltaflowList']))

        if deltaStatus['blockfill']:
            # store current candle and start a new Candle

            # replace current candle with completed candle
            dcount = 0
            for flow in deltaStatus['deltaflowList']:
                # for f in flow:
                #     print('DeltaFlowList ' + str(dcount) + ' ' + f['side'] + ' ' + str(f['size']))

                zero = deltaStatus['deltaflowList'].index(flow)
                if zero == 0:

                    print('ADD BLOCK ZERO')

                    newCandle = addDeltaBlock(flow, deltablocks, deltaCount, coin)
                    ### replace last block (unfillled becomes filled)
                    deltablocks[-1] = newCandle

                else:

                    currentCandle = addDeltaBlock(flow, deltablocks, deltaCount, coin)
                    deltablocks.append(currentCandle)
                    if deltaStatus['deltaflowList'].index(flow) == len(deltaStatus['deltaflowList']) - 1:
                        # reset deltaflow - the last delta block
                        deltaflow = flow

                dcount += 1


            r.set('deltablocks_' + coin, json.dumps(deltablocks))
            r.set('deltaflow_' + coin, json.dumps(deltaflow))

        else: # add the unit to the delta flow

            # print('ADD DELTA UNIT') # len(deltablocks), len(deltaflow)

            # update current candle with new unit data
            currentCandle = addDeltaBlock(deltaflow, deltablocks, deltaCount, coin)
            deltablocks[-1] = currentCandle
            r.set('deltablocks_' + coin, json.dumps(deltablocks))
            r.set('deltaflow_' + coin, json.dumps(deltaflow))


def logVolumeUnit(buyUnit, sellUnit, coin, size):    ## load vol flow

    vFlow = 'volumeflow_' + coin + str(size)
    vBlocks = 'volumeblocks_' + coin + str(size)

    if not r.get(vFlow):
        r.set(vFlow, json.dumps([]))
        r.set(vBlocks, json.dumps([]))

    # if LOCAL:
    #     print('LOG VOLUME UNIT SKIP')
    #     return False

    if LOCAL:
        block = size * 100_000
    else:
        block = size * 1_000_000


    volumeflow = json.loads(r.get(vFlow))

    totalMsgSize = buyUnit['size'] + sellUnit['size']

    # if LOCAL:
    #     print(coin + ' LOG VOLUME UNIT ' + str(totalMsgSize))

    ## calculate current candle size
    volumeflowTotal = 0
    for t in volumeflow:
        volumeflowTotal += t['size']

    if volumeflowTotal > block:
        ### Deal with the uncomman event where the last function left an excess on volume flow
        print('VOL FLOW EXCESS ' + str(volumeflowTotal))
        volumeblocks = json.loads(r.get(vBlocks))
        currentCandle = addBlock(volumeflow, volumeblocks, 'volblock_' + str(size), coin)

        volumeblocks[-1] = currentCandle

        volumeflow = []

        if buyUnit['size'] > 1:
            volumeflow.append(buyUnit)
        if sellUnit['size'] > 1:
            volumeflow.append(sellUnit)

        currentCandle = addBlock(volumeflow, volumeblocks, 'vol_' + str(size), coin)

        volumeblocks.append(currentCandle)

        r.set(vBlocks, json.dumps(volumeblocks))
        r.set(vFlow, json.dumps(volumeflow))


    elif volumeflowTotal + totalMsgSize <= block:  # Normal addition of trade to volume flow
        # print(volumeflowTotal, '< Block')

        if buyUnit['size'] > 1:
            volumeflow.append(buyUnit)
        if sellUnit['size'] > 1:
            volumeflow.append(sellUnit)


        volumeblocks = json.loads(r.get(vBlocks))
        currentCandle = addBlock(volumeflow, volumeblocks, 'vol_' + str(size), coin)

        LastIndex = len(volumeblocks) - 1
        if LastIndex < 0:
            volumeblocks.append(currentCandle)
        else:
            volumeblocks[LastIndex] = currentCandle

        r.set(vBlocks, json.dumps(volumeblocks))
        r.set(vFlow, json.dumps(volumeflow))

    else: # Need to add a new block

        # print('carryOver')
        # print('new blockkkkk --  Total msg size: ' + str(totalMsgSize) + ' Vol flow total: ' + str(volumeflowTotal))
        lefttoFill = block - volumeflowTotal

        ## split buys and sells evenly
        if totalMsgSize == 0:
            totalMsgSize = 1

        proportion = lefttoFill/totalMsgSize

        ## left to fill 100_000  totalmsg size 1_300_000  (1_000_000 buys   300_000 sells)
        ## proportion = 0.076

        buyFill = buyUnit['size'] * proportion
        sellFill = sellUnit['size'] * proportion

        buyCopy = buyUnit.copy()
        sellCopy = sellUnit.copy()

        buyCopy['size'] = int(buyFill)
        sellCopy['size'] = int(sellFill)

        if buyCopy['size'] > 0:
            volumeflow.append(buyCopy)
            buyUnit['size'] -= int(buyFill)
        if sellCopy['size'] > 0:
            volumeflow.append(sellCopy)
            sellUnit['size'] -= int(sellFill)

        volumeblocks = json.loads(r.get(vBlocks))
        LastIndex = len(volumeblocks) - 1
        # print('VOL BLOCK BREAK')
        newCandle = addBlock(volumeflow, volumeblocks, 'volblock_' + str(size), coin)
        volumeblocks[LastIndex] = newCandle  # replace last candle (current) with completed

        r.set(vBlocks, json.dumps(volumeblocks))

        ## volume flow has been added as full candle and should be reset
        volumeflow = []

        while buyUnit['size'] > block:
            ## keep appending large blocks
            # r.set('discord_' + coin, 'Carry Over: ' + str(buyUnit['size']) + ' -- ' + str(sellUnit['size']))
            volumeblocks = json.loads(r.get(vBlocks))
            newUnit = buyUnit.copy()
            newUnit['size'] = block
            buyUnit['size'] = buyUnit['size'] - block
            newCandle = addBlock([newUnit], volumeblocks, 'carry_' + str(size), coin)
            volumeblocks.append(newCandle)
            r.set(vBlocks, json.dumps(volumeblocks))

        while sellUnit['size'] > block:
            ## keep appending large blocks
            # r.set('discord_' + coin, 'Carry Over: ' + str(buyUnit['size']) + ' -- ' + str(sellUnit['size']))
            volumeblocks = json.loads(r.get(vBlocks))
            newUnit = sellUnit.copy()
            newUnit['size'] = block
            sellUnit['size'] = sellUnit['size'] - block
            newCandle = addBlock([newUnit], volumeblocks, 'carry_' + str(size), coin)
            volumeblocks.append(newCandle)
            r.set(vBlocks, json.dumps(volumeblocks))

        if buyUnit['size'] + sellUnit['size']  >  block:
            ## This is very unlikley so just set an alert
            r.set('discord_' + coin, 'Unlikley Carry: ' + str(buyUnit['size']) + ' -- ' + str(sellUnit['size']))


        # Create new flow block with left over contracts
        if buyUnit['size'] > 1:
            volumeflow.append(buyUnit)
        if sellUnit['size'] > 1:
            volumeflow.append(sellUnit)

        volumeblocks = json.loads(r.get(vBlocks))
        currentCandle = addBlock(volumeflow, volumeblocks, 'vol_' + str(size), coin)
        volumeblocks.append(currentCandle)
        r.set(vBlocks, json.dumps(volumeblocks))
        r.set(vFlow, json.dumps(volumeflow))


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

        return json.dumps({
            'VOL: ' : round(dailyVolume/100_000)/10,
            'CVD:' : round(dailyCVD/100_000)/10,
            'Price:' : dailyPriceDelta,
            'DIV' : dailyDIV
            })

    except:
        return 'getPreviousDay() fail'


def historyReset(coin):
    # print('HISTORY RESET ' + coin)

    if r.get('history_' + coin) == None:
        r.set('history_' + coin, json.dumps([]))

    current_time = dt.datetime.utcnow()

    dt_string = current_time.strftime("%d/%m/%Y")

    if current_time.hour == 23 and current_time.minute == 59:
        print('History Reset Current Time UTC : ' + str(current_time))
        history = json.loads(r.get('history_' + coin))

        pdDict = {
                    'date' : dt_string,
                }

        for k in r.keys():
            if 'blocks' in k and coin in k:
                pdDict[k] = json.loads(r.get(k))
                print('History Loads ' + k)

        if len(history) > 0:
            lastHistory = json.loads(r.get('history_' + coin))[len(history)-1]

            if lastHistory['date'] != dt_string:
                print('REDIS STORE', dt_string)

                history.append(pdDict)

                r.set('history_' + coin, json.dumps(history))

                if coin == 'BTC':
                    tb = json.loads(r.get('timeblocks_BTC'))
                    pd = getPreviousDay(tb)
                    r.set('discord_' + coin, 'history log: ' + pd)
        else:
            print('REDIS STORE INITIAL')

            history.append(pdDict)
            r.set('history_' + coin, json.dumps(history))

            if coin == 'BTC':
                    tb = json.loads(r.get('timeblocks_BTC'))
                    pd = getPreviousDay(tb)
                    r.set('discord_' + coin, 'history log: ' + pd)

    if current_time.hour == 0 and current_time.minute == 0:
        print('REDIS RESET', current_time)
        if r.get('newDay_' + coin) != dt_string:
            print('REDIS RESET')

            for k in r.keys():
                if k[0] == 'v':
                    r.delete(k)
                print(k)


            r.set('timeflow_' + coin, json.dumps([]) )  # this is the flow of message data to create next candle
            r.set('timeblocks_' + coin, json.dumps([]) ) # this is the store of new time based candles
            r.set('newDay_' + coin, dt_string)

            r.set('discord_' + coin, coin + ' new day')

    return True


def compiler(message, pair, coin):

    timestamp = message[0]['timestamp']
    ts = str(datetime.strptime(timestamp.split('.')[0], "%Y-%m-%dT%H:%M:%S"))
    trade_time_ms = int(message[0]['trade_time_ms'])


    sess = session.latest_information_for_symbol(symbol=pair)
    streamOI = sess['result'][0]['open_interest']
    streamTime = round(float(sess['time_now']), 1)
    # print(streamTime)
    streamPrice = float(sess['result'][0]['last_price'])


    manageStream(streamTime, streamPrice, streamOI, coin)

    buyUnit = {
                    'side' : 'Buy',
                    'size' : 0,
                    'trade_time_ms' : trade_time_ms,
                    'timestamp' : ts,
                    'streamTime' : streamTime,
                    'streamPrice' : streamPrice,
                    'streamOI' : streamOI,
                    'tradecount' : 0,
                    'spread' : {}
                }

    sellUnit = {
                    'side' : 'Sell',
                    'size' : 0,
                    'trade_time_ms' : trade_time_ms,
                    'timestamp' : ts,
                    'streamTime' : streamTime,
                    'streamPrice' : streamPrice,
                    'streamOI' : streamOI,
                    'tradecount' : 0,
                    'spread' : {}
    }

    for x in message:

        size = x['size']

        if coin == 'BTC':
            #  21010.51 -->  42020.2 --> 42020 --> 21010.5
            priceString = str(  round  ( float(x['price'])  *2 )/2)
        elif coin == 'ETH':
            # 1510.21 -->  30204.2 --> 30204 --> 15102 --> 1510.2
            priceString = str(  round  ( float(x['price'])  *100 )/100)
        elif coin == 'SOL':
            # 23.645  -->  23.645 --> 30204 --> 15102 --> 1510.2
            priceString = str(  round  ( float(x['price'])  *100 )/100)
            size = round ( x['size']*10  )  / 10
        elif coin == 'GALA':
            # 0.04848 -- >
            priceString = str(  round  ( float(x['price'])  *100000 )/100000)
            size = round ( x['size']*10  )  / 10
        elif coin == 'BIT':
            # 0.5774
            priceString = str(  round  ( float(x['price'])  *10000 )/10000)
            size = round ( x['size']*10  )  / 10

        if x['side'] == 'Buy':
            spread = buyUnit['spread']
            if priceString not in spread:
                spread[priceString] = size
            else:
                spread[priceString] += size

            buyUnit['size'] += size
            buyUnit['tradecount'] += 1

        if x['side'] == 'Sell':
            spread = sellUnit['spread']
            if priceString not in spread:
                spread[priceString] = size
            else:
                spread[priceString] += size

            sellUnit['size'] += x['size']
            sellUnit['tradecount'] += 1

    # print(coin + ' COMPILER RECORD:  Buys - ' + str(buyUnit['size']) + ' Sells - ' + str(sellUnit['size']) )

    return [buyUnit, sellUnit]


def handle_trade_message(msg):

    pair = msg['topic'].split('.')[1]
    coin = pair.split('USD')[0]

    coinDict = json.loads(r.get('coinDict'))

    # print(coinDict)

    if not coinDict[coin] or not coinDict[coin]['active']:
        ## print('not active ' + coin)
        return False

    if coinDict[coin] and coinDict[coin]['purge']:
        print('purge ' + coin)
        for k in r.keys():
            if coin in k:
                r.delete(k)

    ### check time and reset
    historyReset(coin)

    # print(coin + ' handle_trade_message: ' + str(len(msg['data'])))
    # print(msg['data'])

    # print( 'Start: ' + str(len(msg['data'])))
    compiledMessage = compiler(msg['data'], pair, coin)


    # print('COMPILER DONE')

    buyUnit = compiledMessage[0]
    sellUnit = compiledMessage[1]
    if LOCAL:
        print('Compiled B:' + str(buyUnit['size']) + ' S:' + str(sellUnit['size']))

    if buyUnit['size'] + sellUnit['size'] <= 2:
        return False

    volControl = coinDict[coin]['volume']
    deltaControl = coinDict[coin]['deltaswitch']
    pause = coinDict[coin]['pause']

    logTimeUnit(buyUnit, sellUnit, coin)

    # print('LOG TIME')


    if volControl[0]: # ignore small size trades
        logVolumeUnit(buyUnit, sellUnit, coin, int(volControl[1]))

    # print('LOG VOLUME')


    if deltaControl['fcCheck'] > 0:
        deltaCount = deltaControl['block']
        if LOCAL:
            deltaCount = 10000
        logDeltaUnit(buyUnit, sellUnit, coin, deltaCount)

    # print('LOG DELTA')




@app.task(bind=True, base=AbortableTask)
def runStream(self):

    print('RUN_STREAM')    # rK = json.loads(r.keys())


    for k in r.keys():
        if k[0] != '_' and k != 'coinDict' and k != 'channelDict' and k != 'task_id':
            r.delete(k)

    if not r.get('coinDict'):
        setCoinDict()

    coinDict = json.loads(r.get('coinDict'))


    for c in coinDict:
        rDict = {
            'lastPrice' : 0,
            'lastTime' : 0,
            'lastOI' : 0,
            '1mOI' : [],
            'oiMarkers' : coinDict[c]['oicheck'],
            'Divs' : {},
            'alerts' : []
        }
        r.set('stream_' + c, json.dumps(rDict) )
        r.set('timeflow_' + c, json.dumps([]) )  # this the flow of message data to create next candle
        r.set('timeblocks_' + c, json.dumps([]) ) # this is the store of new time based candles


    print('WEB_SOCKETS')

    ws_inverseP = inverse_perpetual.WebSocket(
        test=False,
        ping_interval=30,  # the default is 30
        ping_timeout=None,  # the default is 10 # set to None and it will never timeout?
        domain="bybit"  # the default is "bybit"
    )

    coins = ["BTCUSD"] #"BITUSD"
    if LOCAL:
        coins = ["BTCUSD"]

    ws_inverseP.trade_stream(
        handle_trade_message, coins
    )

    # ws_usdtP = usdt_perpetual.WebSocket(
    #     test=False,
    #     ping_interval=30,  # the default is 30
    #     ping_timeout=None,  # the default is 10 # set to None and it will never timeout?
    #     domain="bybit"  # the default is "bybit"
    # )

    # ws_usdtP.trade_stream(
    #     handle_trade_message, ["GALAUSDT"]
    # )


    # ws_inverseP.instrument_info_stream(
    #     handle_info_message, "BTCUSD"
    # )


    startDiscord()

    while not self.is_aborted():
        sleep(0.1)

    return print('Task Closed')


if LOCAL:
    runStream()





