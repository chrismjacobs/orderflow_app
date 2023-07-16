import os
import redis
import json
import math
from math import trunc


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
    }

    return newCandle


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
        if coin == 'BTC':
            t['vwapTick'] = str(trunc(vwapPrice/10)*10)
        elif coin == 'ETH':
            t['vwapTick']  = math.floor(vwapPrice)

    return timeblocks

def getPVAStatus(newBlocks):

    last10vols = []

    for b in newBlocks:

        returnPVA = {
            'pva150' : False,
            'pva200' : False,
            'vol': 0,
            'percentage' : 0,
            'deltapercentage' : 0,
            'PVAbearDIV' : None,
            'PVAbullDIV' : None,
            'flatOI' : None
        }

        total = 0

        for v in last10vols:
            total += v

        average = total/10

        if len(last10vols) == 10:
            returnPVA['percentage'] = round((b['total']/average)*100)

            if b['total'] > average * 2:
                returnPVA['pva200'] = True

            elif b['total'] > average * 1.5:
                returnPVA['pva150'] = True

        if len(last10vols) == 10:
            last10vols.pop(0)

        last10vols.append(b['total'])

        b['pva_status'] = returnPVA

    return newBlocks



def getImbalances(tickList):

    for i in range(len(tickList)):  # 0 1 2

        if i + 1 < len(tickList):
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

    return tickList


def getTicks(newCandle, unit):
    ticks = []

    for t in unit['tickList']:

        for n in newCandle['tickList']:
            tp = n['tickPrice']
            if int(tp) not in ticks:
                ticks.append(int(tp))

            if t['tickPrice'] == n['tickPrice']:
                n['Sell'] += t['Sell']
                n['Buy'] += t['Buy']
                break
        else:
            newTick =  t['tickPrice']
            if int(newTick) < ticks[-1]:
                newCandle['tickList'].append(t)
            else:
                position = 0
                for price in ticks:
                    if int(newTick) > price:
                        ticks.insert(position,int(newTick))
                        newCandle['tickList'].insert(position,t)
                        break

    return newCandle

def getBlocks(size, blocksString):

    ## get all the timeblocks (5Min)
    blocks = json.loads(blocksString)

    #print(len(blocks))

    # new list of candle blocks
    newList = []

    ## new candle of size
    newCandle = {}

    firstUnit = {}

    count = 1

    for unit in blocks:
        # print('count', count, unit)

        ## ADD fist unit
        if count == 1:
            newCandle = {}
            for y in unit:
                # create new candle key value pairs
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

            newCandle = getTicks(newCandle, unit)

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

            newCandle['close'] = unit['close']
            newCandle['price_delta'] = newCandle['close'] - newCandle['open']
            newCandle['oi_cumulative'] = unit['oi_cumulative']
            newCandle['oi_delta'] = newCandle['oi_cumulative'] - newCandle['oi_open']
            newCandle['time_delta'] = newCandle['trade_time_ms'] - unit['trade_time_ms']

            newCandle = getTicks(newCandle, unit)


            newList.append(newCandle)

            count = 1

    newBlocks = getPVAStatus(newList)


    #print(len(newBlocks))
    return json.dumps(newBlocks)



