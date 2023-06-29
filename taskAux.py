import os
import redis
import json
import discord
from discord.ext import tasks, commands
from discord import SyncWebhook
from datetime import datetime
from pybit import inverse_perpetual, usdt_perpetual

LOCAL = False

try:
    import config
    LOCAL = True
    if LOCAL:
        REDIS_URL = config.REDIS_URL_TEST
    else:
        REDIS_URL = config.REDIS_URL
    DISCORD_CHANNEL = config.DISCORD_CHANNEL
    DISCORD_TOKEN = config.DISCORD_TOKEN
    DISCORD_USER = config.DISCORD_USER
    API_KEY = config.API_KEY
    API_SECRET = config.API_SECRET
    DISCORD_WEBHOOK = config.DISCORD_WEBHOOK
    r = redis.from_url(REDIS_URL, ssl_cert_reqs=None, decode_responses=True)
except:
    REDIS_URL = os.getenv('CELERY_BROKER_URL')
    DISCORD_CHANNEL = os.getenv('DISCORD_CHANNEL')
    DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
    DISCORD_USER = os.getenv('DISCORD_USER')
    API_KEY = os.getenv('API_KEY')
    API_SECRET = os.getenv('API_SECRET')
    DISCORD_WEBHOOK = os.getenv('DISCORD_WEBHOOK')
    r = redis.from_url(REDIS_URL, decode_responses=True)


session = inverse_perpetual.HTTP(
    endpoint='https://api.bybit.com',
    api_key=API_KEY,
    api_secret=API_SECRET
)


def monitorLimits():
    pair = "BTCUSD"

    position = session.my_position(symbol=pair)['result']

    positionSize = int(position['size'])

    if positionSize == 0:
        ### Trade exited so delete left limit order
        recentOrders = session.get_active_order(symbol=pair)['result']['data']
        orderID = 0
        for ro in recentOrders:
            if ro['order_status'] == 'New':
                orderID = ro['order_id']
                result = session.cancel_all_active_orders(symbol="BTCUSD")['ret_msg']
                print('CANCEL', result)
                break


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
        setCoinDict()
        await user.send('Running')
        checkRedis.start(user)

    @tasks.loop(seconds=3)
    async def checkRedis(user):
        #print('DISCORD REDIS CHECK')

        if not r.get('channelDict'):
            r.set('channelDict', DISCORD_CHANNEL)

        channelDict = json.loads(r.get('channelDict'))

        if not r.get('monitor'):
            r.set('monitor', 'on')

        if r.get('monitor') == 'on':
            try:
                monitorLimits()
            except Exception as e:
                print('MONITOR ERROR', e)
                channel = bot.get_channel(int(channelDict['BTC']))
                await channel.send('MONITOR ERROR: ' + e)

        for coin in channelDict:
            ## need incase redis gets wiped
            if not r.get('discord_' + coin):
                r.set('discord_' + coin, 'discord set')
            if not r.get('discord_' + coin + '_holder'):
                r.set('discord_' + coin + '_holder', 'discord set')

            channel = bot.get_channel(int(channelDict[coin]))

            # print(channel, int(channelDict[coin]))
            msg = r.get('discord_' + coin)
            msg_h = r.get('discord_' + coin + '_holder')

            if msg != 'blank':
                await channel.send(msg)
                r.set('discord_' + coin, 'blank')
            elif msg_h != 'blank':
                await channel.send(msg_h)
                r.set('discord_' + coin + '_holder', 'blank')


    @bot.event
    async def on_message(msg):
        user = bot.get_user(int(DISCORD_USER))
        # print('MESSAGE DDDDDDDDD', msg.content)
        replyText = 'ho'

        deltaSet = {
            'db' : ['deltaswitch', 'Buy'],
            'ds' : ['deltaswitch', 'Sell'],
            'vb' : ['volswitch', 'Buy'],
            'vs' : ['volswitch', 'Sell']
        }

        if len(msg.content) > 20:
            ## ignore long messages
            return False
        if msg.content == 'B':
            lastCandle = json.loads(r.get('timeblocks_BTC'))[-2]
            print(lastCandle)
            oi = round(lastCandle['oi_delta']/1000)
            b = round(lastCandle['buys']/1000)
            s = round(lastCandle['sells']/1000)
            replyText = str(lastCandle['total']) + ' OI: ' + str(oi) + 'k Buys: ' + str(b) + 'k Sells: ' + str(s) + 'k'
        elif 'check' in msg.content:
            checkRedis.start(user)
            replyText = 'check'
        elif 'try' in msg.content:
            try:
                webhook = SyncWebhook.from_url(DISCORD_WEBHOOK)
                webhook.send("check")
            except Exception as e:
                print('DISCORD WEBHOOK EXCEPION ' + e)
        elif 'elta purge' in msg.content:
            coin = 'BTC'
            dFlow = 'deltaflow_' + coin
            dBlocks = 'deltablocks_' + coin
            replyText = 'purge action'
            r.set(dFlow, json.dumps([]))
            r.set(dBlocks, json.dumps([]))


        elif 'nsi' in msg.content and r.get('ansi') == 'on':
            r.set('ansi', 'off')
            replyText = 'Ansi ' + r.get('ansi')
        elif 'nsi' in msg.content and r.get('ansi') == 'off':
            r.set('ansi', 'on')
            replyText = 'Ansi ' + r.get('ansi')


        elif 'tack' in msg.content and r.get('stack') == 'on':
            r.set('stack', 'off')
            replyText = 'Stacks ' + r.get('stack')
        elif 'tack' in msg.content and r.get('stack') == 'off':
            r.set('stack', 'on')
            replyText = 'Stacks ' + r.get('stack')

        elif 'onitor off' in msg.content:
            r.set('monitor', 'off')
            replyText = 'Set Monitor ' + r.get('monitor')
        elif 'onitor on' in msg.content:
            r.set('monitor', 'on')
            replyText = 'Set Monitor ' + r.get('monitor')
        elif 'onitor check' in msg.content:
            replyText = 'Monitor is set to' + r.get('monitor')


        elif msg.content == 'Dict' or msg.content == 'dict':
            setCoinDict()
            replyText = 'Coin Dict Set'
        elif msg.content.split(' ')[0] in deltaSet:
            latestprice = float(session.latest_information_for_symbol(symbol='BTCUSD')['result'][0]['last_price'])
            coinDict = json.loads(r.get('coinDict'))
            code = msg.content.split(' ')[0]
            price = int(msg.content.split(' ')[1])

            if price > 100_000:
                replyText = 'Price out of range'
            elif price < 10_000 and price is not 0:
                replyText = 'Price out of range'
            elif 's' in code and price < latestprice:
                replyText = 'Price too low'
            elif 'b' in code and price > latestprice:
                replyText = 'Price too high'
            else:
                try:
                    switch = deltaSet[code][0]
                    side = deltaSet[code][1]
                    coinDict['BTC'][switch][side]['price'] = price
                    if len(msg.content.split(' ')) > 2:
                        add = msg.content.split(' ')[2]
                        if '.' in add:
                            coinDict['BTC'][switch][side]['fraction'] = float(msg.content.split(' ')[2])
                        else:
                            coinDict['BTC'][switch][side]['stop'] = int(msg.content.split(' ')[2])

                    r.set('coinDict', json.dumps(coinDict))
                    replyText = 'Set: ' + side + ' ' + str(price) + ' ' + str(coinDict['BTC'][switch][side]['fraction']) + ' ' + str(coinDict['BTC'][switch][side]['stop'])
                except Exception as e:
                    print('DELTA SET ERROR', e)
                    replyText = 'DELTA SET ERROR'
        else:
            return False

        if msg.author == user:
            await user.send(replyText)
            # ping('rekt-app.onrender.com', verbose=True)


    bot.run(DISCORD_TOKEN)


def sendMessage(coin, string, bg, text):
    str1 = "```ansi\n"

    escape =  "\u001b[0;"   ## 0 == normal text  1 bold

    colors = {  ### bg / text
        '': [''],
        'grey': ['44;'],
        'red' : ['45;', '31m'],
        'green' : ['43;', '32m'],
        'yellow' : ['41;', '33m'],
        'blue' : ['40;', '34m'],
        'pink' : ['45;', '35m'],
        'cyan' : ['42;', '36m'],
        'white' : ['47;', '37m']
    }
    ## bground first then color

    str2 = "\n```"

    msg = str1 + escape +  colors[bg][0] + colors[text][1] + string + str2

    ansi = r.get('ansi')
    if not ansi:
        ansi = 'on'
        r.set('ansi', ansi)

    if ansi == 'off':
        msg = string

    if not coin:
        return msg
    elif r.get('discord_' + coin) != 'blank':
        r.set('discord_' + coin, msg)
    else:
        r.set('discord_' + coin + '_holder', msg)

def actionBIT(side):
    r.set('discord_BIT', side)
    print('ACTION BIT')


def getHL(side, current, stop, mode):

    now = datetime.now()
    minutes = 5
    timestamp = int(datetime.timestamp(now)) - int(minutes)*60
    data = session.query_kline(symbol="BTCUSD", interval="1", from_time=str(timestamp))['result']

    hAry = []
    lAry = []

    for i in range(0, len(data)):
        hAry.append(int(data[i]['high'].split('.')[0]))
        lAry.append(int(data[i]['low'].split('.')[0]))

    mHi = max(hAry)
    mLow = min(lAry)

    if side == 'Buy':
        distance = abs(current - mLow)
        if distance > stop:
            stop_loss = current - stop
        else:
            stop_loss = mLow - 45

    if side == 'Sell':
        distance = abs(current - mHi)
        if distance > stop:
            stop_loss = current + stop
        else:
            stop_loss = mHi + 45


    return stop_loss

def marketOrder(side, fraction, stop, profit, mode):

    pair = 'BTCUSD'

    position = session.my_position(symbol=pair)['result']

    positionSide = position['side']
    positionSize = int(position['size'])
    positionLev = float(position['leverage'])

    if positionSize > 0 or positionLev > 2:
        message = 'Position already open: ' + positionSide + ' ' + str(positionLev)
        print(message)
        sendMessage('BTC', message, '', 'red')
        return False
    else:
        print('Order continue')

    price = float(session.latest_information_for_symbol(symbol=pair)['result'][0]['last_price'])
    funds = session.get_wallet_balance()['result']['BTC']['equity']
    # leverage = 2
    # session.set_leverage(symbol=pair, leverage=leverage)
    qty = (price * funds * 2) * fraction

    #stop_loss = getHL(side, price, stop, mode)

    stop_adjust = stop

    try:
        if stop_adjust < 1:
            stop_adjust = price*(stop/100)/positionLev
        if stop_adjust > 100 or stop_adjust < 30:
            stop_adjust = 70
    except:
        print('STOP LOSS Adjust failed')


    print('MARKET ORDER ' + str(price) + ' sl:' + str(stop_adjust))

    limits = {
        'Buy' : -1,
        'Sell' : 1
    }

    sideRev = None

    if side == 'Buy':
        take_profit = price + profit
        stop_loss = price - stop_adjust
        sideRev = 'Sell'
    if side == 'Sell':
        take_profit = price - profit
        stop_loss = price + stop_adjust
        sideRev = 'Buy'

    oType = 'Market'
    oPrice = None

    if mode == 'volswitch':
        oType == 'Limit'
        oPrice = price + limits[side]
        r.set('monitor', 'off')


    order = session.place_active_order(
    symbol = pair,
    side = side,
    order_type = oType,
    price = oPrice,
    stop_loss = stop_loss,
    take_profit = take_profit,
    qty = qty,
    time_in_force = "GoodTillCancel"
    )

    message = order['ret_msg']
    return_code = order['ret_code']  # 0  = 'good'
    # data = json.dumps(order['result'])


    return_price = order["result"]["price"]  # float

    print('ORDER MESSAGE ' + message)

    if message == 'OK' and mode == 'deltaswitch':
        try:
            webhook = SyncWebhook.from_url(DISCORD_WEBHOOK)
            webhook.send("check")
        except Exception as e:
            print('DISCORD WEBHOOK EXCEPION ' + e)
            return True

        r.set('monitor', 'on')
        position = session.my_position(symbol="BTCUSD")['result']
        positionPrice = float(position['entry_price'])

        ## Get VWAP
        timeblocks = json.loads(r.get('timeblocks_BTC'))
        vwap = round(positionPrice + (limitPrice*limits[sideRev]))

        try:
            if timeblocks[-2]['vwapTick']:
                vwap = round(timeblocks[-2]['vwapTick'] + (15*limits[sideRev]))
            elif timeblocks[-2]['vwap_task']:
                vwap = round(timeblocks[-2]['vwap_task'] + (15*limits[sideRev]))

            if abs(positionPrice - vwap) > 100:
                vwap = round(positionPrice + (100*limits[sideRev]))
            if abs(positionPrice - vwap) < 60:
                vwap = round(positionPrice + (60*limits[sideRev]))

        except:
            print('VWAP TP EXCEPTION')

        try:
        ### place limit TP
            limitPrice = profit / 6
            newLimit = session.place_active_order(
            symbol = pair,
            side = sideRev,
            order_type = 'Limit',
            price =  vwap,
            qty = round(qty*0.5),
            time_in_force = "GoodTillCancel"
            )
        except Exception as e:
            print('LIMIT ERROR', e)
        else:
            print('LIMIT SUCCESS')


    return True


def getSwitchMessage(SIDE, ACTIVE, THD, PD, BT, CTD, FC):

    switchMessage = 'nothing'

    try:
        switchMessage = SIDE + ' Active: ' + str(ACTIVE) + ' Threshold: ' + str(THD) + ' PDs: ' + json.dumps(PD) + ' totals: ' +  json.dumps(BT) + ' time: ' + str(CTD) + ' fc: ' + str(FC)
    except Exception as e:
        print('SWITCH MESSAGE ' + e)

    return switchMessage


def actionDELTA(blocks, newCandle, coin, coinDict, lastCandleisBlock):

    deltaControl = coinDict[coin]['deltaswitch']

    if deltaControl['Buy']['price'] == 0 and deltaControl['Sell']['price'] == 0:
        # print('delta zero')
        return 'ZO'

    if deltaControl['Sell']['price'] > 0 and blocks[-1]['high'] > deltaControl['Sell']['price'] and deltaControl['Sell']['swing'] == False:
        deltaControl['Sell']['swing'] = True
        deltaControl['Buy']['swing'] = False
        print('DELTA SELL SWING TRUE')
        r.set('coinDict', json.dumps(coinDict))
        return 'SW'

    if deltaControl['Buy']['price'] > 0 and blocks[-1]['low'] < deltaControl['Buy']['price'] and deltaControl['Buy']['swing'] == False:
        deltaControl['Buy']['swing'] = True
        deltaControl['Sell']['swing'] = False
        print('DELTA BUY SWING TRUE')
        r.set('coinDict', json.dumps(coinDict))
        return 'SW'

    side = None
    if deltaControl['Sell']['swing'] == True:
        side = 'Sell'
    elif deltaControl['Buy']['swing'] == True:
        side = 'Buy'
    else:
        return 'NO SIDE'



    fcCheck = deltaControl['fcCheck']

    currentTimeDelta = newCandle['time_delta']/1000

    count = 1



    activeRecent = False

    fastCandles = 0
    posDelta = 0
    negDelta = 0

    tds = []

    blockList = blocks[-(fcCheck):]
    ### if last candle is not block then don't count it
    if not lastCandleisBlock:
        blockList = blocks[-(fcCheck +1) : -1]


    for b in blockList:  ## examine candles leading up to current
        t = b['time_delta']/1000
        tds.append(t)
        if t < 5:
            fastCandles += 1
        if b['switch'] == 'ATC' or b['switch'] == 'ATT':
            activeRecent = True
        if b['delta']/b['total'] < -0.5:
            negDelta += 1
        if b['delta']/b['total'] > 0.5:
            posDelta += 1
        count += 1

    try:
        print('ACTION DELTA CHECK: ' + side + ' SWING:' + str(deltaControl[side]['swing']) + ' ACTIVE:' + str(deltaControl[side]['active']) + ' TD:' + str(currentTimeDelta) + ' FC:' + str(fastCandles) + ' LC:' + str(lastCandleisBlock) )
    except:
        print('ACTION MESSAGE FAIL')

    percentDelta0 = newCandle['delta']/newCandle['total']  #current block

    lc1 = -1
    lc2 = -2
    lc3 = -3

    ### if last candle is not block then don't count it
    if not lastCandleisBlock:
        lc1 = -2
        lc2 = -3
        lc3 = -4

    percentDelta1 = blocks[lc1]['delta']/blocks[lc1]['total']  #last block
    percentDelta2 = blocks[lc2]['delta']/blocks[lc2]['total']  #last blocks
    percentDelta3 = blocks[lc3]['delta']/blocks[lc3]['total']  #last blocks


    pds = [round(percentDelta0, 3), round(percentDelta1, 3), round(percentDelta2, 3), round(percentDelta3, 3) ]


    thresholdMarket = percentDelta0 >= 0.99 and percentDelta1 >= 0.99
    thresholdActivate = negDelta  >= 2


    if side == 'Sell':
        thresholdMarket = percentDelta0 <= -0.99 and percentDelta1 <= -0.99
        thresholdActivate = posDelta >= 2



    stallCondition_1candle =  newCandle['total'] > 500_000 and thresholdActivate
    stallCondition_2candle =  False #blocks[-1]['total'] + newCandle['total'] > 500_000 and thresholdActivate
    stallCondition = stallCondition_1candle or stallCondition_2candle
    blockTotals = [newCandle['total'], blocks[lc1]['total'], blocks[lc2]['total']]



    if currentTimeDelta > 5 and thresholdActivate and fastCandles == fcCheck and deltaControl[side]['active'] == False:
        ## delta action has stalled: lookout is active
        deltaControl[side]['active'] = True
        r.set('coinDict', json.dumps(coinDict))
        msg = getSwitchMessage(side, deltaControl[side]['active'], thresholdMarket, pds, blockTotals, currentTimeDelta, fastCandles)
        print('DELTA STALL CONDITION ATT: ' + msg)
        return 'ATT'

    elif stallCondition and fastCandles == fcCheck and deltaControl[side]['active'] == False:
        ## delta action has stalled: lookout is active
        deltaControl[side]['active'] = True
        r.set('coinDict', json.dumps(coinDict))
        msg = getSwitchMessage(side, deltaControl[side]['active'], thresholdMarket, pds, blockTotals, currentTimeDelta, fastCandles)
        print('DELTA STALL CONDITION ATC: ' + msg)
        return 'ATC'

    elif deltaControl[side]['active'] and thresholdMarket:
        print('PLACE DELTA')
        if LOCAL:
            return 'MO'

        MO = marketOrder(side, deltaControl[side]['fraction'], deltaControl[side]['stop'], deltaControl[side]['profit'], 'deltaswitch')

        if MO:
            resetCoinDict(coinDict, side, 'deltaswitch')
            msg = getSwitchMessage(side, deltaControl[side]['active'], thresholdMarket, pds, blockTotals, currentTimeDelta, fastCandles)
            print('DELTA ORDER MESSAGE ' + msg)
            return 'MO'
        else:
            return 'MF'

    elif fastCandles == fcCheck and not activeRecent:

        deltaControl[side]['active'] = False
        msg = getSwitchMessage(side, deltaControl[side]['active'], thresholdMarket, pds, blockTotals, currentTimeDelta, fastCandles)

        print('DELTA FAST RESET: ' + msg)
        r.set('coinDict', json.dumps(coinDict))
        return 'AF'


    msg = getSwitchMessage(side, deltaControl[side]['active'], thresholdMarket, pds, blockTotals, currentTimeDelta, fastCandles)

    return msg


def actionVOLUME(blocks, coin, coinDict, bullDiv, bearDiv):

    volumeControl = coinDict[coin]['volswitch']
    print('volume control', volumeControl)

    if volumeControl['Buy']['price'] == 0 and volumeControl['Sell']['price'] == 0:
        print('volume zero')
        return False

    if volumeControl['Sell']['price'] > 0 and blocks[-1]['high'] > volumeControl['Sell']['price'] and volumeControl['Sell']['swing'] == False:
        volumeControl['Sell']['swing'] = True
        volumeControl['Buy']['swing'] = False
        print('VOL SELL SWING TRUE')
        r.set('coinDict', json.dumps(coinDict))
        return True

    if volumeControl['Buy']['price'] > 0 and blocks[-1]['low'] < volumeControl['Buy']['price'] and volumeControl['Buy']['swing'] == False:
        volumeControl['Buy']['swing'] = True
        volumeControl['Sell']['swing'] = False
        print('BUY SWING TRUE')
        r.set('coinDict', json.dumps(coinDict))
        return True


    side = None
    if volumeControl['Sell']['swing'] == True:
        side = 'Sell'
    elif volumeControl['Buy']['swing'] == True:
        side = 'Buy'
    else:
        return False

    fastCandles = []
    for b in blocks[-4:-2]:
        if b['time_delta']/1000 < 60:

            bUnit = [  b['time_delta']/1000,
                       round( (b['delta']/b['total'])*100   ),
                       b['oi_delta']
                    ]
            fastCandles.append(bUnit)
            break
    else:
        print('NO FAST CANDLE ACTIVATION')
        return False

    print('VOLUME SWING ACTIVE')

    percentDelta = blocks[-1]['delta']/blocks[-1]['total']
    oiDelta = blocks[-1]['oi_delta']

    lastUnit = [  blocks[-1]['time_delta']/1000,   round( percentDelta*100 ),  oiDelta  ]

    fastCandles.append(lastUnit)

    fcString = json.dumps(fastCandles)
    print('volswitch pass:  FC= ' + fcString)

    cond1 = side == 'Buy' and percentDelta >= 0.3 and oiDelta > - 100_000 and not bearDiv
    cond2 = side == 'Sell' and percentDelta <= -0.3 and oiDelta > - 100_000 and not bullDiv
    cond3 = side == 'Buy' and bullDiv
    cond4 = side == 'Sell' and bearDiv

    if cond1 or cond2 or cond3 or cond4:

        conditionString = str(cond1) + ' ' + str(cond2) + ' ' + str(cond3) + ' ' + str(cond4) + ' ' + fcString

        print('VOLSWITCH CONDITIONS: ' + conditionString)

        r.set('discord_' + 'BTC', 'VOLSWITCH CONDITIONS: ' + conditionString)

        MO = marketOrder(side, volumeControl[side]['fraction'], volumeControl[side]['stop'], volumeControl[side]['profit'], 'volswitch')

        if MO:
            resetCoinDict(coinDict, side, 'volswitch')
            msg = 'Volume Action: ' + volumeControl['side'] + ' ' +  str(percentDelta)
            print('MARKET MESSAGE ' + msg)
        else:
            print('MARKET ORDER FAIL')

    print('ACTION VOLUME')

    return blocks[-1]



def resetCoinDict(coinDict, side, mode):

    coinDict['BTC'][mode][side]['swing'] = False
    coinDict['BTC'][mode][side]['price'] = 0

    if coinDict['BTC'][mode][side]['backup'] and coinDict['BTC'][mode][side]['backup'] > 0:
        coinDict['BTC'][mode][side]['price'] = int(coinDict['BTC'][mode][side]['backup'])
        coinDict['BTC'][mode][side]['backup'] = 0

    if mode == 'deltaswitch':
        coinDict['BTC'][mode][side]['active'] = False



    r.set('coinDict', json.dumps(coinDict))
    r.set('discord_' + 'BTC', 'coinDict Reset: ' + side + ' ' + mode)


def setCoinDict():
    deltaDict = {
                'fcCheck': 7,
                'block' : 100_000,
                'Sell' : {
                    'price' : 0,
                    'swing' : False,
                    'active' : False,
                    'fraction' : 0.6,
                    'stop' : 100,
                    'profit' : 300,
                    'backup' : 0
                },
                'Buy' : {
                    'price' : 0,
                    'swing' : False,
                    'active' : False,
                    'fraction' : 0.6,
                    'stop' : 100,
                    'profit' : 300,
                    'backup' : 0
                }
            }

    volDict = {
                    'Sell' : {
                        'price' : 0,
                        'swing' : False,
                        'fraction' : 0.6,
                        'stop' : 100,
                        'profit' : 300,
                        'backup' : 0
                    },
                    'Buy' : {
                        'price' : 0,
                        'swing' : False,
                        'fraction' : 0.6,
                        'stop' : 100,
                        'profit' : 300,
                        'backup' : 0
                    }
                }

    coinDict = {
            'BTC' : {
                'oicheck' : [1_500_000, 2_000_000],
                'volume' : [True, 5],
                'active' : True,
                'imbalances' : False,
                'pause' : False,
                'purge' : False,
                'deltaswitch' : deltaDict,
                'volswitch' : volDict
            },
            'ETH' : {
                'oicheck' : [800_000, 800_000],
                'volume' : [True, 1],
                'active' : False,
                'imbalances' : False,
                'pause' : False,
                'purge' : False,
            },
    }

    r.set('coinDict', json.dumps(coinDict))









