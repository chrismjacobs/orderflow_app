import os
import redis
import json
import discord
from discord.ext import tasks, commands
from datetime import datetime
from pybit import inverse_perpetual, usdt_perpetual


try:
    import config
    LOCAL = True
    REDIS_URL = config.REDIS_URL
    DISCORD_CHANNEL = config.DISCORD_CHANNEL
    DISCORD_TOKEN = config.DISCORD_TOKEN
    DISCORD_USER = config.DISCORD_USER
    API_KEY = config.API_KEY
    API_SECRET = config.API_SECRET
    r = redis.from_url(REDIS_URL, ssl_cert_reqs=None, decode_responses=True)
except:
    REDIS_URL = os.getenv('CELERY_BROKER_URL')
    DISCORD_CHANNEL = os.getenv('DISCORD_CHANNEL')
    DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')
    DISCORD_USER = os.getenv('DISCORD_USER')
    API_KEY = os.getenv('API_KEY')
    API_SECRET = os.getenv('API_SECRET')
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
        elif 'onitor' in msg.content and r.get('monitor') == 'on':
            r.set('monitor', 'off')
            replyText = 'Monitor ' + r.get('monitor')
        elif 'onitor' in msg.content and r.get('monitor') == 'off':
            r.set('monitor', 'on')
            replyText = 'Monitor ' + r.get('monitor')
        elif msg.content == 'Dict' or msg.content == 'dict':
            setCoinDict()
            replyText = 'Coin Dict Set'
        elif msg.content.split(' ')[0] in deltaSet:
            latestprice = float(session.latest_information_for_symbol(symbol='BTCUSD')['result'][0]['last_price'])
            coinDict = json.loads(r.get('coinDict'))
            code = msg.content.split(' ')[0]
            price = int(msg.content.split(' ')[1])
            if price > 100_000 or price < 10_000:
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
                    r.set('coinDict', json.dumps(coinDict))
                    replyText = 'Set: ' + side + ' ' + str(price)
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
        print('Position already open: ' + positionSide)
        return False
    else:
        print('Order continue')

    price = float(session.latest_information_for_symbol(symbol=pair)['result'][0]['last_price'])
    funds = session.get_wallet_balance()['result']['BTC']['equity']
    # leverage = 2
    # session.set_leverage(symbol=pair, leverage=leverage)
    qty = (price * funds * 2) * fraction

    stop_loss = getHL(side, price, stop, mode)

    print('MARKET ORDER ' + str(price))

    limits = {
        'Buy' : -1,
        'Sell' : 1
    }

    sideRev = None

    if side == 'Buy':
        take_profit = price + profit
        sideRev = 'Sell'
    if side == 'Sell':
        take_profit = price - profit
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
        r.set('monitor', 'on')
        position = session.my_position(symbol="BTCUSD")['result']
        positionPrice = float(position['entry_price'])

        try:
        ### place limit TP
            newLimit = session.place_active_order(
            symbol = pair,
            side = sideRev,
            order_type = 'Limit',
            price =  round(positionPrice + (50*limits[sideRev])),
            qty = round(qty/2),
            time_in_force = "GoodTillCancel"
            )
        except Exception as e:
            print('LIMIT ERROR', e)
        else:
            print('LIMIT SUCCESS')


    return True


def actionDELTA(blocks, coin, coinDict):

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
        return False

    fastCandles = 0

    fcCheck = deltaControl['fcCheck']

    currentTimeDelta = 0

    count = 1

    tds = []
    for b in blocks[-(fcCheck+1):]:  ## examine candles leading up to current
        t = b['time_delta']/1000

        # first candle recorded next 7 candles appended
        if count == 8:
            currentTimeDelta = t
        else:
            tds.append(t)
            if t < 5:
                fastCandles += 1
        count += 1

    percentDelta1 = blocks[-1]['delta']/blocks[-1]['total']
    percentDelta2 = blocks[-2]['delta']/blocks[-2]['total']

    threshold = percentDelta1 > 0.99 and percentDelta2 > 0.99
    if side == 'Sell':
        threshold = threshold = percentDelta1 < -0.99 and percentDelta2 < -0.99

    # print('delta pass:  FC=' + str(fastCandles) + ' Prev 7' + json.dumps(tds) + ' Active: ' + str(deltaControl[side]['active'])  + ' Current Time: ' + str(currentTimeDelta) + ' %D ' + str(percentDelta1) + ' Threshold: ' + str(threshold))

    if currentTimeDelta > 5 and fastCandles == fcCheck:
        ## delta action has stalled: lookout is active
        deltaControl[side]['active'] = True
        print('DELTA STALL')
        r.set('coinDict', json.dumps(coinDict))
        return 'AT'
    elif fastCandles == fcCheck:
        deltaControl[side]['active'] = False
        print('DELTA FAST RESET')
        r.set('coinDict', json.dumps(coinDict))
        return 'AF'



    if deltaControl[side]['active'] and threshold:
        print('PLACE DELTA')

        MO = marketOrder(side, deltaControl[side]['fraction'], deltaControl[side]['stop'], deltaControl[side]['profit'], 'deltaswitch')

        if MO:
            resetCoinDict(coinDict, side, 'deltaswitch')
            msg = 'Delta Action: ' + side + ' ' +  str(percentDelta1) + ' ' + str(currentTimeDelta)
            print('DELTA ORDER MESSAGE ' + msg)
            return 'MO'
        else:
            return 'MF'

    print('ACTION DELTA')

    return threshold


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
            resetCoinDict(coinDict, side, 'volswtich')
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









