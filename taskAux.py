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

        channelDict = json.loads(r.get('channelDict'))

        for coin in channelDict:

            ## need incase redis gets wiped
            if not r.get('discord_' + coin):
                r.set('discord_' + coin, 'discord set')

            channel = bot.get_channel(int(channelDict[coin]))

            # print(channel, int(channelDict[coin]))

            if r.get('discord_' + coin) != 'blank':
                msg = r.get('discord_' + coin)
                await channel.send(msg)
                r.set('discord_' + coin, 'blank')

    @bot.event
    async def on_message(msg):
        user = bot.get_user(int(DISCORD_USER))
        # print('MESSAGE DDDDDDDDD', msg.content)
        replyText = 'ho'

        if msg.content == 'B':
            lastCandle = json.loads(r.get('timeblocks_BTC'))[-2]
            print(lastCandle)
            oi = round(lastCandle['oi_delta']/1000)
            b = round(lastCandle['buys']/1000)
            s = round(lastCandle['sells']/1000)
            replyText = str(lastCandle['total']) + ' OI: ' + str(oi) + 'k Buys: ' + str(b) + 'k Sells: ' + str(s) + 'k'

        if msg.content == 'Ansi' and r.get('ansi') == 'on':
            r.set('ansi', 'off')
            replyText = 'Ansi off'
        elif msg.content == 'Ansi' and r.get('ansi') == 'off':
            r.set('ansi', 'on')
            replyText = 'Ansi on'

        if msg.content == 'Stack' and r.get('stack') == 'on':
            r.set('stack', 'off')
            replyText = 'Stacks on'
        elif msg.content == 'Stack' and r.get('stack') == 'off':
            r.set('stack', 'on')
            replyText = 'Stacks off'


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
    else:
        r.set('discord_' + coin, msg)


def actionBIT(side):
    r.set('discord_BIT', side)
    print('ACTION BIT')

def getHL(side, current, stop):

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
            stop_loss = mLow - 11

    if side == 'Sell':
        distance = abs(current - mHi)
        if distance > stop:
            stop_loss = current + stop
        else:
            stop_loss = mHi + 11


    return stop_loss

def marketOrder(side, fraction, stop, profit):

    position = session.my_position(symbol="BTCUSD")['result']

    positionSide = position['side']
    positionSize = int(position['size'])
    positionLev = float(position['leverage'])

    if positionSize > 0 or positionLev > 2:
        print('Position already open: ' + positionSide)
        return False

    price = float(session.latest_information_for_symbol(symbol="BTCUSD")['result'][0]['last_price'])
    funds = session.get_wallet_balance()['result']['BTC']['equity']
    # leverage = 2
    # session.set_leverage(symbol="BTCUSD", leverage=leverage)
    qty = (price * funds * 2) * fraction

    stop_loss = getHL(side, price, stop)

    print('MARKET ORDER ' + str(price))

    if side == 'Buy':
        take_profit = price + profit

    if side == 'Sell':
        take_profit = price - profit


    order = session.place_active_order(
    symbol="BTCUSD",
    side=side,
    order_type='Market',
    price=None,
    stop_loss = stop_loss,
    take_profit = take_profit,
    qty=qty,
    time_in_force="GoodTillCancel"
    )

    message = order['ret_msg']
    data = json.dumps(order['result'])

    print('ORDER', order)
    print('MESSAGE', message)
    print('DATA', data)

    return True


def actionDELTA(blocks, coin, coinDict):

    deltaControl = coinDict[coin]['delta']

    if deltaControl['Buy']['price'] == 0 and deltaControl['Sell']['price'] == 0:
        print('zero')
        return False

    if deltaControl['Sell']['price'] > 0 and blocks[-1]['high'] > deltaControl['Sell']['price'] and deltaControl['Sell']['swing'] == False:
        deltaControl['Sell']['swing'] = True
        deltaControl['Buy']['swing'] = False
        print('SELL SWING TRUE')
        r.set('coinDict', json.dumps(coinDict))
        return True

    if deltaControl['Buy']['price'] > 0 and blocks[-1]['low'] < deltaControl['Buy']['price'] and deltaControl['Buy']['swing'] == False:
        deltaControl['Buy']['swing'] = True
        deltaControl['Sell']['swing'] = False
        print('BUY SWING TRUE')
        r.set('coinDict', json.dumps(coinDict))
        return True

    side = None
    if deltaControl['Sell']['swing'] == True:
        side = 'Sell'
    elif deltaControl['Buy']['swing'] == True:
        side = 'Buy'
    else:
        return False


    fastCandles = 0

    currentTimeDelta = 0

    count = 1

    tds = []
    for b in blocks[-8:]:
        t = b['time_delta']/1000

        # first candle recorded next 7 candles appended
        if count == 8:
            currentTimeDelta = t
        else:
            tds.append(t)
            if t < 5:
                fastCandles += 1
        count += 1

    percentDelta = blocks[-1]['delta']/blocks[-1]['total']

    print('delta pass:  FC=' + str(fastCandles) + ' Prev 7' + json.dumps(tds) + ' Active: ' + str(deltaControl[side]['active'])  + ' CT:' + str(currentTimeDelta) + ' %D' + str(percentDelta))

    if currentTimeDelta > 5 and fastCandles > 6 and deltaControl[side]['active'] == False:
        ## delta action has stalled: lookout is active
        deltaControl[side]['active'] = True
        print('DELTA STALL')
        r.set('coinDict', json.dumps(coinDict))
        return True
    elif fastCandles > 6 and deltaControl[side]['active'] == True:
        deltaControl[side]['active'] = False
        print('DELTA FAST RESET')
        r.set('coinDict', json.dumps(coinDict))
        return True

    threshold = percentDelta > 0.9
    if side == 'Sell':
        threshold = percentDelta < -0.9

    if deltaControl[side]['active'] and threshold:

        MO = marketOrder(side, deltaControl[side]['fraction'], deltaControl[side]['stop'], deltaControl[side]['profit'])

        if MO:
            resetCoinDict(coinDict, side)
            msg = 'Delta Action: ' + deltaControl['side'] + ' ' +  str(percentDelta) + ' ' + str(currentTimeDelta)
            print('MARKET MESSAGE ' + msg)
        else:
            print('MARKET ORDER FAIL')

    print('ACTION DELTA')

    return blocks[-1]


def actionVOLUME(blocks, coin, coinDict, bullDiv, bearDiv):

    volumeControl = coinDict[coin]['volume']

    if volumeControl['Buy']['price'] == 0 and volumeControl['Sell']['price'] == 0:
        print('zero')
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

    fastCandle = None
    for b in blocks[-4:-2]:
        if b['time_delta']/1000 < 60:
            fastCandle = {
                    'time' : b['time_delta']/1000
                }
        else:
            return False

    print('VOLUME SWING ACTIVE')

    percentDelta = blocks[-1]['delta']/blocks[-1]['total']
    oiDelta = blocks[-1]['oi_delta']

    print('volume pass:  FC=' + json.dumps(fastCandle) + ' %d:' + str(percentDelta) + ' oi:' + str(oiDelta))

    cond1 = side == 'Buy' and percentDelta > 0.3 and oiDelta > - 100_000
    cond2 = side == 'Sell' and percentDelta < 0.3 and oiDelta > - 100_000
    cond3 = side == 'Buy' and bullDiv
    cond4 = side == 'Sell' and bearDiv

    if cond1 or cond2 or cond3 or cond4:

        print('VOLUME CONDITIONS: ', cond1, cond2, cond3, cond4)

        MO = marketOrder(side, volumeControl[side]['fraction'], volumeControl[side]['stop'], volumeControl[side]['profit'])

        if MO:
            resetCoinDict(coinDict, side)
            msg = 'Volume Action: ' + volumeControl['side'] + ' ' +  str(percentDelta)
            print('MARKET MESSAGE ' + msg)
        else:
            print('MARKET ORDER FAIL')

    print('ACTION VOLUME')

    return blocks[-1]



def resetCoinDict(coinDict, side):


    coinDict['BTC']['delta'][side]['active'] = False
    coinDict['BTC']['delta'][side]['swing'] = False
    coinDict['BTC']['delta'][side]['price'] = 0

    if coinDict['BTC']['delta'][side]['backup'] > 0:
        coinDict['BTC']['delta'][side]['price'] = int(coinDict['BTC']['delta'][side]['backup'])
        coinDict['BTC']['delta'][side]['backup'] = 0


    r.set('coinDict', json.dumps(coinDict))
    r.set('discord_' + 'BTC', 'coinDict Reset: ' + side)







