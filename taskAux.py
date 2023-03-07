import os
import redis
import json
import discord
from discord.ext import tasks, commands
from meta import session
from datetime import datetime

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

        coinDict = json.loads(r.get('coinDict'))

        for coin in coinDict:

            ## need incase redis gets wiped
            if not r.get('discord_' + coin):
                r.set('discord_' + coin, 'discord set')

            channel = bot.get_channel(int(coinDict[coin]['channel']))

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
            oi = round(lastCandle['oi_delta']/1000)
            b = round(lastCandle['buys']/1000)
            s = round(lastCandle['sells']/1000)
            replyText = str(lastCandle['total']) + ' OI: ' + str(oi) + 'k Buys: ' + str(b) + 'k Sells: ' + str(s) + 'k'

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

    noAnsi = False
    if noAnsi:
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

def marketOrder(side, fraction, stop):

    print('MARKET ORDER')

    position = session.my_position(symbol="BTCUSD")['result']

    positionSide = position['side']
    positionSize = int(position['size'])
    positionLev = float(position['leverage'])

    if positionSize > 0 or positionLev > 2:
        return False

    price = float(session.latest_information_for_symbol(symbol="BTCUSD")['result'][0]['last_price'])
    funds = session.get_wallet_balance()['result']['BTC']['equity']
    # leverage = 2
    # session.set_leverage(symbol="BTCUSD", leverage=leverage)
    qty = (price * funds * 2) * fraction

    stop_loss = getHL(side, price, stop)

    if side == 'Buy':
        take_profit = price + 60

    if side == 'Sell':
        take_profit = price - 60


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

    print(deltaControl, len(blocks), blocks[-1]['close'], deltaControl['price'], deltaControl['side'] == ['Sell'], blocks[-1]['close'] > deltaControl['price'])

    if deltaControl['price'] == 0:
        print('zero')
        return False
    elif deltaControl['swing'] == True:
        print('swing true')
        pass
    elif deltaControl['side'] == 'Sell' and blocks[-1]['close'] > deltaControl['price']:
        deltaControl['swing'] = True
        print('SELL SWING TRUE')
        r.set('coinDict', json.dumps(coinDict))
        return True
    elif deltaControl['side'] == 'Buy' and blocks[-1]['close'] < deltaControl['price']:
        deltaControl['swing'] = True
        print('BUY SWING TRUE')
        r.set('coinDict', json.dumps(coinDict))
        return True
    else:
        print('swing false')
        return False


    fastCandles = 0

    lastElements = [-2, -3, -4, -5, -6, -7, -8]  # -9, -10, -11
    timeElements = []

    if len(blocks) >= 11:
        for t in lastElements:
            timeDelta = blocks[t]['time_delta']/1000
            timeElements.append(round(timeDelta))
            if timeDelta <= 5:
                fastCandles += 1


    currentTimeDelta = blocks[-1]['time_delta']/1000

    print('stage two pass ' + str(fastCandles) + ' ' + str(currentTimeDelta) + ' ' + str(deltaControl['active']))

    if currentTimeDelta > 5 and fastCandles > 6 and deltaControl['active'] == False:
        ## delta action has stalled: lookout is active
        deltaControl['active'] = True
        print('DELTA STALL')
        r.set('coinDict', json.dumps(coinDict))
        return True
    elif fastCandles > 6 and deltaControl['active'] == True:
        deltaControl['active'] = False
        print('DELTA FAST RESET')
        r.set('coinDict', json.dumps(coinDict))
        return True

    percentDelta = blocks[-1]['delta']/blocks[-1]['total']

    if deltaControl['active'] and deltaControl['side'] == 'Sell' and percentDelta < -0.9:
        deltaControl['price'] == 0
        deltaControl['active'] == False
        deltaControl['swing'] == False
        r.set('coinDict', json.dumps(coinDict))
        marketOrder('Sell', deltaControl['fraction'], deltaControl['stop'])
        r.set('discord_' + 'BTC', 'Delta Action: ' + deltaControl['side'] + ' ' +  str(percentDelta) + ' ' + str(currentTimeDelta))

    elif deltaControl['active'] and deltaControl['side'] == 'Buy' and percentDelta > 0.9:
        deltaControl['price'] == 0
        deltaControl['active'] == False
        deltaControl['swing'] == False
        r.set('coinDict', json.dumps(coinDict))
        marketOrder('Buy', deltaControl['fraction'], deltaControl['stop'])
        r.set('discord_' + 'BTC', 'Delta Action: ' + deltaControl['side'] + ' ' +  str(percentDelta) + ' ' + str(currentTimeDelta))

    print('ACTION DELTA')







