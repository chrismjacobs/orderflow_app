import os
import redis
import json
import discord
from discord.ext import tasks, commands


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


def actionDELTA(blocks):
    # lastElements = [-2, -3, -4, -5, -6]
    #     timeElements = []

    #     if len(blocks) >= 7:
    #         for t in lastElements:
    #             timeDelta = blocks[t]['time_delta']/1000
    #             timeElements.append(round(timeDelta))
    #             if timeDelta < 30:
    #                 fastCandles += 1


    #     if fastCandles >= 3:
    #         if switchUp:
    #             switch = True
    #             r.set('discord_' + coin, 'Delta Switch Up: ' + json.dumps(timeElements) )
    #             streamAlert('Delta Switch Up: ' + json.dumps(timeElements), 'Delta', coin)
    #         if switchDown:
    #             switch = True
    #             r.set('discord_' + coin, 'Delta Switch Down: ' + json.dumps(timeElements) )
    #             streamAlert('Delta Switch Down: ' + json.dumps(timeElements), 'Delta', coin)

    print('ACTION DELTA')

