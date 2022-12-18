import discord
import time
from discord.ext import tasks
import redis
import os
from celery import Celery
from celery.utils.log import get_task_logger


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

app = Celery('tasks', broker=REDIS_URL, backend=REDIS_URL)
logger = get_task_logger(__name__)

# async def send_message(message, user_message, is_private):
#     try:
#         response = responses.get_response(user_message)
#         await message.author.send(response) if is_private else await message.channel.send(response)

#     except Exception as e:
#         print(e)

@app.task()
def runBot():
    ## intents controls what the bot can do; in this case read message content
    intents = discord.Intents.default()
    intents.message_content = True
    intents.members = True
    client = discord.Client(intents=intents)


    @client.event
    async def on_ready():
        print(f'{client.user} is now running!')
        checkRedis.start()

    @tasks.loop(seconds=10)
    async def checkRedis():
        print('DISCORD REDIS')
        channel = client.get_channel(DISCORD_CHANNEL)
        user = client.get_user(DISCORD_USER)
        print(user)

        if r.get('discord') != 'blank':
            await user.send(r.get('discord'))
            r.set('discord', 'blank')


    # @client.event
    # async def on_message(message):

    #     ## this stops the infinite loop where bot replies to it's own message
    #     if message.author == client.user:
    #         return

    #     print(message)

    #     username = str(message.author)
    #     user_message = str(message.content)
    #     channel = str(message.channel)

    #     print(f'{username} said: "{user_message}" ({channel})')


    #     if user_message[0] == '?':
    #         user_message = user_message[1:]
    #         await send_message(message, user_message, is_private=True)
    #     else:
    #         await send_message(message, user_message, is_private=False)

    client.run(DISCORD_TOKEN)

if LOCAL:
    runBot()
