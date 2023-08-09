import os
import redis
import json



try:
    import config
    SECRET_KEY = config.SECRET_KEY
    REDIS_IP = config.REDIS_IP
    REDIS_PASS = config.REDIS_PASS
    LOCAL = True
    DEBUG = True
    print('SUCCESS')
except:
    SECRET_KEY = os.getenv('SECRET_KEY')
    LOCAL = False
    DEBUG = False
    REDIS_IP = os.getenv('REDIS_IP')
    REDIS_PASS = os.getenv('REDIS_PASS')

    print('EXCEPTION')


r = redis.Redis(
    host=REDIS_IP,
    port=6379,
    password=REDIS_PASS,
    decode_responses=True
    )

