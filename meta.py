import boto3
import os
import redis
import json
from pybit import inverse_perpetual, usdt_perpetual
from functools import wraps
from flask import make_response, request


try:
    import config
    API_KEY = config.API_KEY
    API_SECRET = config.API_SECRET
    AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
    SECRET_KEY = config.SECRET_KEY
    REDIS_URL = config.REDIS_URL
    START_CODE = config.START_CODE
    LOCAL = True
    DEBUG = True
    # r = redis.from_url(REDIS_URL, ssl_cert_reqs=None, decode_responses=True)
    RENDER_API = config.RENDER_API
    RENDER_SERVICE = config.RENDER_SERVICE
    LOGIN = config.LOGIN
    REDIS_IP = config.REDIS_IP
    REDIS_PASS = config.REDIS_PASS
    print('SUCCESS')
except:
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    SECRET_KEY = os.getenv('SECRET_KEY')
    API_KEY = os.getenv('API_KEY')
    API_SECRET = os.getenv('API_SECRET')
    START_CODE = os.getenv('START_CODE')
    REDIS_URL = os.getenv('CELERY_BROKER_URL')
    RENDER_API = os.getenv('RENDER_API')
    RENDER_SERVICE = os.getenv('RENDER_SERVICE')
    LOCAL = False
    DEBUG = False
    LOGIN = os.getenv('LOGIN')
    REDIS_IP = os.getenv('REDIS_IP')
    REDIS_PASS = os.getenv('REDIS_PASS')

    print('EXCEPTION')

try:
    LOGIN_DEETS = json.loads(LOGIN)
except:
    LOGIN_DEETS = '{"user": "Fail", "code": "0"}'

r = redis.Redis(
    host=REDIS_IP,
    port=6379,
    password=REDIS_PASS,
    decode_responses=True
    )

def auth_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        username = LOGIN_DEETS['user']
        passcode = LOGIN_DEETS['code']

        if auth and auth.username == username and auth.password == passcode:
            return f(*args, **kwargs)
        return make_response("<h1>Access denied!</h1>", 401, {'WWW-Authenticate': 'Basic realm="Login required!"'})

    return decorated


s3_resource = boto3.resource('s3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key= AWS_SECRET_ACCESS_KEY)

session = inverse_perpetual.HTTP(
    endpoint='https://api.bybit.com',
    api_key= API_KEY,
    api_secret=API_SECRET
)

print('bybit session', session)

session_unauth_USD = usdt_perpetual.HTTP(
    endpoint="https://api.bybit.com"
)
session_unauth_USDT = inverse_perpetual.HTTP(
    endpoint="https://api.bybit.com"
)