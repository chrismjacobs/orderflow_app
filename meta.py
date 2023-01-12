import boto3
import os
import redis
from pybit import inverse_perpetual, usdt_perpetual

try:
    import config
    API_KEY = config.API_KEY
    API_SECRET = config.API_SECRET
    AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
    SECRET_KEY = config.SECRET_KEY
    SQLALCHEMY_DATABASE_URI = config.SQLALCHEMY_DATABASE_URI
    REDIS_URL = config.REDIS_URL
    START_CODE = None
    LOCAL = True
    DEBUG = True
    r = redis.from_url(REDIS_URL, ssl_cert_reqs=None, decode_responses=True)
    print('SUCCESS')
except:
    print('EXCEPTION')
    SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI')
    AWS_ACCESS_KEY_ID = os.getenv['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = os.getenv['AWS_SECRET_ACCESS_KEY']
    SECRET_KEY = os.getenv['SECRET_KEY']
    API_KEY = os.getenv['API_KEY']
    API_SECRET = os.getenv['API_SECRET']
    START_CODE = os.getenv['START_CODE']
    REDIS_URL = os.getenv('CELERY_BROKER_URL')
    LOCAL = False
    DEBUG = False



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