from flask import Flask, flash, render_template, redirect, request, jsonify
from tasks import runStream
from celery.schedules import crontab
import os, json
import redis

try:
    import config
    LOCAL = True
    REDIS_URL = config.REDIS_URL
    r = redis.from_url(REDIS_URL, ssl_cert_reqs=None, decode_responses=True)
except:
    REDIS_URL = os.getenv('CELERY_BROKER_URL')
    r = redis.from_url(REDIS_URL, decode_responses=True)


print('URL', REDIS_URL)
print('REDIS', r)

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', "super-secret")


@app.route('/')
def main():

    return render_template('orderflow.html')


@app.route('/getOF', methods=['POST'])
def getOF():
    volumeblocks = r.get('blockflow')
    timeblocks = r.get('timeblocks')
    stream = r.get('stream')
    tradeList = r.get('tradeList')


    return jsonify({
        'volumeblocks' : volumeblocks,
        'stream' : stream,
        'tradeList' : tradeList,
        'timeblocks' : timeblocks
    })

@app.route('/start')
def start():
    return render_template('start.html')


@app.route('/add', methods=['POST'])
def add_inputs():
    x = int(request.form['x'] or 0)

    if x == 825:
        runStream.delay()
        flash("Your command has been submitted: " + str(x))
    else:
        flash("Your command has failed: " + str(x))

    return redirect('/')

if __name__ == '__main__':
    app.run()
