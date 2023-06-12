from flask import Flask, flash, render_template, redirect, request, jsonify, url_for
from flask_login import current_user, login_required, LoginManager
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_mail import Mail
from time import sleep
import json
import redis
import os
import time
import requests
from analysis import getBlocks, getVWAP, getImbalances
from meta import SECRET_KEY, SQLALCHEMY_DATABASE_URI, DEBUG, r, LOCAL, START_CODE, s3_resource
import datetime as dt

try:
    import config
    REDIS_URL = config.REDIS_URL
    r = redis.from_url(REDIS_URL, ssl_cert_reqs=None, decode_responses=True)
    RENDER_API = config.RENDER_API
    RENDER_SERVICE = config.RENDER_SERVICE
except:
    REDIS_URL = os.getenv('CELERY_BROKER_URL')
    RENDER_API = os.getenv('RENDER_API')
    RENDER_SERVICE = os.getenv('RENDER_SERVICE')
    r = redis.from_url(REDIS_URL, decode_responses=True)

if not LOCAL:
    from tasks import runStream


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['DEBUG'] = DEBUG
app.config['SECRET_KEY'] = SECRET_KEY
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)
bcrypt = Bcrypt()
login_manager = LoginManager(app)
login_manager.login_view = 'login' # if user isn't logged in it will redirect to login page
login_manager.login_message_category = 'info'


@app.route('/')
@login_required
def home():

    if START_CODE == 'block':
        return redirect('/login')

    return render_template('orderflow.html')

@app.route('/setPrices', methods=['POST'])
def setPrices():

    coinDict = json.loads(request.form['coinOBJ'])
    reset = request.form['reset']


    print(reset, coinDict, type(coinDict))

    if reset == 'true':
        coinDict['BTC']['deltaswitch']['Sell']['active'] = False
        coinDict['BTC']['deltaswitch']['Sell']['swing'] = False
        coinDict['BTC']['deltaswitch']['Sell']['price'] = 0

        coinDict['BTC']['deltaswitch']['Buy']['active'] = False
        coinDict['BTC']['deltaswitch']['Buy']['swing'] = False
        coinDict['BTC']['deltaswitch']['Buy']['price'] = 0

        r.set('coinDict', json.dumps(coinDict))
        r.set('discord_' + 'BTC', 'coinDict Reset')
    else:
        r.set('coinDict', json.dumps(coinDict))

    return jsonify({'coinDict' : coinDict})


@app.route('/getOF', methods=['POST'])
def getOF():
    timeBlockSize = int(request.form ['timeBlockSize'])
    coin = request.form ['coin']

    # print('BLOCK SIZES', coin, volumeBlockSize, timeBlockSize)

    coinDict = r.get('coinDict')
    coinInfo = json.loads(coinDict)[coin]
    size = coinInfo['volume'][1]

    stream = r.get('stream_' + coin)

    timeBlocks = json.loads(r.get('timeblocks_' + coin))
    #timeFlow = r.get('timeflow_' + coin)

    timeBlocks = getVWAP(timeBlocks, coin)

    # deltaBlocks = r.get('deltablocks_' + coin)
    # deltaFlow = r.get('deltaflow_' + coin)

    lastHistory = {}

    try:
        historyBlocks = json.loads(r.get('history_' + coin))
        if len(historyBlocks) > 0:
            lastHistory = historyBlocks[-1]
            # print(lastHistory.keys())
    except:
        print('NO HISTORY')

    if 'timeblocks_' + coin in lastHistory:
        ## combine History and current
        timeBlocks = lastHistory['timeblocks_' + coin] + timeBlocks

    if timeBlockSize > 5:
        timeBlocks = getBlocks(timeBlockSize/5, timeBlocks)

    deltaBlocks = []

    checkDelta = r.get('deltablocks_' + coin)

    if checkDelta:
        deltaBlocks = json.loads(checkDelta)

    # print('DELTA', deltaBlocks)
    # print('TIME', timeBlocks)

    # if 'deltablocks_' + coin in lastHistory:
    #     ## combine History and current
    #     deltaBlocks = lastHistory['deltablocks_' + coin] + deltaBlocks

    volumeBlocks = {}
    # volumeFlow = {}

    volumeCheck = r.get('volumeblocks_' + coin + str(size))
        # volumeFlow[size] = json.loads(r.get('volumeflow_' + coin + str(size)))

    if volumeCheck:
        volumeBlocks = json.loads(volumeCheck)


    if 'volumeblocks_' + coin + str(size) in lastHistory:
        ## combine History and current
        volumeBlocks = lastHistory['volumeblocks_' + coin + str(size)] + volumeBlocks


    user = False
    if current_user.is_authenticated:
        user = current_user.username


    for tb in timeBlocks:
        tb['tickList'] = getImbalances(tb['tickList'])
    for vb in volumeBlocks:
        vb['tickList'] = getImbalances(vb['tickList'])



    jDict = {
        'stream' : stream,
        'volumeBlocks' : json.dumps(volumeBlocks),
        'timeBlocks' : json.dumps(timeBlocks),
        'deltaBlocks' : json.dumps(deltaBlocks),
        'login' : current_user.is_authenticated,
        'user' : user,
        'coin' : coin,
        'coinDict' : coinDict
    }

    jx = jsonify(jDict)

    # print('JSONIFY X', jDict)

    return jx




@app.route('/serviceAction', methods=['POST'])
def serviceAction():

    action = request.form ['action']

    ## action = suspend or resume

    url = "https://api.render.com/v1/services/" + RENDER_SERVICE

    headers = {"accept": "application/json", "authorization": 'Bearer ' +  RENDER_API}
    # payload = {"clearCache": "do_not_clear"}

    if action != 'check':
        surl = url + "/" + action

        response = requests.post(surl, headers=headers)

        # if action == 'deploy':
        #     response = requests.post(url, json=payload, headers=headers)


        print(response.text)


    sResponse = requests.get(url, headers=headers)
    suspended = json.loads(sResponse.text)['suspended']

    durl = url + "/deploys?limit=20"
    dResponse = requests.get(durl, headers=headers)
    print(json.loads(dResponse.text)[0]['deploy'])
    status = json.loads(dResponse.text)[0]['deploy']['status']

    sDict = {
        'suspended' : suspended,
        'status' : status
    }

    jx = jsonify(sDict)

    # print('JSONIFY X', jDict)

    return jx

@app.route('/start')
@login_required
def start():
    return render_template('start.html')

@app.route('/workerAction', methods=['POST'])
@login_required
def worker():
    x = int(request.form['passcode'])
    print('workerAction', x, START_CODE)

    if x == int(START_CODE):
        block = True

        while block:
            t = dt.datetime.today()
            print(t.minute, t.minute%5)
            if t.minute%5 == 0:
                ## multiple of 5 mins
                block = False
            else:
                time.sleep(5)


        task = runStream.delay()
        r.set('task_id', str(task))
        print('task_id', str(task))
        flash("Your command has been submitted: " + str(task))
    else:
        flash("Your command has failed: " + str(x))

    return render_template('start.html')

@app.route('/workerStop', methods=['POST'])
@login_required
def taskend():
    x = int(request.form['passcode'])
    print('workerStop', x, START_CODE)

    if x == int(START_CODE):
        task_id = r.get('task_id')
        task = runStream.AsyncResult(task_id)
        task.abort()
        print('task aborted', str(task))
        flash("Your command has been submitted: " + str(task))
    else:
        flash("Your command has failed: " + str(x))

    return render_template('start.html')

@app.route("/tradingview", methods=['POST'])
def tradingview_webhook():
    print('TRADING VIEW ACTION: ')
    data = json.loads(request.data)

    if data['code'] != SECRET_KEY:
        return False

    url = "https://api.render.com/v1/services/" + RENDER_SERVICE

    headers = {"accept": "application/json", "authorization": 'Bearer ' +  RENDER_API}
    # payload = {"clearCache": "do_not_clear"}

    durl = url + "/deploys?limit=20"
    dResponse = requests.get(durl, headers=headers)
    print(json.loads(dResponse.text)[0]['deploy'])
    status = json.loads(dResponse.text)[0]['deploy']['status']

    sResponse = requests.get(url, headers=headers)
    suspended = json.loads(sResponse.text)['suspended']

    if suspended == 'suspended':
        surl = url + "/" + 'resume'
        response = requests.post(surl, headers=headers)
        print('TV RESUME ' + response.text)

    elif status == 'live':
        task = runStream.delay()
        print('TV STREAM STARTED')








    return redirect('/')


from routesAdmin import *
from routesJournal import *
from routesTrade import *


if __name__ == '__main__':
    app.run()
