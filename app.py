from flask import Flask, flash, render_template, redirect, request, jsonify, url_for
from flask_login import current_user, login_required, LoginManager
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_mail import Mail
import json
from analysis import getBlocks
from meta import SECRET_KEY, SQLALCHEMY_DATABASE_URI, DEBUG, r, LOCAL, START_CODE, s3_resource

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
def home():

    return render_template('orderflow.html')

@app.route('/getOF', methods=['POST'])
def getOF():

    timeBlockSize = int(request.form ['timeBlockSize'])
    coin = request.form ['coin']

    # print('BLOCK SIZES', coin, volumeBlockSize, timeBlockSize)

    coinDict = r.get('coinDict')
    coinInfo = json.loads(coinDict)[coin]

    stream = r.get('stream_' + coin)

    timeBlocks = r.get('timeblocks_' + coin)
    timeFlow = r.get('timeflow_' + coin)


    # deltaBlocks = r.get('deltablocks_' + coin)
    # deltaFlow = r.get('deltaflow_' + coin)

    lastHistory = {}

    historyBlocks = json.loads(r.get('history_' + coin))
    if len(historyBlocks) > 0:
        lastHistory = historyBlocks[-1]

    if 'timeblocks_' + coin in lastHistory:
        ## combine History and current
        currentTime = json.loads(timeBlocks)
        newTime = lastHistory['timeblocks_' + coin] + currentTime
        timeBlocks = json.dumps(newTime)

    if timeBlockSize > 5:
        timeBlocks = getBlocks(timeBlockSize/5, timeBlocks)

    # if 'deltablocks' in lastHistory:
    #     ## combine History and current
    #     currentDelta = json.loads(deltaBlocks)
    #     newDelta = lastHistory['deltablocks'] + currentDelta
    #     deltaBlocks = json.dumps(newDelta)

    volumeBlocks = {}
    # volumeFlow = {}

    for size in coinInfo['volsize']:
        volumeBlocks[size] = json.loads(r.get('volumeblocks_' + coin + str(size)))
        # volumeFlow[size] = json.loads(r.get('volumeflow_' + coin + str(size)))

        if 'volumeblocks_' + coin + str(size) in lastHistory:
        ## combine History and current
            currentVolume = volumeBlocks[size]
            newVolume = lastHistory['volumeblocks_' + coin + str(size)] + currentVolume
            volumeBlocks[size] = newVolume

    user = False
    if current_user.is_authenticated:
        user = current_user.username

    jDict = {
        'stream' : stream,
        'volumeBlocks' : json.dumps(volumeBlocks),
        # 'volumeFlow' : json.dumps(volumeFlow),
        'timeBlocks' : timeBlocks,
        # 'timeFlow' : timeFlow,
        # 'deltaBlocks' : deltaBlocks,
        # 'deltaFlow' : deltaFlow,
        'login' : current_user.is_authenticated,
        'user' : user,
        'coin' : coin,
        'coinDict' : coinDict
    }

    jx = jsonify(jDict)

    # print('JSONIFY X', jDict)

    return jx

@app.route('/start')
@login_required
def start():
    return render_template('start.html')

@app.route('/workerAction', methods=['POST'])
@login_required
def worker():
    x = int(request.form['x'] or 0)
    print('workerAction', x, START_CODE)

    if x == int(START_CODE):
        task = runStream.delay()
        r.set('task_id', str(task))
        print('task_id', str(task))
        flash("Your command has been submitted: " + str(task))
    else:
        flash("Your command has failed: " + str(x))

    return render_template('start.html')

@app.route("/tradingview", methods=['POST'])
def tradingview_webhook():
    # data = json.loads(request.data)
    print('TRADING VIEW ACTION')

    return redirect('/')


from routesAdmin import *
from routesJournal import *
from routesTrade import *


if __name__ == '__main__':
    app.run()
