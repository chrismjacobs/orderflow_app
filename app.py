from flask import Flask, flash, render_template, redirect, request, jsonify, url_for
from flask_login import current_user, login_required, LoginManager
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_mail import Mail
import json
from analysis import getBlocks, getVWAP, getImbalances
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
@login_required
def home():

    if START_CODE == 'block':
        return redirect('/login')

    return render_template('orderflow.html')

@app.route('/setDelta', methods=['POST'])
def setDelta():

    coinDict = request.form['coinOBJ']

    print(coinDict)

    r.set('coinDict', coinDict)

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
