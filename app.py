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

    volumeBlockSize = int(request.form ['volumeBlockSize'])
    timeBlockSize = int(request.form ['timeBlockSize'])

    print('BLOCK SIZES', volumeBlockSize, timeBlockSize)


    stream = r.get('stream')

    timeBlocks = r.get('timeblocks')
    timeFlow = r.get('timeflow')

    volumeBlocks = r.get('volumeblocks')
    volumeFlow = r.get('volumeflow')

    deltaBlocks = r.get('deltablocks')
    deltaFlow = r.get('deltaflow')

    lastHistory = {}

    historyBlocks = json.loads(r.get('history'))
    if len(historyBlocks) > 0:
        lastHistory = historyBlocks[-1]


    if lastHistory['timeblocks']:
        ## combine History and current
        currentTime = json.loads(timeBlocks)
        newTime = lastHistory['timeblocks'] + currentTime
        timeBlocks = json.dumps(newTime)

    if lastHistory['deltablocks']:
        ## combine History and current
        currentDelta = json.loads(deltaBlocks)
        newDelta = lastHistory['deltablocks'] + currentDelta
        deltaBlocks = json.dumps(newDelta)

    if lastHistory['volumeblocks']:
        ## combine History and current
        currentVolume = json.loads(volumeBlocks)
        newVolume = lastHistory['volumeblocks'] + currentVolume
        volumeBlocks = json.dumps(newVolume)




    if volumeBlockSize == 2:
        volumeBlocks = r.get('volumeblocks2m')

        if lastHistory['volumeblocks2m']:
            ## combine History and current
            currentVolume2m = json.loads(volumeBlocks)
            newVolume2m = lastHistory['volumeblocks2m'] + currentVolume2m
            volumeBlocks = json.dumps(newVolume2m)


    if volumeBlockSize == 5:
        volumeBlocks = r.get('volumeblocks5m')

        if lastHistory['volumeblocks5m']:
            ## combine History and current
            currentVolume5m = json.loads(volumeBlocks)
            newVolume5m = lastHistory['volumeblocks5m'] + currentVolume5m
            volumeBlocks = json.dumps(newVolume5m)


    if timeBlockSize > 5:
        timeBlocks = getBlocks(timeBlockSize/5, timeBlocks)


    user = False
    if current_user.is_authenticated:
        user = current_user.username

    jDict = {
        'stream' : stream,
        'volumeBlocks' : volumeBlocks,
        'volumeFlow' : volumeFlow,
        'timeBlocks' : timeBlocks,
        'timeFlow' : timeFlow,
        'deltaBlocks' : deltaBlocks,
        'deltaFlow' : deltaFlow,
        'login' : current_user.is_authenticated,
        'user' : user
    }

    jx = jsonify(jDict)

    # print('JSONIFY X', jDict)

    return jx

@app.route('/start')
def start():
    return render_template('start.html')

@app.route('/worker', methods=['POST'])
def worker():
    x = int(request.form['x'] or 0)

    if x == START_CODE:
        task = runStream.delay()
        r.set('task_id', str(task))
        print('task_id', str(task))
        flash("Your command has been submitted: " + str(task))
    else:
        flash("Your command has failed: " + str(x))

    return redirect('/')

@app.route("/tradingview", methods=['POST'])
def tradingview_webhook():
    data = json.loads(request.data)

    return redirect('/')


from routesAdmin import *
from routesJournal import *
from routesTrade import *


if __name__ == '__main__':
    app.run()
