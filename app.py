from flask import Flask, render_template, request, jsonify
import json
from analysis import getBlocks, getVWAP, getImbalances
from meta import SECRET_KEY, DEBUG, r
import datetime as dt


app = Flask(__name__)
app.config['DEBUG'] = DEBUG
app.config['SECRET_KEY'] = SECRET_KEY

@app.route('/')
def home():

    return render_template('orderflow.html')



@app.route('/getOF', methods=['POST'])
def getOF():
    timeBlockSize = int(request.form ['timeBlockSize'])
    coin = request.form ['coin']

    coinDict = r.get('coinDict')
    coinInfo = json.loads(coinDict)[coin]
    size = coinInfo['volume'][1]

    stream = r.get('stream_' + coin)

    timeBlocks = json.loads(r.get('timeblocks_' + coin))
    timeBlocks = getVWAP(timeBlocks, coin)

    lastHistory = {}

    try:
        historyBlocks = json.loads(r.get('history_' + coin))
        if len(historyBlocks) > 0:
            lastHistory = historyBlocks[-1]
            print(lastHistory.keys())
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

    volumeBlocks = {}

    volumeCheck = r.get('volumeblocks_' + coin + str(size))

    if volumeCheck:
        volumeBlocks = json.loads(volumeCheck)


    if 'volumeblocks_' + coin + str(size) in lastHistory:
        ## combine History and current
        volumeBlocks = lastHistory['volumeblocks_' + coin + str(size)] + volumeBlocks

    for tb in timeBlocks:
        tb['tickList'] = getImbalances(tb['tickList'])
    for vb in volumeBlocks:
        vb['tickList'] = getImbalances(vb['tickList'])


    jDict = {
        'stream' : stream,
        'volumeBlocks' : json.dumps(volumeBlocks),
        'timeBlocks' : json.dumps(timeBlocks),
        'deltaBlocks' : json.dumps(deltaBlocks),
        'coin' : coin,
        'coinDict' : coinDict
    }

    jx = jsonify(jDict)

    # print('JSONIFY X', jDict)

    return jx



if __name__ == '__main__':
    app.run()

