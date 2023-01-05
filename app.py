from flask import Flask, flash, render_template, redirect, request, jsonify
from analysis import getBlocks
import os, json
import redis

LOCAL = False

try:
    import config
    LOCAL = True
    REDIS_URL = config.REDIS_URL
    r = redis.from_url(REDIS_URL, ssl_cert_reqs=None, decode_responses=True)
except:
    REDIS_URL = os.getenv('CELERY_BROKER_URL')
    START_CODE = int(os.getenv('START_CODE'))
    r = redis.from_url(REDIS_URL, decode_responses=True)
    from tasks import runStream
    # from bot import runBot


print('URL', REDIS_URL)
print('REDIS', r)

app = Flask(__name__)
app.config['DEBUG'] = True
app.secret_key = os.getenv('FLASK_SECRET_KEY', "super-secret")

@app.route('/')
def main():


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

    volumeFlow = r.get('volumeflow')
    volumeFlow = r.get('volumeflow')

    lastHistory = {}

    historyBlocks = json.loads(r.get('history'))
    if len(historyBlocks) > 0:
        lastHistory = historyBlocks[-1]


    if lastHistory['timeblocks']:
        ## combine History and current
        current = json.loads(timeBlocks)
        newblocks = lastHistory['timeblocks'] + current
        timeBlocks = json.dumps(newblocks)


    if volumeBlockSize == 2:
        volumeBlocks = r.get('volumeblocks2m')
    if volumeBlockSize == 5:
        volumeBlocks = r.get('volumeblocks5m')
    if timeBlockSize > 5:
        timeBlocks = getBlocks(timeBlockSize/5, timeBlocks)

    deltaBlocks = r.get('deltablocks')
    deltaFlow = r.get('deltaflow')


    jDict = {
        'stream' : stream,
        'volumeBlocks' : volumeBlocks,
        'volumeFlow' : volumeFlow,
        'timeBlocks' : timeBlocks,
        'timeFlow' : timeFlow,
        'deltaBlocks' : deltaBlocks,
        'deltaFlow' : deltaFlow
    }

    jx = jsonify(jDict)

    # print('JSONIFY X', jDict)

    return jx

@app.route('/start')
def start():
    return render_template('start.html')

@app.route('/add', methods=['POST'])
def add_inputs():
    x = int(request.form['x'] or 0)

    if x == START_CODE:
        task = runStream.delay()
        r.set('task_id', str(task))
        print('task_id', str(task))
        flash("Your command has been submitted: " + str(task))
    else:
        flash("Your command has failed: " + str(x))

    return redirect('/')

if __name__ == '__main__':
    app.run()
