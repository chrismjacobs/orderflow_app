from flask import Flask, flash, render_template, redirect, request, jsonify
from analysis import getVolumeBlock
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





    stream = r.get('stream')

    timeBlocks = r.get('timeblocks')
    timeFlow = r.get('timeflow')

    volumeBlocks = r.get('volumeblocks')
    volumeFlow = r.get('volumeflow')

    if volumeBlockSize > 1:
        volumeBlocks = getVolumeBlock(volumeBlockSize)

    jDict = {
        'volumeBlocks' : volumeBlocks,
        'stream' : stream,
        'volumeFlow' : volumeFlow,
        'timeBlocks' : timeBlocks,
        'timeFlow' : timeFlow
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
