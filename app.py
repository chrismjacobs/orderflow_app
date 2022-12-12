from flask import Flask, flash, render_template, redirect, request, jsonify
from tasks import runStream
from celery.contrib.abortable import AbortableTask
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
app.config['DEBUG'] = True
app.secret_key = os.getenv('FLASK_SECRET_KEY', "super-secret")


@app.route('/')
def main():

    return render_template('orderflow.html')



@app.route('/getOF', methods=['POST'])
def getOF():

    volumeBlockSize = int(request.form ['volumeBlockSize'])
    timeBlockSize = int(request.form ['timeBlockSize'])

    volumeBlocks = r.get('blockflow')
    timeBlocks = r.get('timeblocks')
    timeFlow = r.get('timeflow')
    stream = r.get('stream')
    tradeList = r.get('tradeList')


    return jsonify({
        'volumeBlocks' : volumeBlocks,
        'stream' : stream,
        'tradeList' : tradeList,
        'timeBlocks' : timeBlocks,
        'timeFlow' : timeFlow
    })

@app.route('/start')
def start():
    return render_template('start.html')


@app.route('/add', methods=['POST'])
def add_inputs():
    x = int(request.form['x'] or 0)

    if x == 825:
        task = runStream.delay()
        r.set('task_id', task)
        print('task_id', task)
        flash("Your command has been submitted: " + str(x))
    elif x == 212:
        task_id = r.set('task_id')
        task = runStream.AsyncResult(task_id)
        print('abort task', task)
        task.abort()
        flash("Your command has been submitted: " + str(x))
    else:
        flash("Your command has failed: " + str(x))

    return redirect('/')



if __name__ == '__main__':
    app.run()
