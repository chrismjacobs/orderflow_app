
from app import app
from meta import s3_resource
import base64
import datetime
import json
from flask import Flask, request, render_template, jsonify, abort
from flask_login import login_required


@app.route('/journal')
@login_required
def journal():

    mList = [
        'October',
        'January'
    ]

    bucket = 'rekt-journal'

    jDict = {

    }

    for m in mList:

        key = 'tradeJournal_' + m + '.json'
        content_object = s3_resource.Object( bucket, key )
        file_content = content_object.get()['Body'].read().decode('utf-8')

        jDict[m] = json.loads(file_content)


    return render_template('journal.html', tradeJournal=json.dumps(jDict))

@app.route('/recordTrade', methods=['POST'])
@login_required
def recordTrade():

    record = request.form ['record']
    imageArray = request.form ['imageArray']
    currentTrade = request.form ['currentTrade']

    currentDate = datetime.date.today()
    month = currentDate.strftime("%B")

    print ('put MetaFile')
    bucket = 'rekt-journal'
    key = 'tradeJournal_' + month + '.json'

    # with open('static/' + key, 'r') as json_file:
    #     file_content = json_file
    # print(key, json_file, type(file_content))
    # jload = json.loads(file_content)

    content_object = s3_resource.Object( bucket, key )
    file_content = content_object.get()['Body'].read().decode('utf-8')
    jload = json.loads(file_content)


    jload[currentTrade] = {}
    jload[currentTrade]['record'] = json.loads(record)
    jload[currentTrade]['imageArray'] = json.loads(imageArray)

    with open('static/' + key, 'w') as json_file:
        json.dump(jload, json_file)

    jstring = json.dumps(jload)
    s3_resource.Bucket(bucket).put_object(
        Key=key, Body=jstring)

    print('json put in bucket location', bucket, key)

    return jsonify({'result' : 'trade recorded'})


@app.route('/addImage', methods=['POST'])
@login_required
def addImage():

    currentDate = datetime.date.today()
    month = currentDate.strftime("%B")


    b64data = request.form ['b64data']
    imageArray = request.form ['imageArray']
    currentTrade = request.form ['currentTrade']

    print(imageArray, type(imageArray))
    imageSet = json.loads(imageArray)

    count = len(imageSet) + 1

    S3_LOCATION = 'https://rekt-journal.s3.ap-northeast-1.amazonaws.com/'
    S3_BUCKET_NAME = 'rekt-journal'
    print('PROCESSING IMAGE')
    image = base64.b64decode(b64data)
    filename = month + '/' + str(currentTrade) + '/' + str(count) +'.png'
    imageLink = S3_LOCATION + filename
    s3_resource.Bucket(S3_BUCKET_NAME).put_object(Key=filename, Body=image)

    imageSet[count] = imageLink

    return jsonify({'result' : json.dumps(imageSet)})


def shareImage(b64data, log, count, month):

    S3_LOCATION = 'https://rekt-journal-lms.s3.ap-northeast-1.amazonaws.com/'
    S3_BUCKET_NAME = 'rekt-journal'
    print('PROCESSING IMAGE')
    image = base64.b64decode(b64data)
    filename = month + '/' + str(log) + '/' + str(count) +'.png'
    imageLink = S3_LOCATION + filename
    s3_resource.Bucket(S3_BUCKET_NAME).put_object(Key=filename, Body=image)
    return imageLink

def putJson(data, log, month):
    print ('put MetaFile')


    key = 'tradeJournal_' + month + '.json'
    string = "static/" + key

    with open(string, "r") as f:
        jload = json.load(f)

    jload[log] = json.load(data)

    bucket = 'rekt-journal'
    jstring = json.dumps(jload)
    s3_resource.Bucket(bucket).put_object(
        Key=key, Body=jstring)

    print('json put in bucket location', bucket, key)

    return 'ok'