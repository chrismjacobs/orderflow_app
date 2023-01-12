from app import app
import json
from flask import Flask, request, render_template, jsonify, abort
from flask_login import login_required
from meta import session, START_CODE


@app.route('/trade')
@login_required
def trade():

    return render_template('trading.html')


@app.route('/getData', methods=['POST'])
@login_required
def getData():


    mode = request.form ['mode']
    side = request.form ['side']
    minutes = request.form ['minutes']
    risk = float(request.form ['risk'])
    first = float(request.form ['first'])
    fraction = float(request.form ['fraction'])
    stop = float(request.form ['stop'])
    leverage = float(request.form ['leverage'])

    print('MODE:', mode, side, minutes, risk, fraction, stop, first)

    latest = session.latest_information_for_symbol(symbol="BTCUSD")

    getBTC = latest['result'][0]

    position = session.my_position(symbol="BTCUSD")['result']

    positionSide = position['side']
    positionSize = int(position['size'])


    if mode == 'first':
        price = float(getBTC['last_price'])
        return jsonify({'result' : price, 'mode' : mode })

    elif mode == 'leverage':

        position = session.my_position(symbol="BTCUSD")['result']
        print(position)
        positionSize = position['size']
        positionLev = float(position['leverage'])

        print('LEV: ', side, risk, fraction, positionSize, positionLev, leverage)

        if positionSize == 0 and positionLev != leverage:
            leverage = setLeverage(first, stop, risk, fraction, leverage)
        else:
            mode = 'alert'
            leverage = 'leverage no change'

        return jsonify({'result' : leverage, 'mode' : mode})

    elif mode == 'stop':
        price = getHiLow(minutes, side)

        stopAdjust = {
            'Buy' : price - 10,
            'Sell' : price + 10
        }

        stop = stopAdjust[side]

        return jsonify({'result' : stop, 'mode' : mode})

    elif mode == 'funds':
        funds = session.get_wallet_balance()['result']['BTC']['equity']
        print('getFunds', funds)
        return jsonify({'result' : funds, 'mode' : mode})

    elif mode == 'size':
        result = positionSize
        print('position size', result)
        return jsonify({'result' : result, 'mode' : mode})

    elif mode == 'cancel':
        result = session.cancel_all_active_orders(symbol="BTCUSD")['ret_msg']
        print('cancel', result)
        return jsonify({'result' : result, 'mode' : mode})

@app.route('/getOrder', methods=['POST'])
@login_required
def getOrder():

    mode = request.form ['mode']
    side = request.form ['side']
    first = float(request.form ['first'])
    spread = float(request.form ['spread'])
    ladder = int(request.form ['ladder'])
    fraction = float(request.form ['fraction'])
    profit = float(request.form ['profit'])
    stop = float(request.form ['stop'])
    leverage = float(request.form ['leverage'])
    pw = request.form ['pw']

    if pw != START_CODE:
        return jsonify({'result' : 'fail'})

    spreadArray = []

    position = session.my_position(symbol="BTCUSD")['result']
    price = float(session.latest_information_for_symbol(symbol="BTCUSD")['result'][0]['last_price'])
    funds = session.get_wallet_balance()['result']['BTC']['equity']

    ### check/set leverage
    positionLev = float(position['leverage'])
    if float(leverage) != positionLev:
        session.set_leverage(symbol="BTCUSD", leverage=leverage)

    if first == None or first == 0:
        if side == 'Buy':
            first = price + 0.5
        if side == 'Sell':
            first = price - 0.5

    start = 1
    if ladder == 1:
        start = 0
    for i in range(start, ladder+1):
        if side == 'Buy':
            spreadArray.append(first - i*spread)
        else:
            spreadArray.append(first + i*spread)

    qty = (price * funds * leverage) * fraction
    print('QTY', price, funds, leverage, qty)

    result = None

    if profit == 0:
        profit = None

    for value in spreadArray:
        result = placeOrder(side, value, stop, qty/len(spreadArray), profit)

    return jsonify({'result' : result})


def setLeverage(first, stop, risk, fraction, leverage):

    if first == None or first == 0:
        first = float(session.latest_information_for_symbol(symbol="BTCUSD")['result'][0]['last_price'])

    distance = abs(first - stop)

    percent_difference = (distance/first)*100  # as decimal

    lev = round((risk/percent_difference)*fraction, 1)

    print(first, stop, distance, percent_difference, lev)

    if risk == 0:
        lev = leverage

    if lev < 1:
        print('Leverage too low', lev)
    else:
        print(session.set_leverage(symbol="BTCUSD", leverage=lev))

    return lev

def getHiLow(minutes, side):

    from datetime import datetime
    now = datetime.now()
    timestamp = int(datetime.timestamp(now)) - int(minutes)*60

    data = session.query_kline(symbol="BTCUSD", interval="1", from_time=str(timestamp))['result']

    print('GET HI LOW ', len(data))


    hAry = []
    lAry = []

    for i in range(0, len(data)):

        hAry.append(int(data[i]['high'].split('.')[0]))
        lAry.append(int(data[i]['low'].split('.')[0]))

    mHi = max(hAry)
    mLow = min(lAry)

    print(mLow)

    if side == 'Buy':
        return mLow
    else:
        return mHi

def placeOrder(side, price, stop_loss, qty, take_profit):

    order = session.place_active_order(
    symbol="BTCUSD",
    side=side,
    order_type='Limit',
    price=price,
    stop_loss = stop_loss,
    take_profit = take_profit,
    qty=qty,
    time_in_force="GoodTillCancel"
    )


    message = order['ret_msg']
    data = json.dumps(order['result'])

    print('ORDER', order)
    print('MESSAGE', message)
    print('DATA', data)

    return data
