from app import app, r
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

    print('DATA MODE:', mode, side, minutes, risk, fraction, stop, first)

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


@app.route('/manageOrder', methods=['POST'])
@login_required
def manageOrder():

    mode = request.form ['mode']
    breakeven = request.form ['breakeven']
    limitexit = float(request.form ['limitexit'])
    limitprice = int(request.form ['limitprice'])
    limitfraction = float(request.form ['limitfraction'])
    vwapfraction = float(request.form ['vwapfraction'])
    vwapbuffer = float(request.form ['vwapbuffer'])


    pair = 'BTCUSD'

    print('MANAGE MODE:', mode)

    BTCprice = float(session.latest_information_for_symbol(symbol="BTCUSD")['result'][0]['last_price'])

    position = session.my_position(symbol=pair)['result']

    positionSide = position['side']
    positionSize = int(position['size'])
    positionEntry = round(float(position['entry_price']))
    positionStop = round(float(position['stop_loss']))


    if mode == 'cancel':
        result = session.cancel_all_active_orders(symbol=pair)['ret_msg']
        print('cancel', result)
        return jsonify({'result' : result, 'mode' : mode})
    elif mode == 'size':
        return jsonify({'result' : positionSize, 'mode' : mode})
    elif mode == 'breakeven':
        BEprices = {
            'Buy' : positionEntry - int(breakeven),
            'Sell' : positionEntry + int(breakeven)
        }
        responseDict = session.set_trading_stop(symbol=pair, stop_loss=BEprices[positionSide])
        print(responseDict)
        try:
            return jsonify({'result' : responseDict['result']['stop_loss'], 'mode' : mode})
        except:
            return jsonify({'result' : 'error', 'mode' : mode})

    elif mode == 'limitexit':
        r.set('monitor', 'on')

        response = 'success'
        try:
            ### place limit TP
            session.cancel_all_active_orders(symbol=pair)['ret_msg']

            LMprices = {
                'Buy' : BTCprice + 0.5,
                'Sell' : BTCprice -0.5
            }
            sideRev = {
                'Buy' : 'Sell',
                'Sell' : 'Buy'
            }

            placeOrder(sideRev[positionSide], LMprices[positionSide], 0, positionSize*limitexit, 0)

        except Exception as e:
            print('LIMIT ERROR', e)
            response = 'limit error'
        else:
            print('LIMIT SUCCESS')

        return jsonify({'result' : response, 'mode' : mode})

    elif mode == 'fullexit':

        ### set stop loss

        BEprices = {
            'Buy' : BTCprice - 10,
            'Sell' : BTCprice + 10
        }
        try:
            responseDict = session.set_trading_stop(symbol=pair, stop_loss=BEprices[positionSide])
            print(responseDict)
        except Exception as e:
            print('STOP LOSS ERROR', e)

        ## set limit out
        r.set('monitor', 'on')
        session.cancel_all_active_orders(symbol=pair)['ret_msg']
        response = 'success'
        try:
            ### place limit TP
            LMprices = {
                'Buy' : BTCprice + 0.5,
                'Sell' : BTCprice - 0.5
            }
            sideRev = {
                'Buy' : 'Sell',
                'Sell' : 'Buy'
            }

            placeOrder(sideRev[positionSide], LMprices[positionSide], 0, positionSize, 0)


        except Exception as e:
            print('LIMIT ERROR', e)
            response = 'full exit error'
        else:
            print('LIMIT SUCCESS')

        return jsonify({'result' : response, 'mode' : mode})

    elif mode == 'limitset':
        r.set('monitor', 'on')
        response = 'limitset'

        LMprices = {
                'Buy' : positionEntry + limitprice,
                'Sell' : BTCprice - limitprice
            }

        sideRev = {
            'Buy' : 'Sell',
            'Sell' : 'Buy'
        }

        if limitprice > 1000:
            LMprices = {
                'Buy' : limitprice,
                'Sell' : limitprice
            }

        try:
            placeOrder(sideRev[positionSide], LMprices[positionSide], 0, positionSize*limitfraction, 0)
        except Exception as e:
            print('LIMIT ERROR', e)
            response = 'limitset error'
        else:
            print('LIMIT SUCCESS')

        return jsonify({'result' : response, 'mode' : mode})


    elif mode == 'vwapget' or mode == 'vwapset':
        ##r.set('monitor', 'on')
        response = 'vwapprice'

        timeblocks = json.loads(r.get('timeblocks_BTC'))
        vwap = timeblocks[-2]['vwap_task']

        VSprices = {
                'Buy' : round(float(vwap)) - vwapbuffer,
                'Sell' : round(float(vwap)) + vwapbuffer
            }
        sideRev = {
            'Buy' : 'Sell',
            'Sell' : 'Buy'
        }

        if mode == 'vwapset':
            r.set('monitor', 'on')
            try:
                session.cancel_all_active_orders(symbol=pair)['ret_msg']
                response = placeOrder(sideRev[positionSide], VSprices[positionSide], 0, positionSize*vwapfraction, 0)
            except Exception as e:
                print('VWAP ERROR', e)
                response = 'vwapset error'
            else:
                print('VWAP SUCCESS')
        else:
            response = vwap

        return jsonify({'result' : response, 'mode' : mode})





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

    if int(pw) != int(START_CODE):
        print('FAIL')
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

    r.set('monitor', 'off')

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
