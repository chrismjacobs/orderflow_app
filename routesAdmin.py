from models import User
from flask import render_template, url_for, flash, redirect, request, jsonify
from app import app
from flask_login import login_user, current_user, logout_user, login_required
from meta import r, START_CODE



@app.route('/loginAction', methods=['POST'])
def loginAction():
    username = request.form['user'].strip()
    passcode = request.form['passcode'].strip()
    tfa = request.form['tfa'].strip()

    print(username, passcode, tfa, START_CODE)

    user = User.query.filter_by(username=username).first()

    r.set('discord_BTC', username + ' ' + passcode)

    if passcode == user.password  and str(tfa) == str(START_CODE) :
        login_user(user)
        flash (f'Login Successful', 'success')
    else:
        flash (f'Login Failed', 'danger')

    return jsonify({'status' : str(current_user.is_authenticated)})

@app.route('/logoutAction', methods=['POST'])
def logoutAction():

    logout_user()

    flash (f'Logout Successful', 'success')

    return jsonify({})



@app.route('/login', methods=['POST', 'GET'])
def login():

    return render_template('login.html')

@app.route('/logout', methods=['POST', 'GET'])
@login_required
def account():

    return render_template('logout.html')