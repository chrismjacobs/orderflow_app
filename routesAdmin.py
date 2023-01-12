from models import User
from flask import render_template, url_for, flash, redirect, request, jsonify
from app import app
from flask_login import login_user, current_user, logout_user, login_required



@app.route('/loginAction', methods=['POST'])
def loginAction():
    user = request.form['user']
    passcode = request.form['passcode']

    user = User.query.filter_by(username='Chris').first()
    print(user)
    if user.password == passcode:
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