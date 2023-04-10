from datetime import datetime, timedelta
# from http.client import UNSUPPORTED_MEDIA_TYPE
# from msilib import AMD64
from app import app, db, login_manager
from flask_login import UserMixin, current_user
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView

# verify token
# from itsdangerous import TimedJSONWebSignatureSerializer as Serializer

#login manager
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username =  db.Column(db.String(20), unique=True, nullable=False)
    date_added = db.Column(db.DateTime, default=datetime.now)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(60), nullable=False)
    extra = db.Column(db.Integer)
    info = db.Column(db.String(), default='')


class MyModelView(ModelView):
    def is_accessible(self):
        if current_user.is_authenticated and current_user.id == 1:
            return True
        else:
            return False


admin = Admin(app)

admin.add_view(MyModelView(User, db.session))










