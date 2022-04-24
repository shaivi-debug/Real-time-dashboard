from app import app
from flaskext.mysql import MySQL
mysql = MySQL()
app.config['MYSQL_DATABASE_USER'] = 'ubuntu'
app.config['MYSQL_DATABASE_PASSWORD'] = 'ubuntu'
app.config['MYSQL_DATABASE_DB'] = 'transaction'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'
mysql.init_app(app)
