import pymysql
from app import app
from config import mysql
from flask import jsonify
from flask import flash, request
import mysql.connector
import time
@app.route('/get')
def details():
   try:
     conn = mysql.connector.connect(host="localhost",user="ubuntu",password="ubuntu",database="transaction")
     print("connection ok")
     cursor = conn.cursor(dictionary=True)
     cursor.execute("SELECT * FROM TRANSACTION_DETAIL")
     empRow = cursor.fetchone()
     while empRow is not None:
        print(empRow)
        time.sleep(2)
        empRow=cursor.fetchone()
     
   except Exception as e:
     print(e)


@app.errorhandler(404)
def showMessage(error=None): 
     message = { 'status': 404, 'message': 'Record not found: ' + request.url,
     }
     respone = jsonify(message) 
     respone.status_code = 404 
     return respone

if __name__ == "__main__":
    app.run()
