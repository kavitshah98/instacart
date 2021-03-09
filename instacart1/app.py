from flask import Flask,render_template, request
import sqlalchemy
import pymysql
from pandas import DataFrame
from IPython.display import HTML
from time import time
import numpy as np
import psycopg2

app = Flask(__name__)

db = pymysql.connect(host="database-2.cuxnpjy7qt1g.us-east-1.rds.amazonaws.com",
                     user="admin",
                     passwd="abcd1234",
                     db="instacart_db")

rsdb = psycopg2.connect(dbname= 'instacartredshift',
                        host='redshift-cluster-1.cfxnwqcezz7k.us-east-1.redshift.amazonaws.com',
                        port= '5439',
                        user= 'root',
                        password= 'Abcd1234')
cur = db.cursor()
currs = rsdb.cursor()
result = ""
@app.route('/',methods=['GET','POST'])
def index():
    global result
    if request.method == 'POST':
        query_con = request.form['query']
        option = request.form['database']
        if option=="mysql":
            print(option)
            tic = time()
            cur.execute(query_con)
            columns = cur.description
            key_list = []
            data_row = cur.fetchall()
            for i in range(len(columns)):
                key_list.append(columns[i][0])
            df=DataFrame(data_row)
            df.index = np.arange(1, len(df)+1)
            df.columns=(key_list)
            toc = time()
            html_out = '<div style="background-color:white;display:inline-block;margin-left: auto;margin-right: auto;">'+df.to_html()+'</div>'
            result = '<h3 style="color:white;">'+"Time elapsed(in seconds):"+str(round((toc-tic),4))+'</h3>'+'<br>'+html_out

        elif option=="redshift":
            print(option)
            tic = time()
            currs.execute(query_con)
            columns = currs.description
            key_list = []
            data_row = currs.fetchall()
            for i in range(len(columns)):
                key_list.append(columns[i][0])
            df=DataFrame(data_row)
            df.index = np.arange(1, len(df)+1)
            df.columns=(key_list)
            toc = time()
            html_out = '<div style="background-color:white;display:inline-block;margin-left: auto;margin-right: auto;">'+df.to_html()+'</div>'
            result = '<h3 style="color:white;">'+"Time elapsed(in seconds):"+str(round((toc-tic),4))+'</h3>'+'<br>'+html_out

        else:
            result = "Please select a database name"

    return render_template('index.html')+result

if __name__ == '__main__':
    app.run(debug=False)
