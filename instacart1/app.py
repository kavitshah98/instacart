from flask import Flask,render_template, request
import sqlalchemy
import pymysql
from pandas import DataFrame
from IPython.display import HTML
from time import time
import numpy as np
import psycopg2

app = Flask(__name__)



'''rsdb = psycopg2.connect(dbname= 'instacartredshift',
                        host='redshift-cluster-1.cfxnwqcezz7k.us-east-1.redshift.amazonaws.com',
                        port= '5439',
                        user= 'root',
                        password= 'Abcd1234')'''

#currs = rsdb.cursor()
result = ""

@app.route('/',methods=['GET','POST'])
def index():
    global result
    result=""
    if request.method == 'POST':
            try:
                query_con = request.form['query']
                option = request.form['database']

                if option=="mysql":
                    db = pymysql.connect(host="database-2.cuxnpjy7qt1g.us-east-1.rds.amazonaws.com",
                                         user="admin",
                                         passwd="abcd1234",
                                         db="instacart_db")
                    cur = db.cursor()
                    tic = time()
                    r = cur.execute(query_con)
                    if cur.description==() and r==0:
                        html_out='<h3 style="color:white">Query Executed</h3>'
                    else:
                        columns = cur.description
                        key_list = []
                        data_row = cur.fetchall()
                        for i in range(len(columns)):
                            key_list.append(columns[i][0])
                        df=DataFrame(data_row)
                        if r==0:
                            df=DataFrame(columns=key_list)
                        else:
                            df.index = np.arange(1, len(df)+1)
                            df.columns=(key_list)
                        html_out = '<div style="background-color:white;display:inline-block;margin-left: auto;margin-right: auto;">'+df.to_html(classes='table table-striped table-hover')+'</div>'
                    cur.close()
                    db.close()
                    toc = time()
                    result = '<h3 style="color:white;">'+"Time elapsed(in seconds):"+str(round((toc-tic),4))+'</h3>'+'<br>'+html_out

                elif option=="redshift":
                    rsdb = psycopg2.connect(dbname= 'instacartredshift',
                                            host='redshift-cluster-1.cfxnwqcezz7k.us-east-1.redshift.amazonaws.com',
                                            port= '5439',
                                            user= 'root',
                                            password= 'Abcd1234')
                    currs = rsdb.cursor()
                    print(option)
                    tic = time()
                    currs.execute(query_con)
                    if "select" not in query_con and "show" not in query_con and "describe" not in query_con:
                        rsdb.commit()
                        html_out='<h3 style="color:white">Query Executed</h3>'
                    else:
                        columns = currs.description
                        key_list = []
                        data_row = currs.fetchall()
                        for i in range(len(columns)):
                            key_list.append(columns[i][0])
                        df=DataFrame(data_row)
                        df.index = np.arange(1, len(df)+1)
                        df.columns=(key_list)
                        html_out = '<div style="background-color:white;display:inline-block;margin-left: auto;margin-right: auto;">'+df.to_html(classes='table table-striped table-hover')+'</div>'
                    currs.close()
                    rsdb.close()
                    toc = time()
                    result = '<h3 style="color:white;">'+"Time elapsed(in seconds):"+str(round((toc-tic),4))+'</h3>'+'<br>'+html_out
            except Exception as e:
                result = '<h3 style="color:white;">'+str(e)+'</h3>'



    return render_template('index.html')+result

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=8080)
