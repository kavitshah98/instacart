from flask import Flask,render_template, request,Response
import sqlalchemy
import pymysql
from pandas import DataFrame
from IPython.display import HTML
from time import time
import numpy as np
import psycopg2
from flask import send_file
import pdfkit


app = Flask(__name__)

result = ""
csv_file = None
pdf_file = None
html_out = ""

@app.route("/getPlotCSV")
def getPlotCSV():
    csv = '1,2,3\n4,5,6\n'
    return Response(
        csv_file,
        mimetype="text/csv",
        headers={"Content-disposition":
                 "attachment; filename=output.csv"})

@app.route("/getPdf")
def getPdf():
    if html_out!="":
        pdfkit.from_url('http://ec2-18-232-130-25.compute-1.amazonaws.com:8080/', pdf_file)
    return Response(
    pdf_file,
    mimetype="text/pdf",
    headers={"Content-disposition":
             "attachment; filename=output.pdf"}
    )

@app.route('/',methods=['GET','POST'])
def index():

    global result,csv_file,html_out
    csv_file = None
    html_out = ""
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
                    if cur.description is not None and r==0:
                        columns = cur.description
                        key_list = []
                        for i in range(len(columns)):
                            key_list.append(columns[i][0])
                        df=DataFrame(columns=key_list)
                        csv_file = df.to_csv()
                        html_out = df.to_html()
                        c=1
                    elif cur.description is None and r==0:
                        c=2
                    elif cur.description is None and r!=0:
                        c=3
                    elif cur.description is not None and r!=0:
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
                        csv_file = df.to_csv()
                        html_out = df.to_html()
                        c=0

                    db.commit()
                    cur.close()
                    db.close()
                    toc = time()
                    if c==1:
                        return render_template('index.html',column_names=df.columns.values,row_data=[],exec_time = ("Time Elapsed(in sec): "+str(round(toc-tic,7))),zip=zip)
                    if c==3:
                        return render_template('index.html',column_names=[],row_data=[],exec_time = ("Time Elapsed(in sec): "+str(round(toc-tic,7))),zip=zip)+"Query Executed: "+str(r)+" rows affected"
                    if c==2:
                        return render_template('index.html',column_names=[],row_data=[],exec_time = ("Time Elapsed(in sec): "+str(round(toc-tic,7))),zip=zip)+"Query Executed: "
                    if c==0:
                        return render_template('index.html',column_names=df.columns.values,row_data=list(df.values.tolist()),exec_time = ("Time Elapsed(in sec): "+str(round(toc-tic,7))),zip=zip)

                elif option=="redshift":
                    db = psycopg2.connect(dbname= 'instacartredshift',
                                            host='redshift-cluster-1.cfxnwqcezz7k.us-east-1.redshift.amazonaws.com',
                                            port= '5439',
                                            user= 'root',
                                            password= 'Abcd1234')
                    cur = db.cursor()
                    tic = time()
                    r = cur.execute(query_con)
                    if cur.description is None:
                        c=2
                    elif cur.description is not None and r!=0:
                        print("chelho")
                        columns = cur.description
                        data_row = cur.fetchall()
                        key_list = []
                        for i in range(len(columns)):
                            key_list.append(columns[i][0])
                        df=DataFrame(data_row)
                        if data_row==[]:
                            df=DataFrame(columns=key_list)
                        else:
                            df.index = np.arange(1, len(df)+1)
                            df.columns=(key_list)
                        csv_file=df.to_csv()
                        c=0

                    db.commit()
                    cur.close()
                    db.close()
                    toc = time()

                    if c==2:
                        return render_template('index.html',column_names=[],row_data=[],exec_time = ("Time Elapsed(in sec): "+str(round(toc-tic,7))),zip=zip)+"Query Executed: "
                    if c==0:
                        return render_template('index.html',column_names=df.columns.values,row_data=list(df.values.tolist()),exec_time = ("Time Elapsed(in sec): "+str(round(toc-tic,7))),zip=zip)

            except Exception as e:
                result = '<h3>'+str(e)+'</h3>'
                return render_template('index.html',column_names=[],row_Data=list(),exec_time="")+result
    else:
        return render_template('index.html',column_names=[],row_data=list(),exec_time="")




if __name__ == '__main__':
    app.run(host="0.0.0.0",port=8080,debug=True)
