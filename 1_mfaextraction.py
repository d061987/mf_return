from loguru import logger
from configparser import ConfigParser
from datetime import datetime, timedelta, date
import pandas as pd
import mysql.connector as mysql
import pymysql
import csv
import requests
from bs4 import BeautifulSoup as bs
import warnings 
warnings.filterwarnings('ignore')
logger.add('mf_fund3_extract.log', rotation='10 MB')
import configparser
from cryptography.fernet import Fernet
import time

config = configparser.ConfigParser()
config.read('config.ini')


run_id = datetime.now().strftime("%Y%m%d%H%M%S")

key = config['mysqldb']['key']
cipher_suite = Fernet(key.encode('utf-8'))
# ciphered_text = cipher_suite.encrypt(config['mysqldb']['db_password'])   

unciphered_pwd = (cipher_suite.decrypt((config['mysqldb']['db_password']).encode('utf-8'))).decode()

# connection = pymysql.connect(host='localhost',
#                                         user='root',
#                                         password='Bappa@1234567',
#                                         db='STOCK2023')
myhost = config['mysqldb']['host']
print(myhost)
connection = mysql.connect(host = 'localhost', port = config['mysqldb']['port'], \
    user = config['mysqldb']['user'], password=unciphered_pwd, database= 'STOCK2023')


audit_query = "Insert into mf_audit (Run_ID	,MF_House, Start_Date, End_Date, Elapsed_Time, Record_Count) values (%s,%s,%s,%s,%s,%s)"

#Extracting The Mutual Fund House ID and name
URL = 'https://www.amfiindia.com/nav-history-download'

Source = requests.get(URL)
soup = bs(Source.text, 'html.parser')

NavDownMFName = []
mf_code = []
for table in soup.find_all('select',{'class': 'select', 'id': 'NavDownMFName'})[0]:
    str_table = str(table)[15:].split('">')
    if len(str_table)>1:
        mf_Id = str_table[0]
        mf_Name = str_table[1].split('<')[0]
        NavDownMFName.append(mf_Id + ' ' +mf_Name)
        mf_code.append(mf_Id)
mf_code = mf_code[2:]


#Defining The Data Extraction Window
f_date = (date.today()-timedelta(days=2200)).strftime('%Y-%m-%d')
t_date = date.today().strftime('%Y-%m-%d')




try:

    for org_cd in mf_code:
        start = time.time()
        try:
            logger.info(f"extracting data for {org_cd}")
            mf_type = '1'


            Nav_url = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf=' + org_cd \
                 + '&tp=' + mf_type + '&frmdt=' + f_date + '&todt=' + t_date
            
            try:
                print(Nav_url)
                stockdata = pd.read_csv(Nav_url)
            except Exception as e:
                print(f"No Data")
            else:
                mf_df = stockdata.iloc[2:,:]

                mf_df[['Scheme_Code','Scheme_Name','ISIN_Div_Payout_ISIN_Growth','ISIN_Div_Reinvestment','Net_Asset_Value','Repurchase_Price',
                'Sale_Price','Date']] = mf_df['Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date'].str.split(';', expand=True)
                
                mf_df1 = mf_df.iloc[:,1:]


                
                # create cursor
                data_cursor=connection.cursor()

                cols = ",".join([str(i) for i in mf_df1.columns.tolist()])
                logger.info(f" Writing Data For {org_cd}")
                for i,row in mf_df1.iterrows():
                    sql = "INSERT INTO mf_fund3 (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
                    data_cursor.execute(sql, tuple(row))
                connection.commit()

                # logger.info(f" Write Successful.{mf_df1.shape}")
                # final_cursor = connection.cursor()
                # insert_query = "INSERT INTO mf_audit ('Successful' )"
                # final_cursor.execute(insert_query)
                # connection.commit()

        except Exception as e:
            print(e)
        finally:
            end = time.time()
            elapsed = end - start
            audit_cursor=connection.cursor()
            values = (run_id,org_cd,f_date,t_date,elapsed,len(mf_df1))
            audit_cursor.execute(audit_query, values)
            connection.commit()
except Exception as e:
    print(e)