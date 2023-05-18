#Importing Libraries
import mysql.connector as mysql
import pandas as pd
from datetime import datetime, timedelta,date
import warnings 
warnings.simplefilter(action='ignore')
import openpyxl
import configparser
from cryptography.fernet import Fernet
from loguru import logger
logger.add('all_fund_ID.log', rotation='10 MB')


config = configparser.ConfigParser()
config.read('config.ini')

# pd.set_option('display.max_columns', None)

key = config['mysqldb']['key']
cipher_suite = Fernet(key.encode('utf-8'))
# ciphered_text = cipher_suite.encrypt(config['mysqldb']['db_password'])   

unciphered_pwd = (cipher_suite.decrypt((config['mysqldb']['db_password']).encode('utf-8'))).decode()

myhost = config['mysqldb']['host']
print(myhost)
db = mysql.connect(host = 'localhost', port = config['mysqldb']['port'], \
    user = config['mysqldb']['user'], password=unciphered_pwd, database= 'STOCK2023')
cursor = db.cursor()
query = "select * from mf_fund3"

cursor.execute(query)
dbs = cursor.fetchall()
print(f"Data base connection successful")

mf_db = pd.read_sql(query, con = db)


mf = mf_db.loc[:,['Scheme_Code','Scheme_Name','Net_Asset_Value','Date']]
mf.dropna(axis=0 , inplace=True)

mf_direct= mf[mf['Scheme_Name'].str.contains('Direct')]
mf_growth = mf_direct[mf_direct['Scheme_Name'].str.contains('Growth')]
mf_growth['Net_Asset_Value'] = pd.to_numeric(mf_growth['Net_Asset_Value'], errors='coerce').fillna(0, downcast='infer')
mf_growth['Date'] = pd.to_datetime(mf_growth['Date'])


all_fund_ID = mf_growth.loc[(mf_growth['Date']==(pd.Timestamp(max(mf_growth['Date']))))].Scheme_Code.unique()
logger.info(f"extracting data for {all_fund_ID}")

return_df = pd.DataFrame(columns=[
    'Scheme_Code', 'Scheme_Name', 'Net_Asset_Value', 'Date', '3M_OLD_Price',
    '3M_per_chng', '6M_OLD_Price', '6M_per_chng', '1Y_OLD_Price',
    '1Y_per_chng', '3Y_OLD_Price', '3Y_per_chng', '5Y_OLD_price',
    '5Y_per_chng'
])

count = 0

for fund in all_fund_ID:
    count = count+1
    all_liquid_mf = mf_growth[mf_growth['Scheme_Code'].str.contains(fund)]
    print(f'fatching data for {count}: {all_liquid_mf.Scheme_Name.unique()}')
    logger.info(f"extracting data for {all_liquid_mf}")

    start_date = pd.Timestamp(max(all_liquid_mf['Date']))
    end_date = start_date + pd.Timedelta(days=-1825)
    # print(start_date,end_date)
    all_mf_5Y = all_liquid_mf.loc[(all_liquid_mf['Date'] <= start_date) & (all_liquid_mf['Date'] >= end_date), :]
    # print(max(union_liquid_mf['Date']),min(union_liquid_mf['Date']))

    all_mf_5Y.sort_values(['Date'],ascending=[0],inplace=True)

    all_mf_5Y['3M_OLD_Price'] = all_mf_5Y['Net_Asset_Value'].shift(-63)
    all_mf_5Y['3M_per_chng']=((all_mf_5Y['Net_Asset_Value']/all_mf_5Y['3M_OLD_Price'])**(12/3)-1)*100
    all_mf_5Y['6M_OLD_Price'] = all_mf_5Y['Net_Asset_Value'].shift(-126)
    all_mf_5Y['6M_per_chng']=((all_mf_5Y['Net_Asset_Value']/all_mf_5Y['6M_OLD_Price'])**(12/6)-1)*100
    all_mf_5Y['1Y_OLD_Price'] = all_mf_5Y['Net_Asset_Value'].shift(-252)
    all_mf_5Y['1Y_per_chng']=((all_mf_5Y['Net_Asset_Value']/all_mf_5Y['1Y_OLD_Price'])**(12/12)-1)*100
    all_mf_5Y['3Y_OLD_Price'] = all_mf_5Y['Net_Asset_Value'].shift(-756)
    all_mf_5Y['3Y_per_chng'] = ((all_mf_5Y['Net_Asset_Value']/all_mf_5Y['3Y_OLD_Price'])**(12/36)-1)*100
    all_mf_5Y['5Y_OLD_price'] = all_mf_5Y['Net_Asset_Value'].shift(-1260)
    all_mf_5Y['5Y_per_chng'] = ((all_mf_5Y['Net_Asset_Value']/all_mf_5Y['5Y_OLD_price'])**(12/60)-1)*100

    all_df = all_mf_5Y.iloc[:1]
    logger.info(f"extracting data for {all_df}")
    # print(union_df)
    # break
    # return_df = return_df.append(union_df,ignore_index=True)
    return_df = pd.concat([return_df,all_df])


# print(return_df.head())
return_df.to_excel('mf_final_return_file.xlsx')
