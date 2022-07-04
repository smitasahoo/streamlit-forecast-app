import streamlit as st
import pandas as pd
import numpy as np
st.title('Update Forecast')
#Get data_load_state

import snowflake.connector
from snowflake.connector import ProgrammingError

connect=snowflake.connector.connect(user='SMITASAHOO',password='Smita@94',account='JY60240.ca-central-1.aws')#,region='ca-central-1',cloud='aws')
                                    #database='SNOWFLAKE_SAMPLE_DATA',schema='INFORMATION_SCHEMA')
#connect=snowflake.connector.connect(user='SMITA.SAHOO@AGROPUR.COM',password='Sm16sa2022',account='bk48470.us-east-2.aws')#,region='us-east-2',cloud='aws')
                                    
cx=connect.cursor()
sql='select * from TEST.TEST_FORECAST.FORECAST'
def fetch_pandas_old(cur, sql):
    cur.execute(sql)
    rows = 0
    while True:
        dat = cur.fetchmany(50000)
        if not dat:
            break
        df = pd.DataFrame(dat,columns=['XXAGR_PUZ_PROD_PROMOTION_GRP','F_PUZ_ATTR_FAMBAN_BANN_GRP','XXAGR_PUZ_REGION_GROUP','WEEK_START_DATE','NET_INVOICED_QTY_MV','Forecast','Update_Forcast'])
        rows += df.shape[0]
    return df
data = fetch_pandas_old(cx,sql)
data_load_state = st.text('Loading data...')
data = fetch_pandas_old(cx,sql)
data = data.astype({'NET_INVOICED_QTY_MV':'float','Forecast':'float','Update_Forcast':'float'})
data_load_state.text('Loading data...done!')
if st.checkbox('Show raw data'):
    st.subheader('Raw data')
    st.write(data)
product = st.selectbox(
    'Select Product',
     data['XXAGR_PUZ_PROD_PROMOTION_GRP'].unique())
banner = st.selectbox(
    'Select Banner',
     data['F_PUZ_ATTR_FAMBAN_BANN_GRP'].unique())
region = st.selectbox(
    'Select Region',
     data['XXAGR_PUZ_REGION_GROUP'].unique())
date = st.date_input(
     "Select week start date ")
st.write('Selected Date:', date)
update_forecast = st.number_input('Insert a updated forecast')
st.write('The updated forecast is ', update_forecast)
#update query
update_query='update TEST.TEST_FORECAST.FORECAST set update_forecast='+str(update_forecast)+' where XXAGR_PUZ_PROD_PROMOTION_GRP=\''+product +'\' and F_PUZ_ATTR_FAMBAN_BANN_GRP=\''+banner+'\'and XXAGR_PUZ_REGION_GROUP=\''+region+'\' and to_date(WEEK_START_DATE)=\''+str(date)+'\''
def update_db(cx,update_query):
  try:
    cx.execute(update_query)
  except ProgrammingError as e:
    if e.errno == 604:
      print("timeout")
      cx.execute("rollback")
    else:
      raise e
  else:
   cx.execute("commit")
  finally:
    cx.close()   
load = st.button('update db')
if load:
  update_db(cx,update_query)
  st.write('updated')
