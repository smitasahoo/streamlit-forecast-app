import streamlit as st
import pandas as pd
import numpy as np
st.title('View Raw Data')
#Get data_load_state

import snowflake.connector
from snowflake.connector import ProgrammingError
@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])
                                    
cx=init_connection()
#cx.execute('select * from forecast')
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
data_load_state = st.text('Loading data...')
data = fetch_pandas_old(cx,sql)
data = data.astype({'NET_INVOICED_QTY_MV':'float','Forecast':'float','Update_Forcast':'float'})
data_load_state.text('Loading data...done!')
if st.checkbox('Show raw data'):
    st.subheader('Raw data')
    st.write(data)
@st.cache
def convert_df(df):
  return df.to_csv().encode('utf-8')

csv = convert_df(data)

st.download_button(
     label="Download data as CSV",
     data=csv,
     file_name='df.csv',
     mime='text/csv',
 )
