import pandas as pd
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import streamlit as st
import numpy as np

# Fill in your SFlake details here
engine = create_engine(URL(
    account = 'JY60240.ca-central-1.aws',
    user = 'SMITASAHOO',
    password = 'Smita@94',
    database = 'TEST',
    schema = 'TEST_FORECAST',
    warehouse = 'COMPUTE_WH',
    role='ACCOUNTADMIN',
))
 
connection = engine.connect()
uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None:
  df = pd.read_csv(uploaded_file)
  st.write(df)
is_insert = st.button('Insert Data')
if is_insert:
  df.to_sql('FORECAST', con=engine,if_exists='append', index=False) 
  st.write('Data Inserted')

#make sure index is False, Snowflake doesnt accept indexes
 
connection.close()
engine.dispose()
