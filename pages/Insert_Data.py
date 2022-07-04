import pandas as pd
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import streamlit as st
import numpy as np

@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])
                                    
cx=init_connection()
uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None:
  df = pd.read_csv(uploaded_file)
  st.write(df)
is_insert = st.button('Insert Data')
if is_insert:
  df.to_sql('FORECAST', con=cx,if_exists='append', index=False) 
  st.write('Data Inserted')

#make sure index is False, Snowflake doesnt accept indexes
 
