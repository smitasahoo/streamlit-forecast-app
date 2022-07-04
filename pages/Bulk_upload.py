import streamlit as st
import pandas as pd
import numpy as np
import os
st.title('Update Forecast In Bulk')
#Get data_load_state

import snowflake.connector
from snowflake.connector import ProgrammingError

#@st.experimental_singleton
#def init_connection():
    #return snowflake.connector.connect(**st.secrets["snowflake"])
                                    
#cx=init_connection().cursor()
def snowflakeconnect():
  connect=snowflake.connector.connect(
      user='SMITASAHOO',password='Smita@94',
      account='JY60240.ca-central-1.aws' ,
      database = 'TEST',
      schema = 'TEST_FORECAST',
      warehouse = 'COMPUTE_WH',
      role='ACCOUNTADMIN')
  cx=connect.cursor()
  return cx                  
cx=snowflakeconnect()

def upsert_to_snowflake(cur,df,id_columns,insert_columns,update_columns,table):
  if df.empty: 
    print(f'No rows to bulk upsert to {table}. Aborting.')
    return

  print(f"BULK UPSERTING {df.shape[0]} {table.upper()} TO SNOWFLAKE")
  filename = f"{table}.json"
  df.to_json(filename,orient='records',lines=True,date_unit='s')
  filepath = os.path.abspath(filename)
  create_stage="CREATE"+" OR "+" REPLACE STAGE my_stage_name FILE_FORMAT=(TYPE='JSON')"
  stage='my_stage_name'
  cur.execute(create_stage)
  cur.execute(f"put file://{filepath} @{stage} overwrite=true;")
  print(f"""merge into {table}
						using (select {','.join([f'$1:{col} as {col}' for col in insert_columns])}
							from @{stage}/{filename}) t
						on ({' and '.join([f't.{col} = {table}.{col}' for col in id_columns])+' and'+'and '.join([f' to_date(t.{col} )= to_date({table}.{col})' for col in date_columns])})
						when matched then
							update set {','.join([f'{col}=t.{col}' for col in update_columns])}
						when not matched then insert ({','.join(insert_columns)})
						values ({','.join([f't.{col}' for col in insert_columns])});""")
  cur.execute(f"""merge into {table}
						using (select {','.join([f'$1:{col} as {col}' for col in insert_columns])}
							from @{stage}/{filename}) t
						on ({' and '.join([f't.{col} = {table}.{col}' for col in id_columns])+' and'+'and '.join([f' to_date(t.{col} )= to_date({table}.{col})' for col in date_columns])})
						when matched then
							update set {','.join([f'{table}.{col}=t.{col}' for col in update_columns])}
						when not matched then insert ({','.join(insert_columns)})
						values ({','.join([f't.{col}' for col in insert_columns])});""")
		# delete json file from the table stage
  
  cur.execute(f"remove @{stage}/{filename};")
		# delete the json file created
  os.remove(filename)
  print('\t...done')
uploaded_file = st.file_uploader("Choose a file")
if uploaded_file is not None:
  bulk_data = pd.read_csv(uploaded_file)
  st.write(bulk_data)
  insert_columns=bulk_data.columns
  id_columns=['XXAGR_PUZ_PROD_PROMOTION_GRP', 'F_PUZ_ATTR_FAMBAN_BANN_GRP','XXAGR_PUZ_REGION_GROUP']
  date_columns=['WEEK_START_DATE']
  update_columns=['UPDATE_FORECAST']
  load = st.button('Click to Bulk Update DB')
  if load:
    upsert_to_snowflake(cx,bulk_data,id_columns,insert_columns,update_columns,'FORECAST')
    st.write('Bulk updated')

