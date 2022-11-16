# NOTE: This app uses enterprise features of AgGrid (https://www.ag-grid.com/) so it's solely your responsibility to aquire the license and/or 
# get permission as the case maybe directly from the creator before using it in produciton or for commercial use.

#!/usr/bin/env python

import streamlit as st
import os
import snowflake.connector
import pandas as pd
from datetime import datetime
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

APP_ICON_URL = "assets/snowpark-icon.png"

# Set these environment variables to connect to Snowflake
ACT = os.getenv('SNOWSQL_ACT') # Account
USR = os.getenv('SNOWSQL_USR') # User
PWD = os.getenv('SNOWSQL_PWD') # Password
ROL = os.getenv('SNOWSQL_ROL') # Role
DBT = os.getenv('SNOWSQL_DBT') # Database
WRH = os.getenv('SNOWSQL_WRH') # Warehouse
SCH = os.getenv('SNOWSQL_SCH') # Schema

# Setup web page
st.set_page_config(
     page_title="Snowflake Snowpark Explorer",
     page_icon=APP_ICON_URL,
     layout="wide",
     menu_items={
         'Get Help': 'https://developers.snowflake.com',
         'About': "This app is powered by Snowflake and Streamlit | Developed by dash[dot]desai[at]snowflake[dot]com"
     }
)
st.image(APP_ICON_URL, width=80)
st.markdown("<h1 style='margin:-80px 0px 0px 100px;'>Snowflake Snowpark Explorer</h1>", unsafe_allow_html=True)
st.subheader("All things you need to know about your Snowpark Python User-Defined Functions (UDFs, UDTFs) and Stored Procedures (SPs)")
st.markdown("<h5>NOTE: To see the UDF, UDTF or SP code, expand the row by clicking on > to the left of the UDF or SP name. For questions or feedback, reach out to <a href='https://twitter.com/iamontheinet' target='_blank'>https://twitter.com/iamontheinet</a>.</h5>", unsafe_allow_html=True)

# Setup custom Master-Detail grid for displaying UDF and SP details
gridOptions = {
    "rowSelection": 'single',
    # enable Master / Detail
    "masterDetail": True,
    # the first Column is configured to use agGroupCellRenderer
    "columnDefs": [
      {
        "field": "Name",
        "cellRenderer": "agGroupCellRenderer",
        "checkboxSelection": False,
      },
      {"field": "Signature"},
      {"field": "Imports"},
      {"field": "Packages"},
      {"field": "Builtin"},
      {"field": "Aggregate"},
      {"field": "Table Function"},
      {"field": "Clustering"},
      {"field": "Secure"},
      {"field": "Handler"},
      {"field": "Date Created"},
    ],
    "defaultColDef": {
      "sortable": True
    },
    "onRowSelected": JsCode(
      """function (params) {
          console.log(params.data.Body);
      }"""
    ).js_code,
    "detailCellRenderer": JsCode(
      """function (params) {
          console.log(params.data.Body);
          return "<textarea style='color:#000000;background-color:#f8f8f8;margin-top:10px;font-size:12px;width:100%;height:100%;box-sizing:border-box;border:2px solid #ccc;border-radius: 4px;'>"+params.data.Body+"</textarea>";
      }"""
    ).js_code,     
    # provide Detail Cell Renderer Params
    "detailCellRendererParams": {
        # provide the Grid Options to use on the Detail Grid
        "detailGridOptions": {
          "columnDefs": [
            {"field": "Body"}
          ],
          "defaultColDef": {
            "flex": 1,
          },
          "getRowHeight": JsCode(
            """function (params) {
              return 200;
            }"""
          ).js_code,       
        },
        # get the rows for each Detail Grid
        "getDetailRowData": JsCode(
          """function (params) {
            params.successCallback(
            [{
              "Body": params.data.Body
            }]);
          }"""
        ).js_code,
    },
}

#Create context to connect to Snowflake
def create_context():
    if "snowflake_context" not in st.session_state:
        ctx = snowflake.connector.connect(
          user=USR,
          password=PWD,
          account=ACT,
          role=ROL,
          warehouse=WRH,
          database=DBT,
          schema=SCH
        )
        st.session_state['snowflake_context'] = ctx
    else:
        ctx = st.session_state['snowflake_context']

    return ctx

#Load UDF or SP object details
def load_data(cur, obj_type):
  if obj_type == 'sprocs':
    show_sql = "SHOW USER PROCEDURES"
    desc_base_sql = "DESCRIBE PROCEDURE"
    obj_description = "user-defined procedure"
  else:
    show_sql = "SHOW USER FUNCTIONS"
    desc_base_sql = "DESCRIBE FUNCTION"
    obj_description = "user-defined function"

  cols = ('Name','Signature','Imports','Packages', 'Builtin', 'Aggregate', 'Table Function', 'Clustering', 'Secure', 'Handler', 'Date Created','Body')
  data = []

  results = cur.execute(show_sql).fetchall()

  #Loop through UDF or SP object attributes
  for rec in results:
    created_at = rec[0]
    name = rec[1]
    schema = rec[2]
    is_builtin = rec[3]
    is_aggregate = rec[4]
    signature = rec[8]
    description = rec[9]
    db = rec[10]
    is_table_function = rec[11]
    is_clustering = rec[12]
    is_secure = rec[13]

    #Make sure the UDF or SP object belongs to the current Database and Schema. (Otherwise, it will result in unauthorized access error.)
    if (description == obj_description and db == DBT and schema == SCH):
      #Extract Name, Arguments and Return value
      name_and_params = signature[:signature.index("RETURN")-1] if signature.find("RETURN") != -1 else signature
      return_value = signature[signature.index("RETURN")+7:] if signature.find("RETURN") != -1 else "N/A"

      #Describe UDF or SP object to get the details 
      desc_sql = f"{desc_base_sql} {name_and_params}"
      props = cur.execute(desc_sql).fetchall()
      # print(props)

      #Extract specific UDF or SP object info from tuples
      if obj_type == 'sprocs':
        body = props[6][1]
        imports = props[7][1]
        imports = imports.split('/')[-1][:-1] if imports != '[]' else 'N/A'
        handler = props[8][1]
        packages = props[10][1]
        packages = packages.strip('][') if packages is not None else 'N/A'
      else:
        body = props[5][1]
        imports = props[6][1]
        imports = imports.split('/')[-1][:-1] if imports != '[]' else 'N/A'
        handler = props[7][1]
        packages = props[9][1]
        packages = packages.strip('][') if packages is not None else 'N/A'

      #Append UDF or SP object details to the list
      data.append([name, signature, imports, packages, is_builtin, is_aggregate, is_table_function, is_clustering, is_secure, handler, created_at.strftime('%b %d %Y'), body])

  #Return Pandas Dataframe with the list of UDF or SP details
  return pd.DataFrame(data,columns=cols)

if __name__ == "__main__":
  ctx = create_context()
  cur = ctx.cursor()

  df_udfs = load_data(cur, 'udfs')
  df_sprocs = load_data(cur, 'sprocs')

  tab1, tab2 = st.tabs(["User-Defined Functions and User-Defined Table Functions", "Stored Procedures"])

  with tab1:
    AgGrid(df_udfs,gridOptions=gridOptions,height=600,allow_unsafe_jscode=True,enable_enterprise_modules=True)

  with tab2:
    AgGrid(df_sprocs,gridOptions=gridOptions,height=600,allow_unsafe_jscode=True,enable_enterprise_modules=True)
