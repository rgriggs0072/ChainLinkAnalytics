import streamlit as st
import pandas as pd
import numpy as np
from io import DEFAULT_BUFFER_SIZE, BytesIO
from PIL import Image
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import openpyxl
# from streamlit_extras.app_logo import add_logo #can be removed
import datetime
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows


#====================================================================================================================================================
# Function below will transform the Pivot table Spreadsheet into virtical columns and add the columns needed to import into Snowflake
#====================================================================================================================================================


def format_pivot_table(workbook):
      # Assuming the sheet name is 'Sheet1', you can modify it as per your actual sheet name
    sheet = workbook['Sheet1']

    # Read the data from the sheet into a DataFrame
    data = sheet.values
    columns = next(data)  # Get the column names from the first row
    df = pd.DataFrame(data, columns=columns)

    # Get the store IDs from the column names
    store_ids = [x for x in df.columns[5:]]

    # Melt the data so that store IDs become a separate column
    df_melted = pd.melt(
        df,
        id_vars=df.columns[:5],
        value_vars=store_ids,
        var_name="store_id",
        value_name="Yes/No",
    )

    #st.write(df_melted.columns)

    # Replace 1 with a green checkmark and NaN with a red X
    df_melted['Yes/No'] = df_melted['Yes/No'].apply(lambda x: 'Yes' if x == 1 else ('No' if pd.isna(x) else '*'))
    #df_melted['Yes/No'] = df_melted['Yes/No'].apply(lambda x: 'Yes' if str(x).strip() == '1' else ('No' if str(x).strip().lower() == 'no' else 'No'))
    # Replace 1 with 'Yes' and NaN with 'No'
    #df_melted['Yes/No'] = df_melted['Yes/No'].apply(lambda x: 'Yes' if x == '1' else ('No' if pd.isna(x) else 'No'))

    




    # Move store_id column to the second position and rename it as STORE_NUMBER
    df_melted.insert(1, "STORE_NUMBER", df_melted.pop("store_id"))

    # Rename column "STORE NUMBER" to 'STORE NAME'
    df_melted.rename(columns={"STORE NUMBER": "STORE_NAME"}, inplace=True)
 
    # Add a new column "STORE_NAME" with empty values
    df_melted.insert(0, "STORE_NAME", "")

    # Reorder the columns with "STORE_NAME" in position 0, "STORE_NUMBER" in position 1, and "UPC" in position 2
    df_melted = df_melted[["STORE_NAME", "STORE_NUMBER", "UPC"] + [col for col in df_melted.columns if col not in ["STORE_NAME", "STORE_NUMBER", "UPC"]]]

    # Delete columns d and E
    #df_melted = df_melted.drop(columns=["SM Distro", "LKY Dist %", "FM Distro", "TTL Dist %"])

  
    
    # # Define the list of desired columns
    #desired_columns = ["STORE_NAME", "STORE_NUMBER", "UPC", "SKU", "PRODUCT_NAME", "MANUFACTURER", "SEGMENT", "Yes/No", "ACTIVATION_STATUS", "COUNTY", "CHAIN_NAME"]

  
   
   
    #df_melted = df_melted.reindex(columns=desired_columns)

    #st.write(df_melted)
    
    # Rename the columns as per your requirements
    df_melted.rename(columns={
        "Name": "PRODUCT_NAME",
        "Yes/No": "YES_NO",
        "SKU #": "SKU"
    }, inplace=True)

    # Display the updated DataFrame
    #print(df_melted)
    # Reindex the DataFrame with the desired columns
  
    
   

    # Remove ' and , characters from all columns
    df_melted = df_melted.replace({'\'': '', ',': '', '\*': '', 'Yes': '1', 'No': '0'}, regex=True)
  
    # Fill STORE_NAME column with "FOOD MAXX" starting from row 2
    #df_melted.loc[0:, "STORE_NAME"] = "FOOD MAXX"

    # Fill SKU column with 0 starting from row 2
    #df_melted.loc[0:, "SKU"] = 0
    #st.write(df_melted)
    df_melted.loc[0:,"ACTIVATION_STATUS"] =""
    df_melted.loc[0:,"COUNTY"] =""
    # Fill CHAIN_NAME column with "FOOD MAXX" starting from row 2
    df_melted.loc[0:, "CHAIN_NAME"] = ""
    #st.write(df_melted)

    
   

    # Convert DataFrame back to workbook object
    new_workbook = openpyxl.Workbook()
    new_sheet = new_workbook.active
    for row in dataframe_to_rows(df_melted, index=False, header=True):
        new_sheet.append(row)

    return new_workbook


#====================================================================================================================================================
# END Function below will transform the Pivot table Spreadsheet into virtical columns and add the columns needed to import into Snowflake
#====================================================================================================================================================
