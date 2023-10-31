from ast import Store
from ctypes import wstring_at
import streamlit as st
import pandas as pd
import openpyxl
import tempfile
from openpyxl.utils.dataframe import dataframe_to_rows



def format_SMART_FINAL_DistroGrid(workbook):
   # Get the desired sheet name
    sheet_name = 'SMART_FINAL_Distro_Grid'  # Replace with your actual sheet name

    st.write('You made it to smart and final big daddy')
    st.write('Function format_SMART_FINAL_DistroGrid is running')

    # Access the sheet in the workbook
    sheet = workbook[sheet_name]

    # Remove filters
    sheet.auto_filter.ref = None

    # Read the data from the sheet into a DataFrame
    data = sheet.values
    columns = next(data)  # Get the column names from the first row
    df = pd.DataFrame(data, columns=columns)

    # Print the data types of the columns in the DataFrame
    # Remove blank rows
    df = df.dropna(how='all')

    numeric_columns = [col for col in df.columns if isinstance(col, int)]
    df[numeric_columns] = df[numeric_columns].astype(float)

    #st.write(df.columns)

    #st.write(df.columns)

    # Get the store IDs from the column names
    store_ids = [x for x in df.columns[3:]]
    #st.write(store_ids)

   
    # Melt the data so that store IDs become a separate column
    df_melted = pd.melt(
        df,
        id_vars=df.columns[:3],
        value_vars=store_ids,
        var_name="store_id",
        value_name="Yes/No",
    )

    
     # Get the Brewer column data
    brewer_data = df_melted['Brewer']

    


    # Insert the Brewer data into column F
    df_melted.insert(5, "MANUFACTURER", brewer_data)

    #st.write(df_melted)

    # Filter the melted DataFrame to exclude rows with missing values in the "MANUFACTURER" column
    df_melted_filtered = df_melted.dropna(subset=["MANUFACTURER"])
    

    st.write(df_melted)

    # Replace 1 with a green checkmark and NaN with a red X
    df_melted['Yes/No'] = df_melted['Yes/No'].apply(lambda x: 'Yes' if x == 1 else ('No' if pd.isna(x) else '*'))

    st.write(df_melted)
    

    # Move store_id column to the second position and rename it as STORE_NUMBER
    df_melted.insert(1, "STORE_NUMBER", df_melted.pop("store_id"))

   

    # Rename column "STORE NUMBER" to 'STORE NAME'
    df_melted.rename(columns={"STORE NUMBER": "STORE_NAME"}, inplace=True)

    # Add a new column "STORE_NAME" with empty values
    df_melted.insert(0, "STORE_NAME", "")

    # Add a new column "SKU" with empty values
    df_melted.insert(0, "SKU", "")

   

    # Reorder the columns with "STORE_NAME" in position 0, "STORE_NUMBER" in position 1, and "UPC" in position 2
    df_melted = df_melted[["STORE_NAME", "STORE_NUMBER", "UPC"] + [col for col in df_melted.columns if col not in ["STORE_NAME", "STORE_NUMBER", "UPC"]]]

  
    # Define the list of desired columns
    desired_columns = ["STORE_NAME", "STORE_NUMBER", "UPC", "SKU #", "Name", "MANUFACTURER", "SEGMENT", "Yes/No", "ACTIVATION_STATUS", "COUNTY", "CHAIN_NAME"]


    # Reindex the DataFrame with the desired columns
    df_melted = df_melted.reindex(columns=desired_columns)

    

    # Rename the columns as per your requirements
    df_melted.rename(columns={
        "Name": "PRODUCT_NAME",
        "Yes/No": "YES_NO",
        "SKU #": "SKU"
    }, inplace=True)

   
 
    # Display the updated DataFrame
    #print(df_melted)
    

    # Remove ' and , characters from all columns
    df_melted = df_melted.replace({'\'': '', ',': '', '\*': '', 'Yes': '1', 'No': '0'}, regex=True)

    # Fill CHAIN_NAME column with "SMARTFINAL" starting from row 2
    df_melted.loc[0:, "CHAIN_NAME"] = "SMART & FINAL"

    # Fill CHAIN_NAME column with "SMARTFINAL" starting from row 2
    df_melted.loc[0:, "STORE_NAME"] = "SMART & FINAL"

    #st.write(df_melted)

    # Convert DataFrame back to workbook object
    new_workbook = openpyxl.Workbook()
    new_sheet = new_workbook.active
    for row in dataframe_to_rows(df_melted, index=False, header=True):
        new_sheet.append(row)

    return new_workbook


