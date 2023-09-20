import streamlit as st
import pandas as pd
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows



def format_RALEYS_DistroGrid(workbook):
    # Assuming the sheet name is 'RALEYS_DG', you can modify it as per your actual sheet name
    sheet = workbook['RALEYS_DG']

    #st.write("you made it to raleys code")

    # Read the data from the sheet into a DataFrame
    data = sheet.values

   
    columns = next(data)  # Get the column names from the first row
    df = pd.DataFrame(data, columns=columns)
    
    #st.write(df)

    # Get the store IDs from the column names
    store_ids = [x for x in df.columns[7:]]

    # Melt the data so that store IDs become a separate column
    df_melted = pd.melt(
        df,
        id_vars=df.columns[:6],
        value_vars=store_ids,
        var_name="store_id",
        value_name="Yes/No",
    )
    #st.write(df_melted)
    
    # Assuming you have already created and transformed df_melted as mentioned in your code
    # Move data from column 'SKU #' to 'SKU'
    #df_melted['SKU'] = df_melted['Raleys SKU #']
    
    # Now, you can drop the original 'SKU #' column if you no longer need it
    #df_melted.drop(columns=['Raleys SKU #'], inplace=True)

    # Replace 1 with a green checkmark and NaN with a red X
    df_melted['Yes/No'] = df_melted['Yes/No'].apply(lambda x: 'Yes' if x == 1 else ('No' if pd.isna(x) else '*'))

    # Move store_id column to the second position and rename it as STORE_NUMBER
    df_melted.insert(1, "STORE_NUMBER", df_melted.pop("store_id"))


    
    # Rename column "STORE NUMBER" to 'STORE NAME'
    df_melted.rename(columns={"STORE NUMBER": "STORE_NAME"}, inplace=True)

    # Add a new column "STORE_NAME" with empty values
    df_melted.insert(0, "STORE_NAME", "")
    
    # Add the missing columns with empty values
    df_melted['Manufacturer'] = ""
    df_melted['ACTIVATION_STATUS'] = ""
    df_melted['COUNTY'] = ""
    df_melted['CHAIN_NAME'] ="RALEYS"
    #st.write(df_melted)
    # Reorder the columns with "STORE_NAME" in position 0, "STORE_NUMBER" in position 1, and "UPC" in position 2
    df_melted = df_melted[["STORE_NAME", "STORE_NUMBER", "UPC", "Raleys SKU #", "Name", "Manufacturer", "Retailer Segment", "Yes/No", "ACTIVATION_STATUS", "COUNTY", "CHAIN_NAME"]]

    # # Reorder the columns with "STORE_NAME" in position 0, "STORE_NUMBER" in position 1, and "UPC" in position 2
    # df_melted = df_melted[["STORE_NAME", "STORE_NUMBER", "UPC"] + [col for col in df_melted.columns if col not in ["STORE_NAME", "STORE_NUMBER", "UPC"]]]

    # Delete columns F and G
    #df_melted = df_melted.drop(columns=["Distro %", "Store Count"])

  
    
    # Define the list of desired columns
    desired_columns = ["STORE_NAME", "STORE_NUMBER", "UPC", "SKU #", "Name", "Manufacturer", "SEGMENT", "Yes/No", "ACTIVATION_STATUS", "COUNTY", "CHAIN_NAME"]


   
    # Reindex the DataFrame with the desired columns
    #df_melted = df_melted.reindex(columns=desired_columns)
    
    

    # Rename the columns as per your requirements
    df_melted.rename(columns={
        "Name": "PRODUCT_NAME",
        "Yes/No": "YES_NO",
        "Raleys SKU #": "SKU",
        "Retailer Segment": "SEGMENT"
        
    }, inplace=True)

    import re

    # Assuming df_melted is your DataFrame
    df_melted['UPC'] = df_melted['UPC'].apply(lambda x: re.sub(r'S$', '', x) if isinstance(x, str) else x)


    # Remove ' and , characters from all columns
    df_melted = df_melted.replace({'\'': '', ',': '', '\*': ''}, regex=True)

    # Fill CHAIN_NAME column with "RALEYS" starting from row 2
    #df_melted.loc[0:, "CHAIN_NAME"] = "RALEYS"

    # Assuming df_melted is your DataFrame
    df_melted['SKU'] = df_melted['SKU'].replace('Waiting', '0')
    df_melted['SKU'] = df_melted['SKU'].replace('In Process', '0')
    df_melted['YES_NO'] = df_melted['YES_NO'].replace('No', 0)
    df_melted['YES_NO'] = df_melted['YES_NO'].replace('Yes', 1)
    

    ## Convert DataFrame back to workbook object
    new_workbook = openpyxl.Workbook()
    new_sheet = new_workbook.active
    for row in dataframe_to_rows(df_melted, index=False, header=True):
        new_sheet.append(row)


    

    

    return new_workbook



