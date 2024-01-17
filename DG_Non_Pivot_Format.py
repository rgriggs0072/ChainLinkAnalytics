from openpyxl import workbook
import streamlit as st
import pandas as pd


def format_non_pivot_table(workbook, stream):
    # Convert the worksheet to a DataFrame
    df = pd.DataFrame(workbook.active.values)
    #st.write(df)
    # Check for NaN cells
    # nan_count = df.isna().sum().sum()
    # if nan_count > 0:
    #     st.warning(f"There are {nan_count} NaN cells in the spreadsheet. Please fill or remove them.")
    #     return None  # Return None to indicate an issue

    # Validate store name and store number for every line
    if any(df.iloc[:, 0:2].isna().any(axis=1)):
        st.warning("Please ensure there is a store name and store number for every line.")
        return None  # Return None to indicate an issue

    # If everything is valid, you can continue processing the workbook

    return workbook
