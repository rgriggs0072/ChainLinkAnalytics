from openpyxl import workbook
import streamlit as st
import pandas as pd




# def format_non_pivot_table(workbook, stream):
#     # Convert the worksheet to a DataFrame
#     df = pd.DataFrame(workbook.active.values)

#     # Iterate over each row to check store name, store number, and UPC
#     for index, row in df.iterrows():
#         store_name = row[0]
#         store_number = row[1]
#         upc = row[2]

#         # Check if any value is missing
#         if pd.isna(store_name) or pd.isna(store_number) or pd.isna(upc):
#             st.warning(f"Please ensure there is a value for store name, store number, and UPC in row {index + 1}.")
#             return None  # Return None to indicate an issue

#     # If everything is valid, you can continue processing the workbook
#     return workbook



# import pandas as pd
# import streamlit as st

# def format_non_pivot_table(workbook, stream):
#     # Convert the worksheet to a DataFrame
#     df = pd.DataFrame(workbook.active.values)

#     # Initialize list to collect rows with missing values
#     rows_with_missing_values = []

#     # Iterate over each row to check store name, store number, and UPC
#     for index, row in df.iterrows():
#         missing_columns = []
#         for col_idx, value in enumerate(row):
#             if pd.isna(value):
#                 # Determine the column name based on the column index
#                 column_name = None  # Initialize column_name variable
#                 if col_idx == 0:
#                     column_name = "STORE NAME"
#                 elif col_idx == 1:
#                     column_name = "STORE NUMBER"
#                 elif col_idx == 2:
#                     column_name = "UPC"

#                 if column_name is not None:
#                     missing_columns.append(column_name)

#         if missing_columns:
#             missing_columns_str = ", ".join(missing_columns)
#             rows_with_missing_values.append((index + 1, missing_columns_str))

#     if rows_with_missing_values:
#         for row_index, missing_columns_str in rows_with_missing_values:
#             st.warning(f"Please ensure there is a value for {missing_columns_str} in row {row_index}.")
#         return None  # Return None to indicate an issue

#     # If everything is valid, you can continue processing the workbook
#     return workbook


import pandas as pd
import streamlit as st

def format_non_pivot_table(workbook, stream, selected_option):
    # Convert the worksheet to a DataFrame
    df = pd.DataFrame(workbook.active.values)

    # Initialize lists to collect rows with issues
    rows_with_missing_values = []
    rows_with_apostrophe_issues = []

    # Unicode representation of the smart quote
    smart_quote = "\u2019"

    # Iterate over each row to check for missing values and apostrophes in specific columns
    for index, row in df.iterrows():
        missing_columns = []
        store_name = str(row[0])  # Assuming store name is in the first column

        # Normalize the store name by replacing apostrophes and smart quotes
        normalized_store_name = store_name.replace("'", "").replace(smart_quote, "")

        # Check for missing values
        for col_idx, value in enumerate(row):
            if pd.isna(value):
                column_name = None
                if col_idx == 0:
                    column_name = "STORE NAME"
                elif col_idx == 1:
                    column_name = "STORE NUMBER"
                elif col_idx == 2:
                    column_name = "UPC"

                if column_name:
                    missing_columns.append(column_name)

        # Check for apostrophes or smart quotes in the store name
        if "'" in store_name or smart_quote in store_name:
            rows_with_apostrophe_issues.append(f"Row {index + 1}: STORE NAME contains an apostrophe or smart quote - {store_name}")

        if missing_columns:
            missing_columns_str = ", ".join(missing_columns)
            rows_with_missing_values.append(f"Row {index + 1}: {missing_columns_str}")

    # Set session state flags based on the warnings
    st.session_state['warnings_present'] = bool(rows_with_missing_values or rows_with_apostrophe_issues)

    # Display warnings for missing values
    if rows_with_missing_values:
        with st.expander("Warning! Missing Values Detected", expanded=True):
            for warning in rows_with_missing_values:
                st.error(warning)

    # Display warnings for store names with apostrophes
    if rows_with_apostrophe_issues:
        with st.expander("Warning! Store Names with Apostrophes Detected", expanded=True):
            for warning in rows_with_apostrophe_issues:
                st.error(warning)

    # Display an "Acknowledge and Continue" button only if there are warnings
    #if st.session_state['warnings_present']:
        # if st.button("Acknowledge and Continue"):
        #     st.session_state['acknowledged_warnings'] = True  # Acknowledge warnings

    # Return the workbook after processing
    return workbook








