#5_Distro Grid Processing.py

import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from PIL import Image
import snowflake.connector
from Home import create_snowflake_connection  # Get connection object
from DG_Pivot_Transform import format_pivot_table
from DG_Non_Pivot_Format import format_non_pivot_table
from Distro_Grid_Snowflake_Uploader import update_spinner, upload_distro_grid_to_snowflake
from openpyxl.utils.dataframe import dataframe_to_rows
import openpyxl
from openpyxl import load_workbook




# ==================================================================================================================
# THIS SECTION OF CODE HANDLES THE LOGO AND SETS THE VIEW TO WIDE 
# ==================================================================================================================

def add_logo(logo_path, width, height):
    """Read and return a resized logo"""
    logo = Image.open(logo_path)
    modified_logo = logo.resize((width, height))
    return modified_logo

my_logo = add_logo(logo_path="./images/DeltaPacific_Logo.jpg", width=200, height=100)
st.sidebar.image(my_logo)
st.sidebar.subheader("Delta Pacific Beverage Co.")
st.subheader("Distribution Grid Processing")

## Add horizontal line
st.markdown("<hr>", unsafe_allow_html=True)
st.write("Download Template Here:")
# Add a hyperlink to your GitHub repository

# Add a download link for the Excel file templates
st.markdown(
    "[Download Pivot Table Template](https://github.com/rgriggs0072/ChainLinkAnalytics/raw/master/import_templates/Pivot_Table_Distro_Grid_Template.xlsx)")
st.markdown(
    "[Download Distro Grid Template](https://github.com/rgriggs0072/ChainLinkAnalytics/raw/master/import_templates/Distribution_Grid_Template.xlsx)")

# ==================================================================================================================
# HERE WE CREATE THE FUNCTION TO GET THE CHAIN OPTIONS FROM SNOWFLAKE FOR THE DROPDOWN
# ==================================================================================================================

def get_options():
    conn = create_snowflake_connection()[0]
    cursor = conn.cursor()
    cursor.execute('SELECT option_name FROM options_table ORDER BY option_name')
    options = [row[0] for row in cursor]
    return options

# ==================================================================================================================
# FUNCTION TO UPDATE THE CHAIN OPTIONS IN SNOWFLAKE FOR THE DROPDOWN
# ==================================================================================================================

def update_options(options):
    conn = create_snowflake_connection()[0]
    cursor = conn.cursor()
    cursor.execute('DELETE FROM options_table')
    for option in options:
        cursor.execute("INSERT INTO options_table (option_name) VALUES (%s)", (option,))
    conn.commit()

# ==================================================================================================================
# Create a container to hold the file uploader and the dropdown for selecting the chain distro grid.
# ==================================================================================================================

file_container = st.container()
with file_container:
    st.subheader(":blue[Distro Grid Fomatting Utility]")

options = get_options()

# Initialize session state variables
if 'new_option' not in st.session_state:
    st.session_state.new_option = ""
if 'option_added' not in st.session_state:
    st.session_state.option_added = False

# Create the dropdown and allow adding new options if needed
if not options:
    st.warning("No options available. Please add options to the list.")
else:
    selected_option = st.selectbox(':red[Select the Chain Distro Grid to format]', options + ['Add new option...'], key="existing_option")
    st.session_state.selected_option = selected_option

if selected_option == 'Add new option...':
    new_option = st.text_input('Enter the new option', value=st.session_state.new_option)
    if st.form_submit_button('Add Option') and new_option:
        options.append(new_option)
        update_options(options)
        st.success('Option added successfully!')
        st.session_state.new_option = ""
else:
    st.write(f"You selected: {selected_option}")

# ======================================================================================================================
# USING THE CONTAINER CREATED EARLIER, CREATES THE UPLOADER FOR FORMATTING THE DISTRIBUTION GRID SPREADSHEET.
# Handle formatting based on whether the spreadsheet is a pivot table or not.
# ======================================================================================================================

# if 'acknowledged_warnings' not in st.session_state:
#     st.session_state['acknowledged_warnings'] = False
# if 'file_uploaded' not in st.session_state:
#     st.session_state['file_uploaded'] = False

# Initialize session state for warnings acknowledgment and file handling
if 'acknowledged_warnings' not in st.session_state:
    st.session_state['acknowledged_warnings'] = False
if 'warnings_present' not in st.session_state:
    st.session_state['warnings_present'] = False
if 'file_uploaded' not in st.session_state:
    st.session_state['file_uploaded'] = False

uploaded_file = st.file_uploader(":red[Browse or drag here the Distribution Grid to Format]", type=["xlsx"], key="uploaded_file")

# If a file is uploaded
if uploaded_file:
    st.session_state['file_uploaded'] = True
    workbook = openpyxl.load_workbook(uploaded_file)
    user_selection = st.selectbox("Select Yes if a Pivot Table or Select No if not a Pivot Table:", ["Choose One", "Yes", "No"])

    if user_selection != "Choose One":
        if st.button("Reformat DG Spreadsheet"):
            formatted_workbook = None
            stream = BytesIO()

            if user_selection == "Yes":
                formatted_workbook = format_pivot_table(workbook, st.session_state.selected_option)
                st.success("Pivot table formatted successfully.")
            elif user_selection == "No":
                formatted_workbook = format_non_pivot_table(workbook, stream, st.session_state.selected_option)

            # If warnings are present and have not been acknowledged
            if st.session_state['warnings_present'] and not st.session_state['acknowledged_warnings']:
                st.warning("Warnings detected! Please remove file, correct the issues listed and re-upload file .")
            else:
                st.session_state['acknowledged_warnings'] = True  # Mark as acknowledged

            # Allow file download if warnings were acknowledged or no warnings were present
            if formatted_workbook and st.session_state['acknowledged_warnings']:
                formatted_workbook.save(stream)
                stream.seek(0)
                st.download_button(
                    label="Download formatted spreadsheet",
                    data=stream,
                    file_name="formatted_spreadsheet.xlsx",
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )

            # Allow the user to discard the current upload if warnings are present
            #if st.session_state['warnings_present'] and not st.session_state['acknowledged_warnings']:
            # if st.button("Remove uploaded file and re-upload"):
            #     print("did I click the remove button? ")
            #     st.session_state['uploaded_file'] = None
            #     st.session_state['acknowledged_warnings'] = False
            #     st.session_state['warnings_present'] = False
            #     st.session_state['file_uploader'] = None  # Clear the file uploader
            #     print("what the hell is the file session state? ", st.session_state['uploaded_file'])
            #     st.rerun()
            # st.write("do we have a session state damn it?",st.session_state['file_uploaded'] )                   

# ======================================================================================================================
# CREATE FILE UPLOADER IN PREPARATION TO WRITE TO SNOWFLAKE
# ======================================================================================================================

snowflake_file_container = st.container()
with snowflake_file_container:
    st.markdown("<hr>", unsafe_allow_html=True)
    st.subheader(":blue[Write Distribution Grid to Snowflake Utility]")

conn = create_snowflake_connection()[0]

uploaded_files = st.file_uploader("Browse or select formatted Distribution Grid excel sheets", type=["xlsx"], accept_multiple_files=True)

# Process each uploaded file and prepare for Snowflake upload
for uploaded_file in uploaded_files:
    excel_file = pd.ExcelFile(uploaded_file)
    sheet_names = excel_file.sheet_names

    for sheet_name in sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        button_key = f"import_button_{uploaded_file.name}_{sheet_name}"
        if st.button("Import Distro Grid into Snowflake", key=button_key):
            with st.spinner('Uploading data to Snowflake ...'):
                upload_distro_grid_to_snowflake(df, selected_option, update_spinner)








