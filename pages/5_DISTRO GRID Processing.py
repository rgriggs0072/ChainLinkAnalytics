import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from PIL import Image
from openpyxl import Workbook
import snowflake.connector
from Home import create_snowflake_connection  # Get connection object
from DG_Pivot_Transform import format_pivot_table
from DG_Non_Pivot_Format import format_non_pivot_table
from Distro_Grid_Snowflake_Uploader import update_spinner, upload_distro_grid_to_snowflake
from openpyxl.utils.dataframe import dataframe_to_rows
import openpyxl
# from streamlit_extras.app_logo import add_logo #can be removed
import datetime
from PIL import Image
from openpyxl import load_workbook

st.set_page_config(layout="wide")


# ==================================================================================================================

# THIS SECTION OF CODE HANDLES THE LOGO AND SETS THE VIEW TO WIDE 

# ==================================================================================================================



def add_logo(logo_path, width, height):
    """Read and return a resized logo"""
    logo = Image.open(logo_path)
    modified_logo = logo.resize((width, height))
    return modified_logo


# add_logo("./images/DeltaPacific_Logo.jpg", width = 200, height = 100)
my_logo = add_logo(logo_path="./images/DeltaPacific_Logo.jpg", width=200, height=100)
st.sidebar.image(my_logo)
st.sidebar.subheader("Delta Pacific Beverage Co.")
st.subheader("Distribution Grid Processing")

## Add horizontal line
st.markdown("<hr>", unsafe_allow_html=True)
st.write("Download Template Here:")
# Add a hyperlink to your GitHub repository


# Your Streamlit app content

# Add a download link for the Excel file
st.markdown(
    "[Download Pivot Table Template](https://github.com/rgriggs0072/ChainLinkAnalytics/raw/master/import_templates/Pivot_Table_Distro_Grid_Template.xlsx)")
st.markdown(
    "[Download Distro Grid Template](https://github.com/rgriggs0072/ChainLinkAnalytics/raw/master/import_templates/Distribution_Grid_Template.xlsx)")


# ==================================================================================================================

# END OF THE SECTION OF CODE HANDLES THE LOGO AND SETS THE VIEW TO WIDE 

# ==================================================================================================================


# ====================================================================================================================
# HERE WE CREATE THE FUNCTION TO GET THE CHAIN OPTIONS FROM SNOWFLAKE FOR THE DROPDOWN
# ====================================================================================================================

# Function to retrieve options from Snowflake table
def get_options():
    conn = create_snowflake_connection()[0]  # Get connection object
    cursor = conn.cursor()
    cursor.execute('SELECT option_name FROM options_table ORDER BY option_name')
    options = [row[0] for row in cursor]
    return options


# #====================================================================================================================
# # END THE FUNCTION TO GET THE CHAIN OPTIONS FROM SNOWFLAKE FOR THE DROPDOWN
# #====================================================================================================================


# ====================================================================================================================
# THE FUNCTION TO UPDATE THE CHAIN OPTIONS IN SNOWFLAKE FOR THE DROPDOWN
# ====================================================================================================================
# Function to update options in Snowflake table
def update_options(options):
    conn = create_snowflake_connection()[0]  # Get connection object
    cursor = conn.cursor()
    cursor.execute('DELETE FROM options_table')
    for option in options:
        cursor.execute("INSERT INTO options_table (option_name) VALUES (%s)", (option,))
    conn.commit()


# ====================================================================================================================
# END THE FUNCTION TO UPDATE THE CHAIN OPTIONS IN SNOWFLAKE FOR THE DROPDOWN
# ====================================================================================================================


# ===================================================================================================================

# Create a container to hold the file uploader
# ===================================================================================================================
file_container = st.container()

# ===================================================================================================================
# ASSISGN AND Add a title to the container
# ===================================================================================================================


with file_container:
    st.subheader(":blue[Distro Grid Fomatting Utility]")

# ===================================================================================================================
# END ASSISGN AND Add a title to the container
# ===================================================================================================================

# --------------------------------------------------------------------------------------------------------------------

# ===================================================================================================================
# Below will get the options for the dropdown to select the store being uploaded or create a new entry if the one
# you want is not available
# ===================================================================================================================


# Retrieve options from Snowflake table
options = get_options()

# Initialize session state variables
if 'new_option' not in st.session_state:
    st.session_state.new_option = ""
if 'option_added' not in st.session_state:
    st.session_state.option_added = False

# Check if options are available
if not options:
    st.warning("No options available. Please add options to the list.")
else:
    # Create the dropdown in Streamlit

    with file_container:
        selected_option = st.selectbox(':red[Select the Chain Distro Grid to format]', options + ['Add new option...'],
                                       key="existing_option")

        # Check if the selected option is missing and allow the user to add it
if selected_option == 'Add new option...':
    st.write("You selected: Add new option...")

    # Show the form to add a new option
    with st.form(key='add_option_form', clear_on_submit=True):
        new_option = st.text_input('Enter the new option', value=st.session_state.new_option)
        submit_button = st.form_submit_button('Add Option')

        if submit_button and new_option:
            options.append(new_option)
            update_options(options)
            st.success('Option added successfully!')
            st.session_state.option_added = True

            # Clear the text input field
            st.session_state.new_option = ""

        else:
            # Display the selected option
            st.write(f"You selected: {selected_option}")

# =====================================================================================================================
# END Below will get the options for the dropdown to select the store being uploaded or create a new entry if the one
# you want is not available
# =====================================================================================================================

# ---------------------------------------------------------------------------------------------------------------------

# ======================================================================================================================
# Using the container created earlier creates the uploader for formatting the Distribution Grid spreadsheet.  Also
# a selection for choosing a Privot table type Spreadsheet or just a vertical column spreadsheet.  Based on your
# selection yes it is a pivot table then the function format_pivot_table is called to handle for transformation
# and if no then the function format_non_pivot_table is called for sheet validation
# ======================================================================================================================


# Initialize session state for acknowledged warnings if it doesn't exist
if 'acknowledged_warnings' not in st.session_state:
    st.session_state['acknowledged_warnings'] = False

with st.container():
    uploaded_file = st.file_uploader(":red[Browse or drag here the Distribution Grid to Format]", type=["xlsx"])

    if uploaded_file is None:
        st.warning("Please upload a spreadsheet first.")
    else:
        workbook = openpyxl.load_workbook(uploaded_file)
        user_selection = st.selectbox("Select Yes if a Pivot Table or Select No if not a Pivot Table:",
                                      ["Choose One", "Yes", "No"])

        if user_selection == "Choose One":
            st.info("Please select whether the spreadsheet is a Pivot Table or not.")
        elif user_selection in ["Yes", "No"]:
            if st.button("Reformat DG Spreadsheet"):
                formatted_workbook = None  # Initialize the variable
                stream = BytesIO()  # Define the stream variable

                if user_selection == "Yes":
                    # Process the uploaded file before using it
                    formatted_workbook = format_pivot_table(workbook)
                    st.success("Pivot table formatted successfully.")
                elif user_selection == "No":
                    # Call your non-pivot formatting function here
                    formatted_workbook = format_non_pivot_table(workbook, stream)

                    # If no warnings are detected, manually set acknowledged_warnings to True
                    if not st.session_state.get('warnings_present', False):
                        st.session_state['acknowledged_warnings'] = True

                # Create a new filename based on the selected option
                new_filename = "formatted_spreadsheet.xlsx"

                # Save and provide the download link for the formatted workbook if it's not None
                if formatted_workbook is not None and (st.session_state['acknowledged_warnings'] or not st.session_state.get('warnings_present', False)):
                    formatted_workbook.save(stream)
                    stream.seek(0)

                    st.download_button(
                        label="Download formatted spreadsheet",
                        data=stream,
                        file_name=new_filename,
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    )


# ======================================================================================================================
# END Using the container created earlier creates the uploader for formatting the Distribution Grid spreadsheet.  Also
# a selection for choosing a Privot table type Spreadsheet or just a vertical column spreadsheet.  Based on your
# selection yes it is a pivot table then the function format_pivot_table is called to handle for transformation
# and if no then the function format_non_pivot_table is called for sheet validation
# ======================================================================================================================

# ----------------------------------------------------------------------------------------------------------------------


# ===========================================================================================================
# create code uploader in preparation to write to snowflake
# ==========================================================================================================

# Create a container to hold the file uploader
snowflake_file_container = st.container()

# Add a title to the container
with snowflake_file_container:
    st.markdown("<hr>", unsafe_allow_html=True)
    st.subheader(":blue[Write Distribution Grid to Snowflake Utility]")

with snowflake_file_container:
    conn = create_snowflake_connection()[0]  # Get connection object

# Initialize session state variables
if 'new_option' not in st.session_state:
    st.session_state.new_option = ""
if 'option_added' not in st.session_state:
    st.session_state.option_added = False

# Check if options are available
if not options:
    st.warning("No options available. Please add options to the list.")
else:
    # Create the dropdown in Streamlit

    selected_option = st.selectbox(':red[Select the Chain Distro Grid to upload to Snowflake]',
                                   options + ['Add new option...'], key="existing_chain_option")

    # Check if the selected option is missing and allow the user to add it
    if selected_option == 'Add new option...':
        st.write("You selected: Add new option...")

        # Show the form to add a new option
        with st.form(key='add_option_form', clear_on_submit=True):
            new_option = st.text_input('Enter the new option', value=st.session_state.new_option)
            submit_button = st.form_submit_button('Add Option')

            if submit_button and new_option:
                options.append(new_option)
                update_options(options)
                st.success('Option added successfully!')
                st.session_state.option_added = True

        # Clear the text input field
        st.session_state.new_option = ""

    else:
        # Display the selected option
        st.write(f"You selected: {selected_option}")
    # create file uploader
    uploaded_files = st.file_uploader("Browse or select formatted Distribution Grid excel sheets", type=["xlsx"],
                                      accept_multiple_files=True)

# Process each uploaded file
for uploaded_file in uploaded_files:
    # Read Excel file into pandas ExcelFile object
    excel_file = pd.ExcelFile(uploaded_file)

    ## Get sheet names from ExcelFile object
    sheet_names = excel_file.sheet_names

    # Display DataFrame for each sheet in Streamlit
    for sheet_name in sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

    # #===========================================================================================================
    # End of code to create code uploader in preparation to write to snowflake
    # ==========================================================================================================

    # Write DataFrame to Snowflake on button click
    button_key = f"import_button_{uploaded_file.name}_{sheet_name}"
    if st.button("Import Distro Grid into Snowflake", key=button_key):
        with st.spinner('Uploading data to Snowflake ...'):
            # Write DataFrame to Snowflake based on the selected store

            upload_distro_grid_to_snowflake(df, selected_option, update_spinner)






