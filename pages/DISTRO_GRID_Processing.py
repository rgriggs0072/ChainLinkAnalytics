import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
from PIL import Image
from openpyxl import Workbook
import snowflake.connector  
from Target_DG_format import format_TARGET_DistroGrid
from Foodmaxx_DG_format import format_FOODMAXX_DistroGrid
from Luckys_DG_format import format_LUCKYS_DistroGrid
from Savemart_DG_format import format_SAVEMART_DistroGrid
from Walmart_DG_format import format_WALMART_DistroGrid
from Raleys_DG_format import format_RALEYS_DistroGrid
from Safeway_DG_format import format_SAFEWAY_DistroGrid
from Wholefoods_DG_format import format_WHOLEFOODS_DistroGrid
from Sprouts_DG_format import format_SPROUTS_DistroGrid
from Smart_Final_DG_format import format_SMART_FINAL_DistroGrid
from Distro_Grid_Snowflake_Uploader import upload_SAFEWAY_distro_grid_to_snowflake
from openpyxl.utils.dataframe import dataframe_to_rows
import openpyxl
# from streamlit_extras.app_logo import add_logo #can be removed
import datetime



#==================================================================================================================

# THIS SECTION OF CODE HANDLES THE LOGO AND SETS THE VIEW TO WIDE 

#==================================================================================================================


# Displaying images on the front end
from PIL import Image
st.set_page_config(layout="wide")


def add_logo(logo_path, width, height):
    """Read and return a resized logo"""
    logo = Image.open(logo_path)
    modified_logo = logo.resize((width, height))
    return modified_logo
#add_logo("./images/DeltaPacific_Logo.jpg", width = 200, height = 100)
my_logo = add_logo(logo_path="./images/DeltaPacific_Logo.jpg", width=200, height = 100)
st.sidebar.image(my_logo)
st.sidebar.subheader("Delta Pacific Beverage Co.")
st.subheader("Distribution Grid Processing")


## Add horizontal line
st.markdown("<hr>", unsafe_allow_html=True)
#==================================================================================================================

# END OF THE SECTION OF CODE HANDLES THE LOGO AND SETS THE VIEW TO WIDE 

#==================================================================================================================


#====================================================================================================================
# HERE WE CREATE CONNECTION TO SNOWFLAKE
#====================================================================================================================


# Load Snowflake credentials from the secrets.toml file
snowflake_creds = st.secrets["snowflake"]

# Establish a new connection to Snowflake
conn = snowflake.connector.connect(
    account=snowflake_creds["account"],
    user=snowflake_creds["user"],
    password=snowflake_creds["password"],
    warehouse=snowflake_creds["warehouse"],
    database=snowflake_creds["database"],
    schema=snowflake_creds["schema"]
)

#====================================================================================================================
# End CREATE CONNECTION TO SNOWFLAKE
#====================================================================================================================



#====================================================================================================================
# HERE WE CREATE THE FUNCTION TO GET THE CHAIN OPTIONS FROM SNOWFLAKE FOR THE DROPDOWN
#====================================================================================================================

# Function to retrieve options from Snowflake table
def get_options():
    cursor = conn.cursor()
    cursor.execute('SELECT option_name FROM options_table ORDER BY option_name')
    options = [row[0] for row in cursor]
    return options

#====================================================================================================================
# END THE FUNCTION TO GET THE CHAIN OPTIONS FROM SNOWFLAKE FOR THE DROPDOWN
#====================================================================================================================



#====================================================================================================================
# THE FUNCTION TO UPDATE THE CHAIN OPTIONS IN SNOWFLAKE FOR THE DROPDOWN
#====================================================================================================================
# Function to update options in Snowflake table
def update_options(options):
    cursor = conn.cursor()
    cursor.execute('DELETE FROM options_table')
    for option in options:
        cursor.execute("INSERT INTO options_table (option_name) VALUES (%s)", (option,))
    conn.commit()


#====================================================================================================================
# END THE FUNCTION TO UPDATE THE CHAIN OPTIONS IN SNOWFLAKE FOR THE DROPDOWN
#====================================================================================================================



#===================================================================================================================

# Create a container to hold the file uploader
#===================================================================================================================
file_container = st.container()

#===================================================================================================================
# ASSISGN AND Add a title to the container
#===================================================================================================================

with file_container:
     st.subheader(":blue[Distro Grid Fomatting Utility]")


#===================================================================================================================
# END ASSISGN AND Add a title to the container
#===================================================================================================================


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
    
        selected_option = st.selectbox(':red[Select the Chain Distro Grid to format]', options + ['Add new option...'], key="existing_option")

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


#====================================================================================================================
# Distibution Grid formatter 

#======================================================================================================================

# Add the file uploader inside the container

    
uploaded_file = st.file_uploader(":red[Browse or drag here the Distribution Grid to Format]", type=["xlsx"])


    # Add horizontal line



formatted_workbook = None  # Initialize the variable

        
with file_container:
    if st.button("Reformat DG Spreadsheet"):
        if uploaded_file is None:
            st.warning("Please upload a spreadsheet first.")
        else:
            # Load the workbook
            workbook = openpyxl.load_workbook(uploaded_file)
            

            # Call the format_TARGET_DistroGrid function for 'TARGET' option
            if selected_option == 'TARGET':
                formatted_workbook = format_TARGET_DistroGrid(workbook)

            elif selected_option == 'FOODMAXX': #ADD THIS CONDITION FOR 'Food Maxx' OPTION
                formatted_workbook = format_FOODMAXX_DistroGrid(workbook)

            elif selected_option == 'LUCKYS': #ADD THIS CONDITION FOR 'LUCKYS' OPTION
                formatted_workbook = format_LUCKYS_DistroGrid(workbook)
            
            elif selected_option == 'SAFEWAY':  # Add this condition for 'SAFEWAY' option
                formatted_workbook = format_SAFEWAY_DistroGrid(workbook)

            elif selected_option == 'WALMART': #ADD THIS CONDITION FOR 'WALMART' OPTION
                formatted_workbook = format_WALMART_DistroGrid(workbook)

            elif selected_option == 'SAVEMART': #ADD THIS CONDITION FOR 'Save Mart' OPTION
                formatted_workbook = format_SAVEMART_DistroGrid(workbook)

            elif selected_option == 'SPROUTS': #ADD THIS CONDITION FOR 'SPROUTS' OPTION
                formatted_workbook = format_SPROUTS_DistroGrid(workbook)

            elif selected_option == 'RALEYS': #ADD THIS CONDITION FOR 'RALEYS' OPTION
                formatted_workbook = format_RALEYS_DistroGrid(workbook)

            elif selected_option == 'WHOLEFOODS': #ADD THIS CONDITION FOR 'WHOLEFOODS' OPTION
                formatted_workbook = format_WHOLEFOODS_DistroGrid(workbook)

            elif selected_option == 'SMART_FINAL': #ADD THIS CONDITION FOR 'SMART_FINAL' OPTION
                formatted_workbook = format_SMART_FINAL_DistroGrid(workbook)


                
            else:
                # Call other formatting functions for different options
                # Add your code here for other formatting functions
                formatted_workbook = workbook  # Use the original workbook

            # Create a new filename based on the selected option
            st.write("I am back")
            new_filename = f"formatted_{selected_option}_spreadsheet.xlsx"

            


# Check if the workbook was successfully formatted
if formatted_workbook is not None:
    
    # Save the formatted workbook to a stream
    stream = BytesIO()
    formatted_workbook.save(stream)
    stream.seek(0)

   
    # Provide the download link for the formatted spreadsheet
    with file_container:
        st.download_button(
            label="Download formatted spreadsheet",
            data=stream.read(),
            file_name=new_filename,
            mime='application/vnd.ms-excel'
        )
        

# Close the Snowflake connection
conn.close()



#=======================================================================================================
# End of distribution grid formatting code
#========================================================================================================


#===========================================================================================================
# create code uploader in preparation to write to snowflake
#==========================================================================================================

# Create a container to hold the file uploader
snowflake_file_container = st.container()


# Add a title to the container
with snowflake_file_container:
    st.markdown("<hr>", unsafe_allow_html=True)
    st.subheader(":blue[Write Distribution Grid to Snowflake Utility]")

with snowflake_file_container:

    # Load Snowflake credentials from the secrets.toml file
    snowflake_creds = st.secrets["snowflake"]

    # Establish a new connection to Snowflake
    conn = snowflake.connector.connect(
        account=snowflake_creds["account"],
        user=snowflake_creds["user"],
        password=snowflake_creds["password"],
        warehouse=snowflake_creds["warehouse"],
        database=snowflake_creds["database"],
        schema=snowflake_creds["schema"]
    )
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
    
        selected_option = st.selectbox(':red[Select the Chain Distro Grid to upload to Snowflake]', options + ['Add new option...'], key="existing_chain_option")

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
        uploaded_files = st.file_uploader("Browse or select formatted Distribution Grid excel sheets", type=["xlsx"], accept_multiple_files=True)

# Process each uploaded file
for uploaded_file in uploaded_files:
    # Read Excel file into pandas ExcelFile object
    excel_file = pd.ExcelFile(uploaded_file)

    ## Get sheet names from ExcelFile object
    sheet_names = excel_file.sheet_names

    

    # Display DataFrame for each sheet in Streamlit
    for sheet_name in sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)


#===========================================================================================================
# End of code to create code uploader in preparation to write to snowflake
#==========================================================================================================




def table_exists(conn, schema, table_name):
    cursor = conn.cursor()
    query = """
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = %s
    AND TABLE_NAME = %s
    """
    cursor.execute(query, (schema, table_name))
    result = cursor.fetchone()
    cursor.close()
    return result[0] > 0



import os

# Process each uploaded file
for uploaded_file in uploaded_files:
    # Read Excel file into pandas ExcelFile object
    excel_file = pd.ExcelFile(uploaded_file)

    # Get the file name from the uploaded_file object
    file_name = uploaded_file.name

    # Extract the file name without the extension
    file_name_without_extension = os.path.splitext(file_name)[0]

    # Split the file name using underscores to get the chain name
    chain_name = file_name_without_extension.split('_')[1]  # Assuming the format is 'formatted_CHAINNAME_spreadsheet.xlsx'

    # Get sheet names from ExcelFile object
    sheet_names = excel_file.sheet_names
    
    # Display DataFrame for each sheet in Streamlit
    for sheet_name in sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Display DataFrame in Streamlit
        st.dataframe(df)

        

   

    # Write DataFrame to Snowflake on button click
    button_key = f"import_button_{uploaded_file.name}_{sheet_name}"
    if st.button("Import Distro Grid into Snowflake", key=button_key):
        with st.spinner('Uploading data to Snowflake ...'):
            # Create a table if it doesn't exist
            table_name = "DISTRO_GRID_TEST"  # Replace with your table name
            schema = "DATASETS"  # Replace with your schema name

        # Write DataFrame to Snowflake based on the selected store
            if chain_name  == selected_option:
                upload_SAFEWAY_distro_grid_to_snowflake(df, schema,table_name,selected_option)
            
            #elif selected_option == "LUCKYS":
            #    upload_reset_SCH_LUCKY_data(df, "COMPUTE_WH", "datasets", "DATASETS")
            #elif selected_option == "WALMART":
            #    upload_reset_SCH_WALMART_data(df, "COMPUTE_WH", "datasets", "DATASETS")
            #    # Add more if-else statements for other stores as needed
            #elif selected_option == "RALEYS":
            #    upload_reset_SCH_RALEYS_data(df, "COMPUTE_WH", "datasets", "DATASETS")
            ## Add more if-else statements for other stores as needed
            #elif selected_option == "FOODMAXX":
            #    upload_reset_SCH_FOODMAXX_data(df, "COMPUTE_WH", "datasets", "DATASETS")
            #elif selected_option == "SMART_FINAL":
            #    upload_reset_SCH_SMART_FINAL_data(df, "COMPUTE_WH", "datasets", "DATASETS")
            #elif selected_option == "SPROUTS":
            #    upload_reset_SCH_SPROUTS_data(df, "COMPUTE_WH", "datasets", "DATASETS")
            #elif selected_option == "TARGET":
            #    upload_reset_SCH_TARGET_data(df, "COMPUTE_WH", "datasets", "DATASETS")
            #elif selected_option == "WHOLEFOODS":
            #    upload_reset_SCH_WHOLEFOODS_data(df, "COMPUTE_WH", "datasets", "DATASETS")
            #elif selected_option == "SAVEMART":
            #     upload_reset_SCH_SAVEMART_data(df, "COMPUTE_WH", "datasets", "DATASETS")
            # Add more if-else statements for other stores as needed
            else:
            # Call other formatting functions for different options
            #st.write("test")#formatted_workbook = workbook  # Use the original workbook    
            # Call other formatting functions for different options
                   # Inform the user that the selected chain does not match the file name
                st.warning(f"The selected chain '{selected_option}' does not match the chain in the file name '{chain_name}'.")
                #formatted_workbook = workbook  # Use the original workbook

        # Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)

        # Replace empty strings with None
        df = df.replace('', None)

        


def create_replace_distro_grid_table():
    
    
    # Get the current date and time
    current_date_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Define the table names
    backup_table_name = f"DISTRO_GRID_{current_date_time}"
    distro_grid_table_name = "DISTRO_GRID_testing"
    distro_grid_temp_table_name = "DISTRO_GRID_TEMP"
    
    # Create a backup table with the current date and time
    create_backup_query = f"CREATE OR REPLACE TABLE {backup_table_name} AS SELECT * FROM {distro_grid_table_name};"
    
    # Truncate the distro_grid table
    truncate_distro_grid_query = f"TRUNCATE TABLE {distro_grid_table_name};"
    
    # Move data from distro_grid_temp to distro_grid
    move_data_query = f"INSERT INTO {distro_grid_table_name} SELECT * FROM {distro_grid_temp_table_name};"
    
    # Truncate the distro_grid_temp table
    truncate_distro_grid_temp_query = f"TRUNCATE TABLE {distro_grid_temp_table_name};"
    
    # Execute the queries
    cursor = conn.cursor()
    cursor.execute(create_backup_query)
    cursor.execute(truncate_distro_grid_query)
    cursor.execute(move_data_query)
    cursor.execute(truncate_distro_grid_temp_query)
    
    # Update the metadata table
    metadata_table_name = "METADATA"  # Replace with your metadata table name
    status_query = f"""
    UPDATE {metadata_table_name}
    SET STATUS = CASE
        WHEN STATUS = 'ACTIVE' THEN 'ARCHIVE'
        ELSE 'ACTIVE'
    END
    WHERE TABLE_NAME = '{distro_grid_table_name}'
    """
    cursor.execute(status_query)
    
    # Close the connection
    conn.close()

##====================================================================================================================
## The code below adds a button to th sidebar that will take the distribution grid live wipping out all old data
##====================================================================================================================


## Streamlit app code
#st.sidebar.subheader(":blue[Distro Grid Go-Live Utility]")

## Create a form in the Streamlit sidebar
#with st.sidebar.form("go_live_form"):
#    st.subheader("Go Live with New Distribution Grid")
#    st.write(":red[By clicking 'Go Live with New Distribution Grid', you will delete your current distro_grid table and perform the data migration.]")
#    st.write(":red[This action cannot be undone.]")
#    submitted = st.form_submit_button("Go Live with Distribution Grid Go Live")
    
#    # If the form is submitted, display a confirmation dialog
#    if submitted:
#        confirmation = st.warning("Are you sure you want to proceed? This action cannot be undone.",icon="⚠️")
#        if confirmation.button("Yes, I want to proceed"):
#            result = create_replace_distro_grid_table()
#            if result:
#                st.sidebar.success("New Distro_Grid table created/replaced and data moved successfully!")
#            else:
#                st.sidebar.error("An error occurred while creating/replacing the Distro_Grid table and moving data.")