# from cgitb import text
from re import S
import streamlit as st
import snowflake.connector
import pandas as pd
from pandas import Series, DataFrame
from PIL import Image
import plotly.express as px
import numpy as np
import base64
import time
import altair as alt
import decimal
import streamlit.components.v1 as components
from datetime import datetime

import logging
import uuid
from io import BytesIO
# Set page to wide display to give more room

st.set_page_config(
    layout="wide",
    initial_sidebar_state="collapsed")
padding_top = 0

#from streamlit.elements.image import MAXIMUM_CONTENT_WIDTH
from cryptography.hazmat.primitives import serialization

# Keep your app's logger at INFO level
logging.basicConfig(level=logging.INFO)
db_logger = logging.getLogger(__name__)

# Set Snowflake Connector's log level to WARNING to reduce its verbosity
snowflake_logger = logging.getLogger("snowflake.connector")
snowflake_logger.setLevel(logging.WARNING)


# ==================================================================================================================
# Read the style css formatting information
# ==================================================================================================================
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# ==================================================================================================================
#  End read the style css formating information
# ==================================================================================================================

# ------------------------------------------------------------------------------------------------------------------

# ==================================================================================================================
# Create the sidebar place for holding client logo and setting page header
# ==================================================================================================================

st.sidebar.header("Dashboard For")


# This function sets the logo and company name inside the sidebar
def add_logo(logo_path, width, height):
    """Read and return a resized logo"""
    logo = Image.open(logo_path)
    modified_logo = logo.resize((width, height))
    return modified_logo


my_logo = add_logo(logo_path="./images/DeltaPacific_Logo.jpg", width=200, height=100)
st.sidebar.image(my_logo)
st.sidebar.subheader("Delta Pacific Beverage Co.")

# Set Page Header
st.header("Delta Pacific Beverage Chain Dashboard")
# Set custom CSS for hr element
st.markdown(
    """
        <style>
            hr {
                margin-top: 0.0rem;
                margin-bottom: 0.5rem;
                height: 3px;
                background-color: #333;
                border: none;
            }
        </style>
    """,
    unsafe_allow_html=True,
)

# Add horizontal line
st.markdown("<hr>", unsafe_allow_html=True)

# ==================================================================================================================
# End block for Create the sidebar place for holding client logo
# ==================================================================================================================

# -------------------------------------------------------------------------------------------------------------------

# ===========================================================================================================================================
# Create three columns to display  Salesperson Store count in column 1,  execution summary in column two and chain barchart in column three
# ===========================================================================================================================================

# Create a layout with three columns
col1, col2 = st.columns([50, 50], gap="medium")


# ============================================================================================================================================================
# end block for Create three columns to display  Salesperson Store count in column 1,  execution summary in column two and chain barchart in column three
# ============================================================================================================================================================

# ------------------------------------------------------------------------------------------------------------------------------------------------------------

# ============================================================================================================================================================
# 11/28/2023 - Randy Griggs - Function to create connection to the database
# ============================================================================================================================================================

# # Function to create and return a Snowflake connection object with logging
# def create_snowflake_connection():
#     try:
#         # Load Snowflake credentials from the secrets.toml file
#         snowflake_creds = st.secrets["snowflake"]

       

#         # Create a connection ID
#         connection_id = str(uuid.uuid4())

#         # Create and return a Snowflake connection object
#         conn = snowflake.connector.connect(
#             account=snowflake_creds["account"],
#             user=snowflake_creds["user"],
#             password=snowflake_creds["password"],
#             warehouse=snowflake_creds["warehouse"],
#             database=snowflake_creds["database"],
#             schema=snowflake_creds["schema"]
#         )

#         return conn, connection_id

#     except snowflake.connector.errors.Error as e:
#         st.error(f"Error creating Snowflake connection: {str(e)}")
#         # Log the error or take appropriate action
#         log_error_info(str(e), connection_id)
#         return None, None  # Return None to indicate an error

# Function to create and return a Snowflake connection object with logging
def create_snowflake_connection():
    connection_id = str(uuid.uuid4())

    try:
        creds = st.secrets["snowflake_secure"]

        # Decode the base64 private key
        private_key_bytes = base64.b64decode(creds["private_key_base64"])

       # Load and decrypt the PEM key
        p_key = serialization.load_pem_private_key(
            private_key_bytes,
            password=creds["private_key_passphrase"].encode()
        )

        private_key = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=creds["user"],
            account=creds["account"],
            private_key=private_key,
            warehouse=creds["warehouse"],
            database=creds["database"],
            schema=creds["schema"],
            role=creds["role"]
        )

        return conn, connection_id

    except Exception as e:
        st.error(f"Error creating Snowflake connection: {str(e)}")
        log_error_info(str(e), connection_id)
        return None, None



# ============================================================================================================================================================
# END of Function 11/28/2023 - Randy Griggs - Function to create connection to the database
# ============================================================================================================================================================

# ------------------------------------------------------------------------------------------------------------------------------------------------------------

# ============================================================================================================================================================
# 12/04/2023 Randy Griggs added connection logging information which gets written to the connection_log table in snowflake
# ============================================================================================================================================================

# # Log connection information to the CONNECTION_LOG table
# def log_connection_info(connection_id):
#     try:
#         conn = create_snowflake_connection()[0]  # Get connection object
#         cursor = conn.cursor()

#         # Log the connection event
#         event_time = datetime.now()
#         event_type = "Connection"
#         username = st.secrets["snowflake"]["user"]
#         query_text = "Snowflake Connection"

#         cursor.execute("""
#             INSERT INTO CONNECTION_LOG
#             (EVENT_TIME, EVENT_TYPE, CONNECTION_ID, USERNAME, QUERY_TEXT)
#             VALUES (%s, %s, %s, %s, %s)
#         """, (event_time, event_type, connection_id, username, query_text ))

#         conn.commit()
#         conn.close()

#     except snowflake.connector.errors.Error as e:
#         st.error(f"Error logging connection information: {str(e)}")


# ============================================================================================================================================================
# END 12/04/2023 Randy Griggs added connection logging information which gets written to the connection_log table in snowflake
# ============================================================================================================================================================

# ------------------------------------------------------------------------------------------------------------------------------------------------------------


# Log query information to the CONNECTION_LOG table
# def log_query_info(query, connection_id, conn):
#     try:
#         cursor = conn.cursor()

#         # Log the query event
#         event_time = datetime.now()
#         event_type = "Query"
#         username = st.secrets["snowflake"]["user"]
#         query_text = query

#         cursor.execute("""
#             INSERT INTO CONNECTION_LOG
#             (EVENT_TIME, EVENT_TYPE, CONNECTION_ID, USERNAME, QUERY_TEXT)
#             VALUES (%s, %s, %s, %s, %s)
#         """, (event_time, event_type, connection_id, username, query_text))

#         conn.commit()
#         log_connection_info(connection_id)

#         cursor.close()

#     except snowflake.connector.errors.Error as e:
#         st.error(f"Error logging query information: {str(e)}")


# ============================================================================================================================================================
# END 11/28/2023 Randy Griggs - Function will be called to handle the DB query and closing the the connection
# ============================================================================================================================================================

# ------------------------------------------------------------------------------------------------------------------------------------------------------------

# ============================================================================================================================================================
# 12/04/2023 Function to log error information to the connection_log table for troubleshooting purposes
# ============================================================================================================================================================
def log_error_info(error_message, connection_id):
    try:
        conn = create_snowflake_connection()[0]  # Get connection object
        cursor = conn.cursor()

        # Log the error event
        event_time = datetime.now()
        event_type = "Error"
        username = st.secrets["snowflake_secure"]["user"]

        cursor.execute("""
            INSERT INTO CONNECTION_LOG 
            (EVENT_TIME, EVENT_TYPE, CONNECTION_ID, USERNAME, ERROR_MESSAGE)
            VALUES (%s, %s, %s, %s, %s)
        """, (event_time, event_type, connection_id, username, error_message))

        conn.commit()
        conn.close()

    except snowflake.connector.errors.Error as e:
        st.error(f"Error logging error information: {str(e)}")


# ============================================================================================================================================================
# END 12/04/2023 Function to log error information to the connection_log table for troubleshooting purposes
# ============================================================================================================================================================

# -----------------------------------------------------------------------------------------------------------------------------------------------------------

# ============================================================================================================================================================
# 11/28/2023 Randy Griggs - Function will be called to handle the DB query and closing the the connection and return the results to the calling function
# ============================================================================================================================================================

# Function to execute a query and close the connection with logging
def execute_query_and_close_connection(query, conn, connection_id, parameters=None):
    try:
        cursor = conn.cursor()

        # Log the query event
        # log_query_info(query, connection_id, conn)

        if parameters:
            cursor.execute(query, tuple(parameters))
        else:
            cursor.execute(query)

        # Fetch the result
        result = cursor.fetchall()

        # Close the connection
        conn.close()

        return result

    except snowflake.connector.errors.Error as e:
        st.error(f"Error executing query: {str(e)}")
        # Log the error
        log_error_info(str(e), connection_id)
        # Take appropriate action if needed
        return None  # Return None to indicate an error
    except Exception as e:
        st.error(f"An unexpected error occurred: {str(e)}")
        # Log the error
        log_error_info(str(e), connection_id)
        # Take appropriate action if needed
        return None  # Return None to indicate an error


# ============================================================================================================================================================
# END 11/28/2023 Randy Griggs - Function will be called to handle the DB query and closing the the connection
# ============================================================================================================================================================

# -----------------------------------------------------------------------------------------------------------------------------------------------------------

# ============================================================================================================================================================
# 12/3/2023 below block with create the db connection and run the stored procedure process_execution_summary() which builds the tables necessary to
# Populate the chain bar graph on the home page
# ============================================================================================================================================================
# Create a connection
conn, connection_id = create_snowflake_connection()

# Run the specific query for building tables (e.g., calling a stored procedure) only on page load
initial_query = "CALL process_execution_summary()"
execute_query_and_close_connection(initial_query, conn, connection_id)


# ============================================================================================================================================================
# END 12/3/2023 below block with create the db connection and run the stored procedure process_execution_summary() which builds the tables necessay to
# Populate the chain bar graph on the home page
# ============================================================================================================================================================

# ------------------------------------------------------------------------------------------------------------------------------------------------------------

# ===========================================================================================================================================
# Function that will connect to DB and pull data to display the Execution Summary Data in Row 1 column 1
# ===========================================================================================================================================

## Function to calculate and return results to calling code execution summary
def display_execution_summary():
    # Your query
    query = "SELECT SUM(\"In_Schematic\") AS total_in_schematic, SUM(\"PURCHASED_YES_NO\") AS purchased, SUM(\"PURCHASED_YES_NO\") / COUNT(*) AS purchased_percentage FROM GAP_REPORT;"
    
    # Create a connection
    conn, connection_id = create_snowflake_connection()

    
    # Execute the query and get the result
    result = execute_query_and_close_connection(query, conn, connection_id)
    #st.write(result)
    # Process the result as neededr
    df = pd.DataFrame(result, columns=["TOTAL_IN_SCHEMATIC", "PURCHASED", "PURCHASED_PERCENTAGE"])

    total_gaps = df['TOTAL_IN_SCHEMATIC'].iloc[0] - df['PURCHASED'].iloc[0]
    purchased_percentage = float(df['PURCHASED_PERCENTAGE'].iloc[0])
    formatted_percentage = f"{purchased_percentage * 100:.2f}%"

    # Sort the DataFrame by the "total_gaps" column in descending order
    # df = df.sort_values(by="TOTAL_IN_SCHEMATIC", ascending=False)

    return df['TOTAL_IN_SCHEMATIC'].iloc[0], df['PURCHASED'].iloc[0], total_gaps, formatted_percentage
# ===========================================================================================================================================
# End Block for Function that will connect to DB and pull data to display the Execution Summary Data in Row 1 column 1
# ===========================================================================================================================================

# -------------------------------------------------------------------------------------------------------------------------------------------


# -------------------------------------------------------------------------------------------------------------------------------------------

# ===========================================================================================================================================
# Block for Function that will connect to DB and pull data to display the the bar chart from view - Execution Summary  - Data in row 1 column 2
# ===========================================================================================================================================

# Function to fetch data for the bar chart
def fetch_chain_schematic_data():
    # Load Snowflake credentials from the secrets.toml file

    # Fetch data for the bar chart (modify the query to match your view)
    query = "SELECT CHAIN_NAME, SUM(\"In_Schematic\") AS total_in_schematic, SUM(\"PURCHASED_YES_NO\") AS purchased, SUM(\"PURCHASED_YES_NO\") / COUNT(*) AS purchased_percentage FROM gap_report GROUP BY CHAIN_NAME;"

    # Create a connection
    conn, connection_id = create_snowflake_connection()

    # Execute the query and get the result
    result = execute_query_and_close_connection(query, conn, connection_id)

    # Convert the result to a DataFrame
    df = pd.DataFrame(result, columns=["CHAIN_NAME", "TOTAL_IN_SCHEMATIC", "PURCHASED", "PURCHASED_PERCENTAGE"])

    # Ensure 'PURCHASED_PERCENTAGE' is treated as a numeric (float) type
    df['PURCHASED_PERCENTAGE'] = df['PURCHASED_PERCENTAGE'].astype(float)

    # Perform rounding
    df['PURCHASED_PERCENTAGE'] = (df['PURCHASED_PERCENTAGE'] * 100).round(2).astype(str) + '%'
    # st.write(df)
    # Close the connection
    conn.close()

    return df


# ===========================================================================================================================================
# END Block for Function that will connect to DB and pull data to display the the bar chart from view - Execution Summary  - Data in column 3
# ===========================================================================================================================================

# ===============================================================================================================================================
# Function to pull supplier data to populate sidebar dropdown
# ===============================================================================================================================================
# Fetch supplier names from the supplier_county table
def fetch_supplier_names():
    query = "SELECT DISTINCT supplier FROM supplier_county order by supplier"

    # Create a connection
    conn, connection_id = create_snowflake_connection()

    # Execute the query and get the result
    result = execute_query_and_close_connection(query, conn, connection_id)

    supplier_names = [row[0] for row in result]

    return supplier_names


# ===============================================================================================================================================
# End Function to pull supplier data to populate sidebar dropdown
# ===============================================================================================================================================


# ----------------------------------------------------------------------------------------------------------------------------------

# =================================================================================================================================================
# Function to pull data to populate the product by supplier Supplier Bar chart once suppliers have been selected
# ================================================================================================================================================


# Fetch schematic summary data for selected suppliers
def fetch_supplier_schematic_barchart_data(selected_suppliers):
    supplier_conditions = ", ".join([f"'{supplier}'" for supplier in selected_suppliers])

    query = f"""
          SELECT 
    SUPPLIER AS Supplier_Name,
    SUM("In_Schematic") AS Total_In_Schematic,
    SUM(PURCHASED_YES_NO) AS Total_Purchased,
    (SUM(PURCHASED_YES_NO) / SUM("In_Schematic")) * 100 AS Purchased_Percentage
FROM
    GAP_REPORT_TMP2
WHERE
    "sc_STATUS" = 'Yes' AND supplier IN ({supplier_conditions})
GROUP BY
    SUPPLIER
    ORDER BY Purchased_Percentage ASC;
    """
    # Create a connection
    conn, connection_id = create_snowflake_connection()

    # Execute the query and get the result
    result = execute_query_and_close_connection(query, conn, connection_id)
    df = pd.DataFrame(result, columns=["Supplier_Name", "Total_In_Schematic", "Purchased", "Purchased_Percentage"])

    # Format the Purchased Percentage column as percentage with two decimal places
    df["Purchased_Percentage"] = df["Purchased_Percentage"].apply(lambda x: f"{float(x):.2f}%")
   

    return df


# =================================================================================================================================================
# End Function to pull data to populate the product by supplier chart once suppliers have been selected
# ================================================================================================================================================

# -------------------------------------------------------------------------------------------------------------------------------------------------

# ================================================================================================================================================
# Function to pull product by supplier scatter chart once the supplier have been selected from the sidebar selection widget
# =================================================================================================================================================

# Fetch schematic summary data for selected suppliers
def fetch_supplier_schematic_summary_data(selected_suppliers):
    supplier_conditions = ", ".join([f"'{supplier}'" for supplier in selected_suppliers])

    query = f"""
    SELECT 
    PRODUCT_NAME,
    "dg_upc" AS UPC,
    SUM("In_Schematic") AS Total_In_Schematic,
    SUM(PURCHASED_YES_NO) AS Total_Purchased,
    (SUM(PURCHASED_YES_NO) / SUM("In_Schematic")) * 100 AS Purchased_Percentage
FROM
    GAP_REPORT_TMP2
WHERE
    "sc_STATUS" = 'Yes' AND SUPPLIER IN ({supplier_conditions})
GROUP BY
    SUPPLIER, PRODUCT_NAME, "dg_upc"
    ORDER BY Purchased_Percentage ASC;

    """
    # Create a connection
    conn, connection_id = create_snowflake_connection()

    # Execute the query and get the result
    result = execute_query_and_close_connection(query, conn, connection_id)

    df = pd.DataFrame(result,
                      columns=["PRODUCT_NAME", "UPC", "Total_In_Schematic", "Total_Purchased", "Purchased_Percentage"])

    # Print connection status
    # print(f"Connection Status: FALSE = Open and TRUE = Closed: {conn.is_closed()}")

    return df


# ================================================================================================================================================
# End Function to pull product by supplier scatter chart once the supplier have been selected from the sidebar selection widget
# =================================================================================================================================================


# -----------------------------------------------------------------------------------------------------------------------------------------------


# -----------------------------------------------------------------------------------------------------------------------------------------------

# ===========================================================================================================================================
# Call display_execution_summary() to get the execution summary data and display it for the user in row 1 column 1
# ===========================================================================================================================================

# Fetch the data from the function
total_in_schematic, total_purchased, total_gaps, formatted_percentage = display_execution_summary()
Revenue_missed = total_gaps * 40.19
# Display the values in col1
with col1:
    # Add styled title above the content in the second column
    col1.markdown("<h1 style='text-align: center; font-size: 18px;'>Execution Summary</h1>", unsafe_allow_html=True)
    border_color = st.get_option("theme.secondaryBackgroundColor")
    # Use st.markdown with HTML and CSS to create a styled container
    col1.markdown(
        f"""
        <div style='
            background-color:#EEEEEE;   /*#D9D9D6*/
            secondaydayBackgroundColor: #ff0000;
            padding: 50px;
            border-radius: 10px;
            box-shadow: 0 0.10rem 1.75rem 0 rgba(58, 59, 69, 0.15);
            text-align: right;
            border:border_color;  /* Add dark grey border */
            height: 60vh;  /* Set a minimum height */'>
            <p> Execution Summary<p>
            <p>Total In Schematic: {total_in_schematic}</p>
            <p>Total Purchased: {total_purchased}</p>
            <p>Total Gaps: {total_gaps}</p>
            <p>Overall Purchased Percentage: {formatted_percentage}</p>
            <p>Overall Missed Revenue: ${Revenue_missed: .2f}</p>

        </div>
        """,
        unsafe_allow_html=True
    )
   

# ===========================================================================================================================================
# End block to Call display_execution_summary() to get the execution summary data and display it for the user in column 2
# ===========================================================================================================================================

# -------------------------------------------------------------------------------------------------------------------------------------------

# ===============================================================================================================================================
# Call function fetch_chain_schematic_data() to get data for bar chart and display it in column 3
# ===============================================================================================================================================
# Fetch chain schematic data
chain_schematic_data = fetch_chain_schematic_data()

# Create a bar chart using Altair with percentage labels on bars
bar_chart = alt.Chart(chain_schematic_data).mark_bar().encode(
    x='CHAIN_NAME',
    y='TOTAL_IN_SCHEMATIC',
    color=alt.Color('CHAIN_NAME', scale=alt.Scale(scheme='viridis')),
    # color=alt.Color('PURCHASED_PERCENTAGE', scale=alt.Scale(scheme='viridis')),
    tooltip=['CHAIN_NAME', 'TOTAL_IN_SCHEMATIC', 'PURCHASED', 'PURCHASED_PERCENTAGE']
).properties(
    width=800,
    height=400,
).configure_title(
    align='center',
    fontSize=16
).encode(
    # text=alt.Text('PURCHASED_PERCENTAGE:Q', format='.2f')
    text=alt.Text('CHAIN_NAME')
).configure_mark(
    fontSize=14
)

# Display the bar chart in the third column
col2.altair_chart(bar_chart, use_container_width=False)

# ===============================================================================================================================================
# END Call function fetch_chain_schematic_data() to get data for bar chart and display it in column 3
# ===============================================================================================================================================

# ----------------------------------------------------------------------------------------------------------------------------------------------

# ===================================================================================================================================================
# Add columns in row 2 of the page
# ===================================================================================================================================================

# Add a new row with columns 1 and 2
row2_col1, row2_col2 = st.columns([30, 70], gap="small")

# ===================================================================================================================================================
# END Add columns in row 2 of the page
# ===================================================================================================================================================

# ----------------------------------------------------------------------------------------------------------------------------------------------


# ===============================================================================================================================================
# This block will call salesperson data from view and display the salesperson, total_distribution, total_gaps, and Execution_percentage Row 1 Col1
# ===============================================================================================================================================

# Execute the SQL query to retrieve the salesperson's store count
query = "SELECT SALESPERSON, TOTAL_DISTRIBUTION, TOTAL_GAPS, EXECUTION_PERCENTAGE FROM SALESPERSON_EXECUTION_SUMMARY order by TOTAL_GAPS DESC"

# Create a connection
conn, connection_id = create_snowflake_connection()

# # Print connection status
# print(f"Connection Status: FALSE = Open and TRUE = Closed: {conn.is_closed()}")

# Execute the query and get the result
result = execute_query_and_close_connection(query, conn, connection_id)

# # Print connection status
# print(f"Connection Status: FALSE = Open and TRUE = Closed: {conn.is_closed()}")


# Create a DataFrame from the query results
salesperson_df = pd.DataFrame(result,
                              columns=['SALESPERSON', 'TOTAL_DISTRIBUTION', 'TOTAL_GAPS', 'EXECUTION_PERCENTAGE'])

# Convert the 'EXECUTION_PERCENTAGE' column to float before rounding
salesperson_df['EXECUTION_PERCENTAGE'] = salesperson_df['EXECUTION_PERCENTAGE'].astype(float)

# Round the 'EXECUTION_PERCENTAGE' column to 2 decimal places
salesperson_df['EXECUTION_PERCENTAGE'] = salesperson_df['EXECUTION_PERCENTAGE'].round(2)

# Rename the columns
salesperson_df = salesperson_df.rename(
    columns={'SALESPERSON': 'Salesperson', 'TOTAL_DISTRIBUTION': 'Distribution', 'TOTAL_GAPS': 'Gaps',
             'EXECUTION_PERCENTAGE': 'Execution Percentage'})

# Limit the number of displayed rows to 6
limited_salesperson_df = salesperson_df.head(100)

# Apply bold styling to each cell in the 'Salesperson' column
limited_salesperson_df_html = limited_salesperson_df.to_html(classes=["table", "table-striped"], escape=False,
                                                             index=False)
for index, row in limited_salesperson_df.iterrows():
    limited_salesperson_df_html = limited_salesperson_df_html.replace(f'<td>{row["Salesperson"]}</td>',
                                                                      f'<td style="font-weight: bold;">{row["Salesperson"]}</td>')

# Define the maximum height for the table container
max_height = '365px'

# Adjust the width of the table by changing the 'width' property
table_style = f"max-height: {max_height}; overflow-y: auto; background-color: #EEEEEE;  text-align: center; padding: 1% 2% 2% 0%; border-radius: 10px; border-left: 0.5rem solid #9AD8E1 !important; box-shadow: 0 0.10rem 1.75rem 0 rgba(58, 59, 69, 0.15) !important; width: 100%;"

# Wrap the table in an HTML div with the specified style
table_with_scroll = f"<div style='{table_style}'><table style='table-layout, 'text-align: left',auto;'><colgroup><col style='width: 20%;'><col style='width: 10%;'><col style='width: 30%;'></colgroup>{limited_salesperson_df_html}</table></div>"

# Display the table in col1 with custom formatting
with row2_col1:
    # Display the table with custom formatting
    st.markdown(table_with_scroll, unsafe_allow_html=True)
    # Add a download link for the Excel file
    excel_data = BytesIO()
    salesperson_df.to_excel(excel_data, index=False)
    excel_data.seek(0)
    st.download_button(label="Download Excel", data=excel_data, file_name="salesperson_execution_summary.xlsx",
                       key='download_button')


# ==================================================================================================================================================
# End  This block will call salesperson data from view and display the salesperson, total_distribution, total_gaps, and Execution_percentage
# ==================================================================================================================================================

# ---------------------------------------------------------------------------------------------------------------------------------------------------

# ============================================================================================================================================================
# Function to check if todays privot table data has processed.  If so will give user option to overwrite the data and if not the procedure BUILD_GAP_TRACKING()
# Procedure will update the table SALESPERSON_EXECUTION_SUMMARY_TBL with todays data
# ============================================================================================================================================================

# Function to check and process data
def check_and_process_data():
    # Create a connection to Snowflake
    conn, connection_id = create_snowflake_connection()

    # Create a cursor object
    cursor = conn.cursor()

    try:
        # Check if data already processed for today
        check_query = f"SELECT COUNT(*) FROM SALESPERSON_EXECUTION_SUMMARY_TBL WHERE LOG_DATE = CURRENT_DATE()"
        cursor.execute(check_query)
        result = cursor.fetchone()

        if result[0] > 0:
            # Data already processed for today, ask if they want to overwrite
            st.warning("Data for today already processed. Do you want to overwrite it?")

            # Add "Yes" and "No" buttons
            yes_button = st.button("Yes, overwrite")
            no_button = st.button("No, keep existing data")

            if yes_button:
                # If yes, remove data for today
                delete_query = f"DELETE FROM SALESPERSON_EXECUTION_SUMMARY_TBL WHERE LOG_DATE = CURRENT_DATE()"
                cursor.execute(delete_query)

                # Call the stored procedure to update the table with new data
                build_gap_tracking_query = "CALL BUILD_GAP_TRACKING()"
                cursor.execute(build_gap_tracking_query)

                st.success("Data overwritten and BUILD_GAP_TRACKING() executed successfully.")

            elif no_button:
                # If no, do nothing
                st.info("Data not overwritten.")

        else:
            # No data for today, proceed with the stored procedure
            build_gap_tracking_query = "CALL BUILD_GAP_TRACKING()"
            cursor.execute(build_gap_tracking_query)

            st.success("BUILD_GAP_TRACKING() executed successfully.")

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()


# ============================================================================================================================================================
# END Function to check if todays privot table data has processed.  If so will give user option to overwrite the data and if not the procedure BUILD_GAP_TRACKING()
# Procedure will update the table SALESPERSON_EXECUTION_SUMMARY_TBL with todays data
# ============================================================================================================================================================


# ===================================================================================================================================================
# Add Pivot table in col 2 row 2 to show salesperson, gaps by data to see progress against gaps over time
# ===================================================================================================================================================
# Execute the SQL query to retrieve the salesperson's store count
query = "SELECT SALESPERSON, TOTAL_GAPS, EXECUTION_PERCENTAGE, LOG_DATE FROM SALESPERSON_EXECUTION_SUMMARY_TBL ORDER BY TOTAL_GAPS DESC"

# Create a connection
conn, connection_id = create_snowflake_connection()

# Execute the query and get the result
result = execute_query_and_close_connection(query, conn, connection_id)

# Create a DataFrame from the query results
gap_df = pd.DataFrame(result, columns=['SALESPERSON', 'TOTAL_GAPS', 'EXECUTION_PERCENTAGE', 'LOG_DATE'])

# Rename the columns
gap_df = gap_df.rename(
    columns={'SALESPERSON': 'Salesperson', 'TOTAL_GAPS': 'Gaps', 'EXECUTION_PERCENTAGE': 'Execution Percentage',
             'LOG_DATE': 'Log Date'})

# Limit the number of displayed rows to 100
limited_gap_df = gap_df.head(100)

# Create the pivot table
gap_df_pivot = gap_df.pivot_table(index=['Salesperson'], columns=['Log Date'], values='Gaps', margins=False)

# Sort the DataFrame by the date column in descending order
gap_df_sorted = gap_df.sort_values(by='Log Date', axis=0, ascending=False)

# Extract the latest 12 columns
latest_columns = gap_df_sorted['Log Date'].unique()[:12]

# Reorder the DataFrame to display the latest columns
gap_df_pivot_limited = gap_df_pivot[latest_columns]

# Convert the column names to DateTime objects and format them
gap_df_pivot_limited.columns = pd.to_datetime(gap_df_pivot_limited.columns).strftime('%y/%m/%d')

# Define the maximum height for the table container
max_height = '365px'

# Adjust the width of the table by changing the 'width' property
table_style = f"max-height: {max_height}; overflow-y: auto; background-color: #EEEEEE; text-align: center; padding: 1% 2% 2% 0%; border-radius: 10px; border-left: 0.5rem solid #9AD8E1 !important; box-shadow: 0 0.10rem 1.75rem 0 rgba(58, 59, 69, 0.15) !important; width: 100%;"

# Apply a smaller font size to the 'Log Date' column
table_html = gap_df_pivot_limited.to_html(classes=["table", "table-striped"], escape=False, render_links=True)

# Add custom style for the 'Log Date' column to reduce font size
table_html = table_html.replace('<th>Log Date</th>', '<th style="font-size: smaller;">Log Date</th>')

# Create colgroup HTML tag with col tags for each column width
colgroup_html = ''.join([f"<col style='width: {100 / len(latest_columns)}%;'>" for _ in latest_columns])

# Add style to the table tag to allow automatic column width adjustment
table_with_scroll = f"<div style='{table_style}'><table style='table-layout, text-align: left, auto;'><colgroup>{colgroup_html}</colgroup>{table_html}</table></div>"

# Display the table in col2 row 2 with custom formatting
with row2_col2:
    # Display the table with custom formatting
    st.markdown(table_with_scroll, unsafe_allow_html=True)

    # Add a download link for the Excel file
    excel_data = BytesIO()
    gap_df_pivot_limited.to_excel(excel_data, index=True)
    excel_data.seek(0)
    st.download_button(label="Download Excel", data=excel_data, file_name="gap_history_report.xlsx",
                       key='download_gap_button')

# ===================================================================================================================================================
# END Add Pivot table in col 2 row 2 to show salesperson, gaps by data to see progress against gaps over time
# ===================================================================================================================================================

# ---------------------------------------------------------------------------------------------------------------------------------------------------


# ==================================================================================================================================================
# This Block of codes creates the sidebar multi select widget for selecting suppliers then calls function to get supplier data then display it in
# the barchart for each supplier.  Additonally it calls the function to get the data for the supplier to populate the scatter chart for each product
# for the selected supplier
# ====================================================================================================================================================


# Create a sidebar select widget for selecting suppliers
selected_suppliers = st.sidebar.multiselect("Select Suppliers", fetch_supplier_names())

# ===================================================================================================================================================
# Add columns in row 2 of the page
# ===================================================================================================================================================

# Add a new row with columns 1 and 2
row3_col1 = st.columns([100], gap="small")

# ===================================================================================================================================================
# END Add columns in row 2 of the page
# ===================================================================================================================================================

# ----------------------------------------------------------------------------------------------------------------------------------------------

# ==================================================================================================================================================
# Creates barchart for supplier execution in total in schematic, total purchased and percent purchased against total in schematic
# =================================================================================================================================================
# with row3_col1:
# Add centered and styled title above the bar chart
st.markdown("<h1 style='text-align: center; font-size: 18px;'>Execution Summary by Supplier</h1>",
            unsafe_allow_html=True)
# Fetch supplier schematic summary data for selected suppliers if there are any
supplier_schematic_summary_data = None
if selected_suppliers:
    supplier_schematic_summary_data = fetch_supplier_schematic_barchart_data(selected_suppliers)

# Display the bar chart if there is data
if supplier_schematic_summary_data is not None:
    # Create a bar chart using Altair
    supplier_bar_chart = alt.Chart(supplier_schematic_summary_data).mark_bar().encode(
        x='Supplier_Name',
        y='Total_In_Schematic',
        color=alt.Color('Purchased_Percentage', scale=alt.Scale(scheme='viridis')),
        tooltip=['Supplier_Name', 'Total_In_Schematic', 'Purchased', 'Purchased_Percentage']
    ).interactive()

    # Display the supplier bar chart
    st.altair_chart(supplier_bar_chart, use_container_width=True)
else:
    # If supplier_schematic_summary_data is None, display a message
    st.write("Please select one or more suppliers to view the chart")

# ==================================================================================================================================================
# END Creates barchart for supplier execution in total in schematic, total purchased and percent purchased against total in schematic
# =================================================================================================================================================

# ---------------------------------------------------------------------------------------------------------------------------------------------------

# =================================================================================================================================================
# Creates scatter chart for product execution by supplier
# =================================================================================================================================================

# Add centered and styled title above the scatter chart
st.markdown("<h1 style='text-align: center; font-size: 18px;'>Execution Summary by Product by Supplier</h1>",
            unsafe_allow_html=True)

# Fetch schematic summary data for selected suppliers if there are any


# Fetch supplier schematic summary data for selected suppliers if there are any
supplier_schematic_summary_data = None
if selected_suppliers:
    df = fetch_supplier_schematic_summary_data(selected_suppliers)
    # Format the Purchased Percentage column as percentage with two decimal places
    # data_types = df.dtypes
    # st.write(data_types)

    # Remove the percentage symbol and convert to float
    # df['Purchased_Percentage'] = df['Purchased_Percentage'].str.replace('', '').astype(float)

    df["Purchased_Percentage"] = df["Purchased_Percentage"].astype(float)

    df["Purchased_Percentage_Display"] = df["Purchased_Percentage"].astype(float) / 100

    # Display the scatter chart if there is data
    if df is not None:

        # data_types = df.dtypes
        # st.write(data_types)
        # Create a scatter chart using Altair
        # Create a scatter chart using Altair with
        scatter_chart = alt.Chart(df).mark_circle().encode(
            x='Total_In_Schematic',
            y='Purchased_Percentage:Q',
            color='PRODUCT_NAME',
            tooltip=[
                'PRODUCT_NAME', 'UPC', 'Total_In_Schematic', 'Total_Purchased',
                alt.Tooltip('Purchased_Percentage_Display:Q', format='.2%'),  # Format this specific field
            ]
        ).interactive()

        # Display the supplier bar chart
        st.altair_chart(scatter_chart, use_container_width=True)
        # st.write(df)
    else:
        # If supplier_schematic_summary_data is None, display a message
        st.write("Please select one or more suppliers to view the chart")

# =================================================================================================================================================
# END Creates scatter chart for product execution by supplier
# =================================================================================================================================================


with row2_col2:
    # call the function in check_and_process_data in Streamlit
    if st.button("Process Gap Pivot Data", key='process_gap_pivot'):
        check_and_process_data()


# def fetch_top_products_by_store():
#     query = "SELECT * FROM TOP_SELLING_PRODUCTS"
    
#     # Create a connection
#     conn, connection_id = create_snowflake_connection()
    
#     try:
#         # Open a cursor, execute the query and fetch the results
#         cur = conn.cursor()
#         cur.execute(query)
#         result = cur.fetchall()  # Fetch all results
#     finally:
#         cur.close()  # Close the cursor
#         conn.close()  # Close the connection

#     return result



# import matplotlib.pyplot as plt



# def plot_results(results):
#     stores = [result[1] for result in results]  # Assuming the second element is sales figure
#     products = [result[0] for result in results]  # Assuming the first element is product name
    
    
#     plt.figure(figsize=(10, 6))
#     plt.bar(products, stores, color='skyblue')
#     plt.ylabel('Products')
#     plt.xlabel('Stores')
#     plt.title('Top Selling Products by Store')
#     plt.xticks(rotation=45)
    
#     # Display the plot in the Streamlit app
#     st.pyplot(plt)

# # Assuming fetch_top_products_by_store() is defined elsewhere and returns data correctly
# top_products = fetch_top_products_by_store()

# # Using Streamlit to layout and display the plot
# st.title('Sales Data Visualization')
# row4_col1 = st.columns(1)  # Creates a single column layout

# with row4_col1[0]:  # Using the first (and only) column in the layout
#     plot_results(top_products)