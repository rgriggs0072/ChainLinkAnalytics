import ipaddress
from re import S
import streamlit as st
import snowflake.connector
import numpy as np
import getpass
import socket
from datetime import datetime
import pandas as pd
from datetime import date
from Home import create_snowflake_connection



# Set page to wide display to give more room
st.set_page_config(
    layout="wide",
    initial_sidebar_state="collapsed")
padding_top = 0



def current_timestamp():
    return datetime.now()



#=====================================================================================================================
# Function to get current date and time for log entry
#=====================================================================================================================
def current_timestamp():
    return datetime.now()

#=====================================================================================================================
# End Function to get current date and time for log entry
#=====================================================================================================================

#----------------------------------------------------------------------------------------------------------------------

#====================================================================================================================

# Function to insert Activity to the log table

#====================================================================================================================


def insert_log_entry(user_id, activity_type, description, success, ip_address, selected_option):
    try:
        conn = create_snowflake_connection()[0]  # Get connection object
        cursor = conn.cursor()
        
        # Replace 'LOG' with the actual name of your log table
        insert_query = """
        INSERT INTO LOG (TIMESTAMP, USERID, ACTIVITYTYPE, DESCRIPTION, SUCCESS, IPADDRESS, USERAGENT)
        VALUES (CURRENT_TIMESTAMP(), %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (user_id, "SQL Activity", description, True, ip_address, selected_option))

        cursor.close()
    except Exception as e:
        # Handle any exceptions that might occur while logging
        print(f"Error occurred while inserting log entry: {str(e)}")

#====================================================================================================================
# Function to insert Activity to the log table
#====================================================================================================================

#--------------------------------------------------------------------------------------------------------------------

#====================================================================================================================
# Function to get IP address of the user carring out the activity
#====================================================================================================================

def get_local_ip():
    try:
        # Get the local host name
        host_name = socket.gethostname()
        
        # Get the IP address associated with the host name
        ip_address = socket.gethostbyname(host_name)
        
        return ip_address
    except Exception as e:
        print(f"An error occurred while getting the IP address: {e}")
        return None

 #====================================================================================================================
# End Function to get IP address of the user carring out the activity
#====================================================================================================================

#--------------------------------------------------------------------------------------------------------------------





def update_spinner(message):
    st.text(f"{message} ...")




def load_data_into_distro_grid(conn, df, selected_option):
    user_id = getpass.getuser()
    local_ip = get_local_ip()
    
    # Log the start of the SQL activity
    description = f"Started {selected_option} Loading data into the Distro_Grid Table"
    insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
    
    # Generate the SQL query for loading data into the Distribution Grid table
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_query = f"""
        INSERT INTO Distro_Grid (
            {', '.join(df.columns)}
        )
        VALUES ({placeholders})
    """
    
    # Create a cursor object
    cursor = conn.cursor()
    
    # Chunk the DataFrame into smaller batches
    chunk_size = 5000  # Adjust the chunk size as per your needs
    chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]
    
    # Execute the query with parameterized values for each chunk
    for chunk in chunks:
        cursor.executemany(insert_query, chunk.values.tolist())
    
    # Log the start of the SQL activity
    description = f"Completed {selected_option} Loading data into the Distro_Grid Table"
    insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
    


def call_procedure(conn):
    try:
        # Call the procedure
        cursor = conn.cursor()
        cursor.execute("CALL UPDATE_DISTRO_GRID()")
        
        # Fetch and print the result
        result = cursor.fetchone()
        print(result[0])  # Output: Update completed successfully.
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")
    finally:
        # Close the cursor and the connection to Snowflake
        cursor.close()
        conn.close()


def upload_distro_grid_to_snowflake(df, selected_option, update_spinner_callback):
    conn = create_snowflake_connection()[0]  # Get connection object
    
    # Replace 'NAN' values with NULL
    df = df.replace('NAN', np.nan).fillna(value='', method=None)
    
    
    # Remove 'S' from the end of UPC if it exists
    df['UPC'] = df['UPC'].apply(lambda x: str(x)[:-1] if str(x).endswith('S') else x)


   

    # Convert 'UPC' column to np.int64
    df['UPC'] = df['UPC'].astype(np.int64)
    
    # Fill missing and non-numeric values in the "SKU" column with zeros
    df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)
    
    # Convert the "SKU" column to np.int64 data type, which supports larger integers
    df['SKU'] = df['SKU'].astype(np.int64)
    
    

    # Log the start of the SQL activity
    user_id = getpass.getuser()
    local_ip = get_local_ip()
    description = f"Started {selected_option} Start Archive Process for distro_grid table"
    insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
    

    # Update spinner message for archive completion
    update_spinner_callback(f"Starting {selected_option} Archive Process")
    
   
    
   # Update spinner message for data loading completion
    update_spinner_callback(f"Started Loading New Data into Distro_Grid Table for {selected_option}")
    
    # Load new data into distro_grid table
    load_data_into_distro_grid(conn, df, selected_option)
    
    # Update spinner message for data loading completion
    update_spinner_callback(f"Completed {selected_option} Loading Data into Distro_Grid Table")
    
    update_spinner_callback(f"Starting Final Update to the Distro Grid for {selected_option}")
    
    # Call procedure to update the distro Grid table with county and update the manufacturer and the product name
    call_procedure(conn)

  
    
    # Update spinner message for procedure completion
    update_spinner_callback(f"Completed Final {selected_option} Update Procedure")
    st.write("Data has been imported into the Distrobution Grid table")
