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


def archive_data(conn, selected_option, data_to_archive):
    if data_to_archive:
        current_date = date.today().isoformat()
        placeholders = ', '.join(['%s'] * (len(data_to_archive[0]) + 1))
        insert_query = f"""
            INSERT INTO DISTRO_GRID_ARCHIVE (
                STORE_NAME, STORE_NUMBER, UPC, SKU, PRODUCT_NAME, 
                MANUFACTURER, SEGMENT, YES_NO, ACTIVATION_STATUS, 
                COUNTY, CHAIN_NAME, ARCHIVE_DATE
            )
            VALUES ({placeholders})
        """
        
        # Add current_date to each row in data_to_archive
        data_to_archive_with_date = [row + (current_date,) for row in data_to_archive]
        
        # Chunk the data into smaller batches
        chunk_size = 5000
        chunks = [data_to_archive_with_date[i:i + chunk_size] for i in range(0, len(data_to_archive_with_date), chunk_size)]
        
        # Execute the query with parameterized values for each chunk
        cursor_archive = conn.cursor()
        for chunk in chunks:
            cursor_archive.executemany(insert_query, chunk)
        cursor_archive.close()


def remove_archived_records(conn, selected_option):
    cursor_to_remove = conn.cursor()
    delete_query = "DELETE FROM DISTRO_GRID WHERE STORE_NAME = %s"
    
    # Execute the delete query with the selected option (store_name)
    cursor_to_remove.execute(delete_query, (selected_option,))
    
    # Commit the delete operation
    conn.commit()
    cursor_to_remove.close()


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
    
    # Convert 'UPC' column to lowercase and remove trailing 's'
    df['UPC_original'] = df['UPC']  # Save original UPC values for comparison
    df['UPC'] = df['UPC'].str.lower().str.rstrip('s')

    # Print the rows where 'UPC' has changed
    changed_upc = df[df['UPC_original'] != df['UPC']]
    print("Rows where 'UPC' had 's' suffix removed:")
    print(changed_upc[['UPC_original', 'UPC']])
    
    
    # Convert the "upc" column to numpy int64 data type, which supports larger integers
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
    
    # Step 1: Fetch data for archiving
    cursor_archive = conn.cursor()
    cursor_archive.execute("SELECT * FROM DISTRO_GRID WHERE STORE_NAME = %s", (selected_option,))
    data_to_archive = cursor_archive.fetchall()
    
    # Step 2: Archive data
    archive_data(conn, selected_option, data_to_archive)
    
    # Update spinner message for archive completion
    update_spinner_callback(f"Completed {selected_option} Archive Process")
    
    # Step 3: Remove archived records from distro_grid table
    remove_archived_records(conn, selected_option)
    
    # Update spinner message for removal completion
    update_spinner_callback(f"Completed {selected_option} Removal of Archive Records")
    
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
    st.write("Data has been imported into Snowflake table: Distro_Grid")