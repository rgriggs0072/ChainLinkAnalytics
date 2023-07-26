import ipaddress
#from msilib import schema
import streamlit as st
import snowflake.connector
import numpy as np
import getpass
import socket
from datetime import datetime
import pandas as pd


def current_timestamp():
    return datetime.now()


#====================================================================================================================
# CREATE CONNECTION TO SNOWFLAKE
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

#--------------------------------------------------------------------------------------------------------------------

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

#====================================================================================================================
# Function to remove data from Distro_Grid table and insert into backup table. After the distro_grid table
# has been cleared of the data for that chain, insert the new grid data into the distro_grid table for that chain
#======================================================================================================================



def upload_SAFEWAY_distro_grid_to_snowflake(df, schema, table_name, selected_option):
    try:
        
        st.write(" they match", selected_option)
        

         ## Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)
        # Convert the "upc" column to numpy int64 data type, which supports larger integers
        df['UPC'] = df['UPC'].astype(np.int64)


        # Fill missing and non-numeric values in the "SKU" column with zeros
        df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)

        # Convert the "SKU" column to np.int64 data type, which supports larger integers
        df['SKU'] = df['SKU'].astype(np.int64)

        # Print the DataFrame before insertion
        print("DataFrame to be inserted:")
        print(df)

        # Generate the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
        INSERT INTO {schema}.{table_name} (
            {', '.join(df.columns)}
        )
        VALUES ({placeholders})
        """

        

        # Chunk the DataFrame into smaller batches
        chunk_size = 1000  # Adjust the chunk size as per your needs
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Execute the query with parameterized values for each chunk
        cursor = conn.cursor()
        for chunk in chunks:
            cursor.executemany(insert_query, chunk.values.tolist())
        cursor.close()

        # Close the connection to Snowflake
        conn.close()

        st.write("Data has been imported into Snowflake table:", table_name)
        

    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while uploading data to Snowflake: {str(e)}")

