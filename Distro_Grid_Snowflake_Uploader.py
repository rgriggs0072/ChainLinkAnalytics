import ipaddress
#from tkinter import SEL
#from msilib import schema
import streamlit as st
import snowflake.connector
import numpy as np
import getpass
import socket
from datetime import datetime
import pandas as pd
from datetime import date


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
        
        
        
        st.write(" they oh yeah baby match", selected_option)
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        st.write("user selected", selected_option)
        
        

        ### Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)
        ## Convert the "upc" column to numpy int64 data type, which supports larger integers
        df['UPC'] = df['UPC'].astype(np.int64)

        ## Fill missing and non-numeric values in the "SKU" column with zeros
        df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)

        ## Convert the "SKU" column to np.int64 data type, which supports larger integers
        df['SKU'] = df['SKU'].astype(np.int64)

        ### Print the DataFrame before insertion
        ##print("DataFrame to be inserted:")
        ##print(df)

        ## Log the start of the SQL activity
        description = f"Started {selected_option} Start Archive Process for distro_grid table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        ## Step 1: Fetch data for archiving
        cursor_archive = conn.cursor()
        cursor_archive.execute("SELECT * FROM DISTRO_GRID WHERE STORE_NAME = %s", (selected_option,))
        data_to_archive = cursor_archive.fetchall()

        ## Print the DataFrame before insertion
        ##print(selected_option)
        ##print(data_to_archive)

        # # Step 2: Archive data
        if data_to_archive:
            current_date = date.today().isoformat()

            # Prepare the SQL query for insertion
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
            chunk_size = 1000
            chunks = [data_to_archive_with_date[i:i + chunk_size] for i in range(0, len(data_to_archive_with_date), chunk_size)]

            # Execute the query with parameterized values for each chunk
            cursor_archive = conn.cursor()
            for chunk in chunks:
                cursor_archive.executemany(insert_query, chunk)
            cursor_archive.close()


            # Log the start of the SQL activity
            #description = "Completed the archive process for the distro_grid table"
            description = f"Completed {selected_option} Archive Process for Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''


            # Log the start of the SQL activity
            #description = "Started the removal of archive records from the Distro_Grid Table"
            description = f"Started {selected_option} removal of archived records from the Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''

            # Step 3: Remove archived records from distro_grid table
            cursor_to_remove = conn.cursor()
        if data_to_archive:
            # Prepare the SQL query for deletion
            delete_query = "DELETE FROM DISTRO_GRID WHERE STORE_NAME = %s"
            # Execute the delete query with the selected option (store_name)
            cursor_to_remove.execute(delete_query, (selected_option,))
            # Commit the delete operation
            conn.commit()
            cursor_to_remove.close()

        #Log the start of the SQL activity
        #description = "Started the removal of archive records from the Distro_Grid Table"
        description = f"Completed {selected_option} removal of archive records from the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Log the start of the SQL activity
        description = f"Started {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Generate the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
            INSERT INTO {schema}.{table_name} (
                {', '.join(df.columns)}
            )
            VALUES ({placeholders})
        """
        # Create a cursor object
        cursor = conn.cursor()
        # Chunk the DataFrame into smaller batches
        chunk_size = 1000  # Adjust the chunk size as per your needs
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Execute the query with parameterized values for each chunk
        for chunk in chunks:
            cursor.executemany(insert_query, chunk.values.tolist())
        
       

        ## Log the start of the SQL activity
        description = f"Completed {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        st.write("Data has been imported into Snowflake table:", table_name)

    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while uploading data to Snowflake: {str(e)}")


        # call proceduer to update the distro Grid table with county and update the manufacturer and the product name
    try:
        # Call the procedure
        cursor.execute("CALL UPDATE_DISTRO_GRID()")
        # Fetch and print the result
        result = cursor.fetchone()
        print(result[0])  # Output: Update completed successfully.
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")
               
         # Close the cursor and the connection to Snowflake
        cursor.close()
        conn.close()
        





def upload_FOODMAXX_distro_grid_to_snowflake(df, schema, table_name, selected_option):

    try:

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
        
        
        
        st.write(" they oh yeah baby match", selected_option)
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        st.write("user selected", selected_option)
        
        

        ### Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)
        ## Convert the "upc" column to numpy int64 data type, which supports larger integers
        df['UPC'] = df['UPC'].astype(np.int64)

        ## Fill missing and non-numeric values in the "SKU" column with zeros
        df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)

        ## Convert the "SKU" column to np.int64 data type, which supports larger integers
        df['SKU'] = df['SKU'].astype(np.int64)

        ### Print the DataFrame before insertion
        ##print("DataFrame to be inserted:")
        ##print(df)

        ## Log the start of the SQL activity
        description = f"Started {selected_option} Start Archive Process for distro_grid table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        ## Step 1: Fetch data for archiving
        cursor_archive = conn.cursor()
        cursor_archive.execute("SELECT * FROM DISTRO_GRID WHERE STORE_NAME = %s", (selected_option,))
        data_to_archive = cursor_archive.fetchall()

        ## Print the DataFrame before insertion
        ##print(selected_option)
        ##print(data_to_archive)

        # # Step 2: Archive data
        if data_to_archive:
            current_date = date.today().isoformat()

            # Prepare the SQL query for insertion
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
            chunk_size = 1000
            chunks = [data_to_archive_with_date[i:i + chunk_size] for i in range(0, len(data_to_archive_with_date), chunk_size)]

            # Execute the query with parameterized values for each chunk
            cursor_archive = conn.cursor()
            for chunk in chunks:
                cursor_archive.executemany(insert_query, chunk)
            cursor_archive.close()


            # Log the start of the SQL activity
            #description = "Completed the archive process for the distro_grid table"
            description = f"Completed {selected_option} Archive Process for Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''


            # Log the start of the SQL activity
            #description = "Started the removal of archive records from the Distro_Grid Table"
            description = f"Started {selected_option} removal of archived records from the Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''

            # Step 3: Remove archived records from distro_grid table
            cursor_to_remove = conn.cursor()
        if data_to_archive:
            # Prepare the SQL query for deletion
            delete_query = "DELETE FROM DISTRO_GRID WHERE STORE_NAME = %s"
            # Execute the delete query with the selected option (store_name)
            cursor_to_remove.execute(delete_query, (selected_option,))
            # Commit the delete operation
            conn.commit()
            cursor_to_remove.close()

            #Log the start of the SQL activity
            description = "Started the removal of archive records from the Distro_Grid Table"
        description = f"Completed {selected_option} removal of archive records from the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Log the start of the SQL activity
        description = f"Started {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Generate the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
            INSERT INTO {schema}.{table_name} (
                {', '.join(df.columns)}
            )
            VALUES ({placeholders})
        """
        # Create a cursor object
        cursor = conn.cursor()
        # Chunk the DataFrame into smaller batches
        chunk_size = 1000  # Adjust the chunk size as per your needs
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Execute the query with parameterized values for each chunk
        for chunk in chunks:
            cursor.executemany(insert_query, chunk.values.tolist())
        
       

        ## Log the start of the SQL activity
        description = f"Completed {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        st.write("Data has been imported into Snowflake table:", table_name)

    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while uploading data to Snowflake: {str(e)}")


        # call proceduer to update the distro Grid table with county and update the manufacturer and the product name
    try:
        # Call the procedure
        cursor.execute("CALL UPDATE_DISTRO_GRID()")
        # Fetch and print the result
        result = cursor.fetchone()
        print(result[0])  # Output: Update completed successfully.
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")
               
         # Close the cursor and the connection to Snowflake
        cursor.close()
        conn.close()
        



def upload_WHOLEFOODS_distro_grid_to_snowflake(df, schema, table_name, selected_option):

    try:

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
        
        
        
        #st.write(" they oh yeah baby match", selected_option)
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        st.write("user selected", selected_option)

        ### Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)
        ## Convert the "upc" column to numpy int64 data type, which supports larger integers
        df['UPC'] = df['UPC'].astype(np.int64)

        ## Fill missing and non-numeric values in the "SKU" column with zeros
        df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)

        ## Convert the "SKU" column to np.int64 data type, which supports larger integers
        df['SKU'] = df['SKU'].astype(np.int64)

        ### Print the DataFrame before insertion
        print("DataFrame to be inserted:")
        ##print(df)

        ## Log the start of the SQL activity
        #description = "Starting the archive process for the distro_grid table"
        description = f"Started {selected_option} Start Archive Process for distro_grid table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        ## Step 1: Fetch data for archiving
        cursor_archive = conn.cursor()
        cursor_archive.execute("SELECT * FROM DISTRO_GRID WHERE STORE_NAME = %s", (selected_option,))
        data_to_archive = cursor_archive.fetchall()
        
        st.write(df)

        ## Print the DataFrame before insertion
        ##print(selected_option)
        ##print(data_to_archive)

        # # Step 2: Archive data
        if data_to_archive:
            current_date = date.today().isoformat()

            # Prepare the SQL query for insertion
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
            chunk_size = 1000
            chunks = [data_to_archive_with_date[i:i + chunk_size] for i in range(0, len(data_to_archive_with_date), chunk_size)]

            # Execute the query with parameterized values for each chunk
            cursor_archive = conn.cursor()
            for chunk in chunks:
                cursor_archive.executemany(insert_query, chunk)
            cursor_archive.close()


            # Log the start of the SQL activity
            #description = "Completed the archive process for the distro_grid table"
            description = f"Completed {selected_option} Archive Process for Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''


            # Log the start of the SQL activity
            #description = "Started the removal of archive records from the Distro_Grid Table"
            description = f"Started {selected_option} removal of archived records from the Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''

            # Step 3: Remove archived records from distro_grid table
            cursor_to_remove = conn.cursor()
        if data_to_archive:
            # Prepare the SQL query for deletion
            delete_query = "DELETE FROM DISTRO_GRID WHERE STORE_NAME = %s"
            # Execute the delete query with the selected option (store_name)
            cursor_to_remove.execute(delete_query, (selected_option,))
            # Commit the delete operation
            conn.commit()
            cursor_to_remove.close()

            #Log the start of the SQL activity
            description = "Started the removal of archive records from the Distro_Grid Table"
        description = f"Completed {selected_option} removal of archive records from the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Log the start of the SQL activity
        description = f"Started {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Generate the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
            INSERT INTO {schema}.{table_name} (
                {', '.join(df.columns)}
            )
            VALUES ({placeholders})
        """
        # Create a cursor object
        cursor = conn.cursor()
        # Chunk the DataFrame into smaller batches
        chunk_size = 1000  # Adjust the chunk size as per your needs
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Execute the query with parameterized values for each chunk
        for chunk in chunks:
            cursor.executemany(insert_query, chunk.values.tolist())
        
       

        ## Log the start of the SQL activity
        description = f"Completed {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        st.write("Data has been imported into Snowflake table:", table_name)

    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while uploading data to Snowflake: {str(e)}")


        # call proceduer to update the distro Grid table with county and update the manufacturer and the product name
    try:
        # Call the procedure
        cursor.execute("CALL UPDATE_DISTRO_GRID()")
        # Fetch and print the result
        result = cursor.fetchone()
        print(result[0])  # Output: Update completed successfully.
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")
               
         # Close the cursor and the connection to Snowflake
        cursor.close()
        conn.close()
        

def upload_SAVEMART_distro_grid_to_snowflake(df, schema, table_name, selected_option):

    try:

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
        
        
        
        #st.write(" they oh yeah baby match", selected_option)
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        st.write("user selected", selected_option)

        ### Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)
        ## Convert the "upc" column to numpy int64 data type, which supports larger integers
        df['UPC'] = df['UPC'].astype(np.int64)

        ## Fill missing and non-numeric values in the "SKU" column with zeros
        df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)

        ## Convert the "SKU" column to np.int64 data type, which supports larger integers
        df['SKU'] = df['SKU'].astype(np.int64)

        ### Print the DataFrame before insertion
        print("DataFrame to be inserted:")
        ##print(df)

        ## Log the start of the SQL activity
        description = "Starting the archive process for the distro_grid table"
        description = f"Started {selected_option} Start Archive Process for distro_grid table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        ## Step 1: Fetch data for archiving
        cursor_archive = conn.cursor()
        cursor_archive.execute("SELECT * FROM DISTRO_GRID WHERE STORE_NAME = %s", (selected_option,))
        data_to_archive = cursor_archive.fetchall()
        
        st.write(df)

        ## Print the DataFrame before insertion
        ##print(selected_option)
        ##print(data_to_archive)

        # # Step 2: Archive data
        if data_to_archive:
            current_date = date.today().isoformat()

            # Prepare the SQL query for insertion
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
            chunk_size = 1000
            chunks = [data_to_archive_with_date[i:i + chunk_size] for i in range(0, len(data_to_archive_with_date), chunk_size)]

            # Execute the query with parameterized values for each chunk
            cursor_archive = conn.cursor()
            for chunk in chunks:
                cursor_archive.executemany(insert_query, chunk)
            cursor_archive.close()


            # Log the start of the SQL activity
            #description = "Completed the archive process for the distro_grid table"
            description = f"Completed {selected_option} Archive Process for Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''


            # Log the start of the SQL activity
            #description = "Started the removal of archive records from the Distro_Grid Table"
            description = f"Started {selected_option} removal of archived records from the Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''

            # Step 3: Remove archived records from distro_grid table
            cursor_to_remove = conn.cursor()
        if data_to_archive:
            # Prepare the SQL query for deletion
            delete_query = "DELETE FROM DISTRO_GRID WHERE STORE_NAME = %s"
            # Execute the delete query with the selected option (store_name)
            cursor_to_remove.execute(delete_query, (selected_option,))
            # Commit the delete operation
            conn.commit()
            cursor_to_remove.close()

            #Log the start of the SQL activity
            description = "Started the removal of archive records from the Distro_Grid Table"
        description = f"Completed {selected_option} removal of archive records from the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Log the start of the SQL activity
        description = f"Started {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Generate the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
            INSERT INTO {schema}.{table_name} (
                {', '.join(df.columns)}
            )
            VALUES ({placeholders})
        """
        # Create a cursor object
        cursor = conn.cursor()
        # Chunk the DataFrame into smaller batches
        chunk_size = 1000  # Adjust the chunk size as per your needs
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Execute the query with parameterized values for each chunk
        for chunk in chunks:
            cursor.executemany(insert_query, chunk.values.tolist())
        
       

        ## Log the start of the SQL activity
        description = f"Completed {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        st.write("Data has been imported into Snowflake table:", table_name)

    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while uploading data to Snowflake: {str(e)}")


        # call proceduer to update the distro Grid table with county and update the manufacturer and the product name
    try:
        # Call the procedure
        cursor.execute("CALL UPDATE_DISTRO_GRID()")
        # Fetch and print the result
        result = cursor.fetchone()
        print(result[0])  # Output: Update completed successfully.
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")
               
         # Close the cursor and the connection to Snowflake
        cursor.close()
        conn.close()
        
def upload_SMART_FINAL_distro_grid_to_snowflake(df, schema, table_name, selected_option):

    try:

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
        
        
        
        #st.write(" they oh yeah baby match", selected_option)
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        st.write("user selected", selected_option)

        ### Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)
        ## Convert the "upc" column to numpy int64 data type, which supports larger integers
        df['UPC'] = df['UPC'].astype(np.int64)

        ## Fill missing and non-numeric values in the "SKU" column with zeros
        df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)

        ## Convert the "SKU" column to np.int64 data type, which supports larger integers
        df['SKU'] = df['SKU'].astype(np.int64)

        ### Print the DataFrame before insertion
        print("DataFrame to be inserted:")
        ##print(df)

        ## Log the start of the SQL activity
        description = "Starting the archive process for the distro_grid table"
        description = f"Started {selected_option} Start Archive Process for distro_grid table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        ## Step 1: Fetch data for archiving
        cursor_archive = conn.cursor()
        cursor_archive.execute("SELECT * FROM DISTRO_GRID WHERE STORE_NAME = %s", (selected_option,))
        data_to_archive = cursor_archive.fetchall()
        
        st.write(df)

        ## Print the DataFrame before insertion
        ##print(selected_option)
        ##print(data_to_archive)

        # # Step 2: Archive data
        if data_to_archive:
            current_date = date.today().isoformat()

            # Prepare the SQL query for insertion
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
            chunk_size = 1000
            chunks = [data_to_archive_with_date[i:i + chunk_size] for i in range(0, len(data_to_archive_with_date), chunk_size)]

            # Execute the query with parameterized values for each chunk
            cursor_archive = conn.cursor()
            for chunk in chunks:
                cursor_archive.executemany(insert_query, chunk)
            cursor_archive.close()


            # Log the start of the SQL activity
            #description = "Completed the archive process for the distro_grid table"
            description = f"Completed {selected_option} Archive Process for Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''


            # Log the start of the SQL activity
            #description = "Started the removal of archive records from the Distro_Grid Table"
            description = f"Started {selected_option} removal of archived records from the Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''

            # Step 3: Remove archived records from distro_grid table
            cursor_to_remove = conn.cursor()
        if data_to_archive:
            # Prepare the SQL query for deletion
            delete_query = "DELETE FROM DISTRO_GRID WHERE STORE_NAME = %s"
            # Execute the delete query with the selected option (store_name)
            cursor_to_remove.execute(delete_query, (selected_option,))
            # Commit the delete operation
            conn.commit()
            cursor_to_remove.close()

            #Log the start of the SQL activity
            description = "Started the removal of archive records from the Distro_Grid Table"
        description = f"Completed {selected_option} removal of archive records from the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Log the start of the SQL activity
        description = f"Started {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Generate the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
            INSERT INTO {schema}.{table_name} (
                {', '.join(df.columns)}
            )
            VALUES ({placeholders})
        """
        # Create a cursor object
        cursor = conn.cursor()
        # Chunk the DataFrame into smaller batches
        chunk_size = 1000  # Adjust the chunk size as per your needs
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Execute the query with parameterized values for each chunk
        for chunk in chunks:
            cursor.executemany(insert_query, chunk.values.tolist())
        
       

        ## Log the start of the SQL activity
        description = f"Completed {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        st.write("Data has been imported into Snowflake table:", table_name)

    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while uploading data to Snowflake: {str(e)}")


        # call proceduer to update the distro Grid table with county and update the manufacturer and the product name
    try:
        # Call the procedure
        cursor.execute("CALL UPDATE_DISTRO_GRID()")
        # Fetch and print the result
        result = cursor.fetchone()
        print(result[0])  # Output: Update completed successfully.
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")
               
         # Close the cursor and the connection to Snowflake
        cursor.close()
        conn.close()
        
        


def upload_LUCKYS_distro_grid_to_snowflake(df, schema, table_name, selected_option):

    try:

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
        
        
        
        #st.write(" they oh yeah baby match", selected_option)
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        st.write("user selected", selected_option)

        ### Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)
        ## Convert the "upc" column to numpy int64 data type, which supports larger integers
        df['UPC'] = df['UPC'].astype(np.int64)

        ## Fill missing and non-numeric values in the "SKU" column with zeros
        df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)

        ## Convert the "SKU" column to np.int64 data type, which supports larger integers
        df['SKU'] = df['SKU'].astype(np.int64)

        ### Print the DataFrame before insertion
        print("DataFrame to be inserted:")
        ##print(df)

        ## Log the start of the SQL activity
        description = "Starting the archive process for the distro_grid table"
        description = f"Started {selected_option} Start Archive Process for distro_grid table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        ## Step 1: Fetch data for archiving
        cursor_archive = conn.cursor()
        cursor_archive.execute("SELECT * FROM DISTRO_GRID WHERE STORE_NAME = %s", (selected_option,))
        data_to_archive = cursor_archive.fetchall()
        
        st.write(df)

        ## Print the DataFrame before insertion
        ##print(selected_option)
        ##print(data_to_archive)

        # # Step 2: Archive data
        if data_to_archive:
            current_date = date.today().isoformat()

            # Prepare the SQL query for insertion
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
            chunk_size = 1000
            chunks = [data_to_archive_with_date[i:i + chunk_size] for i in range(0, len(data_to_archive_with_date), chunk_size)]

            # Execute the query with parameterized values for each chunk
            cursor_archive = conn.cursor()
            for chunk in chunks:
                cursor_archive.executemany(insert_query, chunk)
            cursor_archive.close()


            # Log the start of the SQL activity
            #description = "Completed the archive process for the distro_grid table"
            description = f"Completed {selected_option} Archive Process for Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''


            # Log the start of the SQL activity
            #description = "Started the removal of archive records from the Distro_Grid Table"
            description = f"Started {selected_option} removal of archived records from the Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''

            # Step 3: Remove archived records from distro_grid table
            cursor_to_remove = conn.cursor()
        if data_to_archive:
            # Prepare the SQL query for deletion
            delete_query = "DELETE FROM DISTRO_GRID WHERE STORE_NAME = %s"
            # Execute the delete query with the selected option (store_name)
            cursor_to_remove.execute(delete_query, (selected_option,))
            # Commit the delete operation
            conn.commit()
            cursor_to_remove.close()

            #Log the start of the SQL activity
            description = "Started the removal of archive records from the Distro_Grid Table"
        description = f"Completed {selected_option} removal of archive records from the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Log the start of the SQL activity
        description = f"Started {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Generate the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
            INSERT INTO {schema}.{table_name} (
                {', '.join(df.columns)}
            )
            VALUES ({placeholders})
        """
        # Create a cursor object
        cursor = conn.cursor()
        # Chunk the DataFrame into smaller batches
        chunk_size = 1000  # Adjust the chunk size as per your needs
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Execute the query with parameterized values for each chunk
        for chunk in chunks:
            cursor.executemany(insert_query, chunk.values.tolist())
        
       

        ## Log the start of the SQL activity
        description = f"Completed {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        st.write("Data has been imported into Snowflake table:", table_name)

    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while uploading data to Snowflake: {str(e)}")


        # call proceduer to update the distro Grid table with county and update the manufacturer and the product name
    try:
        # Call the procedure
        cursor.execute("CALL UPDATE_DISTRO_GRID()")
        # Fetch and print the result
        result = cursor.fetchone()
        print(result[0])  # Output: Update completed successfully.
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")
               
         # Close the cursor and the connection to Snowflake
        cursor.close()
        conn.close()



def upload_RALEYS_distro_grid_to_snowflake(df, schema, table_name, selected_option):

    try:

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
        
        
        
        #st.write(" they oh yeah baby match", selected_option)
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        st.write("user selected", selected_option)

        ### Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)
        ## Convert the "upc" column to numpy int64 data type, which supports larger integers
        df['UPC'] = df['UPC'].astype(np.int64)

        ## Fill missing and non-numeric values in the "SKU" column with zeros
        df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)

        ## Convert the "SKU" column to np.int64 data type, which supports larger integers
        df['SKU'] = df['SKU'].astype(np.int64)

        ### Print the DataFrame before insertion
        print("DataFrame to be inserted:")
        ##print(df)

        ## Log the start of the SQL activity
        description = "Starting the archive process for the distro_grid table"
        description = f"Started {selected_option} Start Archive Process for distro_grid table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        ## Step 1: Fetch data for archiving
        cursor_archive = conn.cursor()
        cursor_archive.execute("SELECT * FROM DISTRO_GRID WHERE STORE_NAME = %s", (selected_option,))
        data_to_archive = cursor_archive.fetchall()
        
        st.write(df)

        ## Print the DataFrame before insertion
        ##print(selected_option)
        ##print(data_to_archive)

        # # Step 2: Archive data
        if data_to_archive:
            current_date = date.today().isoformat()

            # Prepare the SQL query for insertion
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
            chunk_size = 1000
            chunks = [data_to_archive_with_date[i:i + chunk_size] for i in range(0, len(data_to_archive_with_date), chunk_size)]

            # Execute the query with parameterized values for each chunk
            cursor_archive = conn.cursor()
            for chunk in chunks:
                cursor_archive.executemany(insert_query, chunk)
            cursor_archive.close()


            # Log the start of the SQL activity
            #description = "Completed the archive process for the distro_grid table"
            description = f"Completed {selected_option} Archive Process for Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''


            # Log the start of the SQL activity
            #description = "Started the removal of archive records from the Distro_Grid Table"
            description = f"Started {selected_option} removal of archived records from the Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''

            # Step 3: Remove archived records from distro_grid table
            cursor_to_remove = conn.cursor()
        if data_to_archive:
            # Prepare the SQL query for deletion
            delete_query = "DELETE FROM DISTRO_GRID WHERE STORE_NAME = %s"
            # Execute the delete query with the selected option (store_name)
            cursor_to_remove.execute(delete_query, (selected_option,))
            # Commit the delete operation
            conn.commit()
            cursor_to_remove.close()

            #Log the start of the SQL activity
            description = "Started the removal of archive records from the Distro_Grid Table"
        description = f"Completed {selected_option} removal of archive records from the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Log the start of the SQL activity
        description = f"Started {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Generate the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
            INSERT INTO {schema}.{table_name} (
                {', '.join(df.columns)}
            )
            VALUES ({placeholders})
        """
        # Create a cursor object
        cursor = conn.cursor()
        # Chunk the DataFrame into smaller batches
        chunk_size = 1000  # Adjust the chunk size as per your needs
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Execute the query with parameterized values for each chunk
        for chunk in chunks:
            cursor.executemany(insert_query, chunk.values.tolist())
        
       

        ## Log the start of the SQL activity
        description = f"Completed {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        st.write("Data has been imported into Snowflake table:", table_name)

    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while uploading data to Snowflake: {str(e)}")


        # call proceduer to update the distro Grid table with county and update the manufacturer and the product name
    try:
        # Call the procedure
        cursor.execute("CALL UPDATE_DISTRO_GRID()")
        # Fetch and print the result
        result = cursor.fetchone()
        print(result[0])  # Output: Update completed successfully.
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")
               
         # Close the cursor and the connection to Snowflake
        cursor.close()
        conn.close()
        

def upload_SPROUTS_distro_grid_to_snowflake(df, schema, table_name, selected_option):

    try:

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
        
        
        
        #st.write(" they oh yeah baby match", selected_option)
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        st.write("user selected", selected_option)

        ### Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)
        ## Convert the "upc" column to numpy int64 data type, which supports larger integers
        df['UPC'] = df['UPC'].astype(np.int64)

        ## Fill missing and non-numeric values in the "SKU" column with zeros
        df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)

        ## Convert the "SKU" column to np.int64 data type, which supports larger integers
        df['SKU'] = df['SKU'].astype(np.int64)

        ### Print the DataFrame before insertion
        print("DataFrame to be inserted:")
        ##print(df)

        ## Log the start of the SQL activity
        description = "Starting the archive process for the distro_grid table"
        description = f"Started {selected_option} Start Archive Process for distro_grid table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        ## Step 1: Fetch data for archiving
        cursor_archive = conn.cursor()
        cursor_archive.execute("SELECT * FROM DISTRO_GRID WHERE STORE_NAME = %s", (selected_option,))
        data_to_archive = cursor_archive.fetchall()
        
        st.write(df)

        ## Print the DataFrame before insertion
        ##print(selected_option)
        ##print(data_to_archive)

        # # Step 2: Archive data
        if data_to_archive:
            current_date = date.today().isoformat()

            # Prepare the SQL query for insertion
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
            chunk_size = 1000
            chunks = [data_to_archive_with_date[i:i + chunk_size] for i in range(0, len(data_to_archive_with_date), chunk_size)]

            # Execute the query with parameterized values for each chunk
            cursor_archive = conn.cursor()
            for chunk in chunks:
                cursor_archive.executemany(insert_query, chunk)
            cursor_archive.close()


            # Log the start of the SQL activity
            #description = "Completed the archive process for the distro_grid table"
            description = f"Completed {selected_option} Archive Process for Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''


            # Log the start of the SQL activity
            #description = "Started the removal of archive records from the Distro_Grid Table"
            description = f"Started {selected_option} removal of archived records from the Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''

            # Step 3: Remove archived records from distro_grid table
            cursor_to_remove = conn.cursor()
        if data_to_archive:
            # Prepare the SQL query for deletion
            delete_query = "DELETE FROM DISTRO_GRID WHERE STORE_NAME = %s"
            # Execute the delete query with the selected option (store_name)
            cursor_to_remove.execute(delete_query, (selected_option,))
            # Commit the delete operation
            conn.commit()
            cursor_to_remove.close()

            #Log the start of the SQL activity
            description = "Started the removal of archive records from the Distro_Grid Table"
        description = f"Completed {selected_option} removal of archive records from the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Log the start of the SQL activity
        description = f"Started {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Generate the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
            INSERT INTO {schema}.{table_name} (
                {', '.join(df.columns)}
            )
            VALUES ({placeholders})
        """
        # Create a cursor object
        cursor = conn.cursor()
        # Chunk the DataFrame into smaller batches
        chunk_size = 1000  # Adjust the chunk size as per your needs
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Execute the query with parameterized values for each chunk
        for chunk in chunks:
            cursor.executemany(insert_query, chunk.values.tolist())
        
       

        ## Log the start of the SQL activity
        description = f"Completed {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        st.write("Data has been imported into Snowflake table:", table_name)

    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while uploading data to Snowflake: {str(e)}")


        # call proceduer to update the distro Grid table with county and update the manufacturer and the product name
    try:
        # Call the procedure
        cursor.execute("CALL UPDATE_DISTRO_GRID()")
        # Fetch and print the result
        result = cursor.fetchone()
        print(result[0])  # Output: Update completed successfully.
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")
               
         # Close the cursor and the connection to Snowflake
        cursor.close()
        conn.close()
        

def upload_TARGET_distro_grid_to_snowflake(df, schema, table_name, selected_option):

    try:

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
        
        
        
        #st.write(" they oh yeah baby match", selected_option)
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        st.write("user selected", selected_option)

        ### Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)
        ## Convert the "upc" column to numpy int64 data type, which supports larger integers
        df['UPC'] = df['UPC'].astype(np.int64)

        ## Fill missing and non-numeric values in the "SKU" column with zeros
        df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)

        ## Convert the "SKU" column to np.int64 data type, which supports larger integers
        df['SKU'] = df['SKU'].astype(np.int64)

        ### Print the DataFrame before insertion
        print("DataFrame to be inserted:")
        ##print(df)

        ## Log the start of the SQL activity
        description = "Starting the archive process for the distro_grid table"
        description = f"Started {selected_option} Start Archive Process for distro_grid table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        ## Step 1: Fetch data for archiving
        cursor_archive = conn.cursor()
        cursor_archive.execute("SELECT * FROM DISTRO_GRID WHERE STORE_NAME = %s", (selected_option,))
        data_to_archive = cursor_archive.fetchall()
        
        st.write(df)

        ## Print the DataFrame before insertion
        ##print(selected_option)
        ##print(data_to_archive)

        # # Step 2: Archive data
        if data_to_archive:
            current_date = date.today().isoformat()

            # Prepare the SQL query for insertion
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
            chunk_size = 1000
            chunks = [data_to_archive_with_date[i:i + chunk_size] for i in range(0, len(data_to_archive_with_date), chunk_size)]

            # Execute the query with parameterized values for each chunk
            cursor_archive = conn.cursor()
            for chunk in chunks:
                cursor_archive.executemany(insert_query, chunk)
            cursor_archive.close()


            # Log the start of the SQL activity
            #description = "Completed the archive process for the distro_grid table"
            description = f"Completed {selected_option} Archive Process for Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''


            # Log the start of the SQL activity
            #description = "Started the removal of archive records from the Distro_Grid Table"
            description = f"Started {selected_option} removal of archived records from the Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''

            # Step 3: Remove archived records from distro_grid table
            cursor_to_remove = conn.cursor()
        if data_to_archive:
            # Prepare the SQL query for deletion
            delete_query = "DELETE FROM DISTRO_GRID WHERE STORE_NAME = %s"
            # Execute the delete query with the selected option (store_name)
            cursor_to_remove.execute(delete_query, (selected_option,))
            # Commit the delete operation
            conn.commit()
            cursor_to_remove.close()

            #Log the start of the SQL activity
            description = "Started the removal of archive records from the Distro_Grid Table"
        description = f"Completed {selected_option} removal of archive records from the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Log the start of the SQL activity
        description = f"Started {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Generate the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
            INSERT INTO {schema}.{table_name} (
                {', '.join(df.columns)}
            )
            VALUES ({placeholders})
        """
        # Create a cursor object
        cursor = conn.cursor()
        # Chunk the DataFrame into smaller batches
        chunk_size = 1000  # Adjust the chunk size as per your needs
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Execute the query with parameterized values for each chunk
        for chunk in chunks:
            cursor.executemany(insert_query, chunk.values.tolist())
        
       

        ## Log the start of the SQL activity
        description = f"Completed {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        st.write("Data has been imported into Snowflake table:", table_name)

    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while uploading data to Snowflake: {str(e)}")


        # call proceduer to update the distro Grid table with county and update the manufacturer and the product name
    try:
        # Call the procedure
        cursor.execute("CALL UPDATE_DISTRO_GRID()")
        # Fetch and print the result
        result = cursor.fetchone()
        print(result[0])  # Output: Update completed successfully.
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")
               
         # Close the cursor and the connection to Snowflake
        cursor.close()
        conn.close()
        

def upload_WALMART_distro_grid_to_snowflake(df, schema, table_name, selected_option):

    try:

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
        
        
        
        #st.write(" they oh yeah baby match", selected_option)
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        st.write("user selected", selected_option)

        ### Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)
        ## Convert the "upc" column to numpy int64 data type, which supports larger integers
        df['UPC'] = df['UPC'].astype(np.int64)

        ## Fill missing and non-numeric values in the "SKU" column with zeros
        df['SKU'] = pd.to_numeric(df['SKU'], errors='coerce').fillna(0)

        ## Convert the "SKU" column to np.int64 data type, which supports larger integers
        df['SKU'] = df['SKU'].astype(np.int64)

        ### Print the DataFrame before insertion
        print("DataFrame to be inserted:")
        ##print(df)

        ## Log the start of the SQL activity
        description = "Starting the archive process for the distro_grid table"
        description = f"Started {selected_option} Start Archive Process for distro_grid table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        ## Step 1: Fetch data for archiving
        cursor_archive = conn.cursor()
        cursor_archive.execute("SELECT * FROM DISTRO_GRID WHERE STORE_NAME = %s", (selected_option,))
        data_to_archive = cursor_archive.fetchall()
        
        st.write(df)

        ## Print the DataFrame before insertion
        ##print(selected_option)
        ##print(data_to_archive)

        # # Step 2: Archive data
        if data_to_archive:
            current_date = date.today().isoformat()

            # Prepare the SQL query for insertion
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
            chunk_size = 1000
            chunks = [data_to_archive_with_date[i:i + chunk_size] for i in range(0, len(data_to_archive_with_date), chunk_size)]

            # Execute the query with parameterized values for each chunk
            cursor_archive = conn.cursor()
            for chunk in chunks:
                cursor_archive.executemany(insert_query, chunk)
            cursor_archive.close()


            # Log the start of the SQL activity
            #description = "Completed the archive process for the distro_grid table"
            description = f"Completed {selected_option} Archive Process for Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''


            # Log the start of the SQL activity
            #description = "Started the removal of archive records from the Distro_Grid Table"
            description = f"Started {selected_option} removal of archived records from the Distro_Grid Table"

            insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
            description = ''

            # Step 3: Remove archived records from distro_grid table
            cursor_to_remove = conn.cursor()
        if data_to_archive:
            # Prepare the SQL query for deletion
            delete_query = "DELETE FROM DISTRO_GRID WHERE STORE_NAME = %s"
            # Execute the delete query with the selected option (store_name)
            cursor_to_remove.execute(delete_query, (selected_option,))
            # Commit the delete operation
            conn.commit()
            cursor_to_remove.close()

            #Log the start of the SQL activity
            description = "Started the removal of archive records from the Distro_Grid Table"
        description = f"Completed {selected_option} removal of archive records from the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Log the start of the SQL activity
        description = f"Started {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        # Generate the SQL query
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
            INSERT INTO {schema}.{table_name} (
                {', '.join(df.columns)}
            )
            VALUES ({placeholders})
        """
        # Create a cursor object
        cursor = conn.cursor()
        # Chunk the DataFrame into smaller batches
        chunk_size = 1000  # Adjust the chunk size as per your needs
        chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

        # Execute the query with parameterized values for each chunk
        for chunk in chunks:
            cursor.executemany(insert_query, chunk.values.tolist())
        
       

        ## Log the start of the SQL activity
        description = f"Completed {selected_option} Loading data into the Distro_Grid Table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''

        st.write("Data has been imported into Snowflake table:", table_name)

    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while uploading data to Snowflake: {str(e)}")


        # call proceduer to update the distro Grid table with county and update the manufacturer and the product name
    try:
        # Call the procedure
        cursor.execute("CALL UPDATE_DISTRO_GRID()")
        # Fetch and print the result
        result = cursor.fetchone()
        print(result[0])  # Output: Update completed successfully.
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error: {e}")
               
         # Close the cursor and the connection to Snowflake
        cursor.close()
        conn.close()