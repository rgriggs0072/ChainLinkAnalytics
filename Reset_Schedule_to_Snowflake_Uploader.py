import ipaddress
import streamlit as st
import snowflake.connector
import numpy as np
import getpass
import socket
from datetime import datetime

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
# Function to upload Reset Schedule Data to Snowflake for 'SAFEWAY'
#====================================================================================================================

def upload_reset_SCH_SAFEWAY_data(df, warehouse, database, schema):

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
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        # Access the selected_option from the session state
        selected_option = st.session_state.selected_option
        st.write(f"You selected: {selected_option}")

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} delete from reset table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''
        # Remove data from the table where STORE_NAME is equal to selected_option
        remove_query = f"""
        DELETE FROM {schema}.RESET_SCHEDULE
        WHERE CHAIN_NAME = '{selected_option}'
        """
        description = f"Completed {selected_option} delete from reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        cursor = conn.cursor()
        cursor.execute(remove_query)
        cursor.close()
     

       # Write DataFrame to Snowflake
        cursor = conn.cursor()

        ## Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)

        ## Convert timestamp values to strings
        df = df.astype({'RESET_DATE': str, 'RESET_TIME': str})

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Generate the SQL query for INSERT
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
        INSERT INTO {schema}.RESET_SCHEDULE
        VALUES ({placeholders})
        """

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Completed {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Execute the query with parameterized values
        cursor.executemany(insert_query, df.values.tolist())
        cursor.close()


        

        conn.close()

        st.success("Data has been successfully written to Snowflake.")
    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while writing to Snowflake: {str(e)}")
    




#=============================================================================================================================
# End Function to load data into Snowflake reset_schedule table for SAFEWAY
# ============================================================================================================================ 


#----------------------------------------------------------------------------------------------------------------------------

#=============================================================================================================================
# Function to load data into Snowflake reset_schedule table for lUCKY
# ============================================================================================================================ 
def upload_reset_SCH_LUCKY_data(df, warehouse, database, schema):

    try: 
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        # Access the selected_option from the session state
        selected_option = st.session_state.selected_option
        st.write(f"You selected: {selected_option}")

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} delete from reset table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''
        # Remove data from the table where STORE_NAME is equal to selected_option
        remove_query = f"""
        DELETE FROM {schema}.RESET_SCHEDULE
        WHERE CHAIN_NAME = '{selected_option}'
        """
        description = f"Completed {selected_option} delete from reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        cursor = conn.cursor()
        cursor.execute(remove_query)
        cursor.close()
     

       # Write DataFrame to Snowflake
        cursor = conn.cursor()

        ## Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)

        ## Convert timestamp values to strings
        df = df.astype({'RESET_DATE': str, 'RESET_TIME': str})

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Generate the SQL query for INSERT
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
        INSERT INTO {schema}.RESET_SCHEDULE
        VALUES ({placeholders})
        """

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Completed {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Execute the query with parameterized values
        cursor.executemany(insert_query, df.values.tolist())
        cursor.close()


        # Log the successful completion of the SQL activity
        #insert_log_entry("SQL Activity", "Completed FOODMAXX upload", True, selected_option)

        conn.close()

        st.success("Data has been successfully written to Snowflake.")
    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while writing to Snowflake: {str(e)}")




#=============================================================================================================================
# End Function to load data into Snowflake reset_schedule table for Luckys
# ============================================================================================================================ 

#-----------------------------------------------------------------------------------------------------------------------------

#=============================================================================================================================
# Function to load data into Snowflake reset_schedule table for Walmart
# ============================================================================================================================ 
def upload_reset_SCH_WALMART_data(df, warehouse, database, schema):

    try: 
        
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        # Access the selected_option from the session state
        selected_option = st.session_state.selected_option
        st.write(f"You selected: {selected_option}")

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} delete from reset table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''
        # Remove data from the table where STORE_NAME is equal to selected_option
        remove_query = f"""
        DELETE FROM {schema}.RESET_SCHEDULE
        WHERE CHAIN_NAME = '{selected_option}'
        """
        description = f"Completed {selected_option} delete from reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        cursor = conn.cursor()
        cursor.execute(remove_query)
        cursor.close()
     

       # Write DataFrame to Snowflake
        cursor = conn.cursor()

        ## Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)

        ## Convert timestamp values to strings
        df = df.astype({'RESET_DATE': str, 'RESET_TIME': str})

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Generate the SQL query for INSERT
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
        INSERT INTO {schema}.RESET_SCHEDULE
        VALUES ({placeholders})
        """

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Completed {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Execute the query with parameterized values
        cursor.executemany(insert_query, df.values.tolist())
        cursor.close()


        # Log the successful completion of the SQL activity
        #insert_log_entry("SQL Activity", "Completed FOODMAXX upload", True, selected_option)

        conn.close()

        st.success("Data has been successfully written to Snowflake.")
    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while writing to Snowflake: {str(e)}")

#=============================================================================================================================
# End Function to load data into Snowflake reset_schedule table for Walmart
# ============================================================================================================================ 

#-----------------------------------------------------------------------------------------------------------------------------

#=============================================================================================================================
# Function to load data into Snowflake reset_schedule table for Raleys
# ============================================================================================================================ 

def upload_reset_SCH_RALEYS_data(df, warehouse, database, schema):

    try: 
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        # Access the selected_option from the session state
        selected_option = st.session_state.selected_option
        st.write(f"You selected: {selected_option}")

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} delete from reset table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''
        # Remove data from the table where STORE_NAME is equal to selected_option
        remove_query = f"""
        DELETE FROM {schema}.RESET_SCHEDULE
        WHERE CHAIN_NAME = '{selected_option}'
        """
        description = f"Completed {selected_option} delete from reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        cursor = conn.cursor()
        cursor.execute(remove_query)
        cursor.close()
     

       # Write DataFrame to Snowflake
        cursor = conn.cursor()

        ## Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)

        ## Convert timestamp values to strings
        df = df.astype({'RESET_DATE': str, 'RESET_TIME': str})

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Generate the SQL query for INSERT
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
        INSERT INTO {schema}.RESET_SCHEDULE
        VALUES ({placeholders})
        """

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Completed {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Execute the query with parameterized values
        cursor.executemany(insert_query, df.values.tolist())
        cursor.close()


        # Log the successful completion of the SQL activity
        #insert_log_entry("SQL Activity", "Completed FOODMAXX upload", True, selected_option)

        conn.close()

        st.success("Data has been successfully written to Snowflake.")
    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while writing to Snowflake: {str(e)}")


#=============================================================================================================================
# End Function to load data into Snowflake reset_schedule table for Raleys
# ============================================================================================================================ 

#-----------------------------------------------------------------------------------------------------------------------------

#=============================================================================================================================
# Function to load data into Snowflake reset_schedule table for FoodMaxx
# ============================================================================================================================ 

def upload_reset_SCH_FOODMAXX_data(df, warehouse, database, schema):

    try: 
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        # Access the selected_option from the session state
        selected_option = st.session_state.selected_option
        st.write(f"You selected: {selected_option}")

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} delete from reset table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''
        # Remove data from the table where STORE_NAME is equal to selected_option
        remove_query = f"""
        DELETE FROM {schema}.RESET_SCHEDULE
        WHERE CHAIN_NAME = '{selected_option}'
        """
        description = f"Completed {selected_option} delete from reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        cursor = conn.cursor()
        cursor.execute(remove_query)
        cursor.close()
     

       # Write DataFrame to Snowflake
        cursor = conn.cursor()

        ## Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)

        ## Convert timestamp values to strings
        df = df.astype({'RESET_DATE': str, 'RESET_TIME': str})

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Generate the SQL query for INSERT
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
        INSERT INTO {schema}.RESET_SCHEDULE
        VALUES ({placeholders})
        """

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Completed {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Execute the query with parameterized values
        cursor.executemany(insert_query, df.values.tolist())
        cursor.close()


        # Log the successful completion of the SQL activity
        #insert_log_entry("SQL Activity", "Completed FOODMAXX upload", True, selected_option)

        conn.close()

        st.success("Data has been successfully written to Snowflake.")
    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while writing to Snowflake: {str(e)}")

#=============================================================================================================================
# End Function to load data into Snowflake reset_schedule table for FoodMaxx
# ============================================================================================================================ 




#=============================================================================================================================
# Function to load data into Snowflake reset_schedule table for SMART & FINAL
# ============================================================================================================================ 

def upload_reset_SCH_SMART_FINAL_data(df, warehouse, database, schema):

   try: 
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        # Access the selected_option from the session state
        selected_option = st.session_state.selected_option
        st.write(f"You selected: {selected_option}")

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} delete from reset table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''
        # Remove data from the table where STORE_NAME is equal to selected_option
        remove_query = f"""
        DELETE FROM {schema}.RESET_SCHEDULE
        WHERE CHAIN_NAME = '{selected_option}'
        """
        description = f"Completed {selected_option} delete from reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        cursor = conn.cursor()
        cursor.execute(remove_query)
        cursor.close()
     

       # Write DataFrame to Snowflake
        cursor = conn.cursor()

        ## Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)

        ## Convert timestamp values to strings
        df = df.astype({'RESET_DATE': str, 'RESET_TIME': str})

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Generate the SQL query for INSERT
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
        INSERT INTO {schema}.RESET_SCHEDULE
        VALUES ({placeholders})
        """

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Completed {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Execute the query with parameterized values
        cursor.executemany(insert_query, df.values.tolist())
        cursor.close()


        # Log the successful completion of the SQL activity
        #insert_log_entry("SQL Activity", "Completed FOODMAXX upload", True, selected_option)

        conn.close()

        st.success("Data has been successfully written to Snowflake.")
   except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while writing to Snowflake: {str(e)}")

#=============================================================================================================================
# End Function to load data into Snowflake reset_schedule table for SMART & FINAL
# ============================================================================================================================ 

#------------------------------------------------------------------------------------------------------------------------------

#=============================================================================================================================
# Function to load data into Snowflake reset_schedule table for SPROUTS
# ============================================================================================================================ 

def upload_reset_SCH_SPROUTS_data(df, warehouse, database, schema):

   try: 
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        # Access the selected_option from the session state
        selected_option = st.session_state.selected_option
        st.write(f"You selected: {selected_option}")

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} delete from reset table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''
        # Remove data from the table where STORE_NAME is equal to selected_option
        remove_query = f"""
        DELETE FROM {schema}.RESET_SCHEDULE
        WHERE CHAIN_NAME = '{selected_option}'
        """
        description = f"Completed {selected_option} delete from reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        cursor = conn.cursor()
        cursor.execute(remove_query)
        cursor.close()
     

       # Write DataFrame to Snowflake
        cursor = conn.cursor()

        ## Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)

        ## Convert timestamp values to strings
        df = df.astype({'RESET_DATE': str, 'RESET_TIME': str})

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Generate the SQL query for INSERT
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
        INSERT INTO {schema}.RESET_SCHEDULE
        VALUES ({placeholders})
        """

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Completed {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Execute the query with parameterized values
        cursor.executemany(insert_query, df.values.tolist())
        cursor.close()


        # Log the successful completion of the SQL activity
        #insert_log_entry("SQL Activity", "Completed FOODMAXX upload", True, selected_option)

        conn.close()

        st.success("Data has been successfully written to Snowflake.")
   except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while writing to Snowflake: {str(e)}")

#=============================================================================================================================
# End Function to load data into Snowflake reset_schedule table for SPROUTS
# ============================================================================================================================ 

#----------------------------------------------------------------------------------------------------------------------------


#=============================================================================================================================
# Function to load data into Snowflake reset_schedule table for TARGET
# ============================================================================================================================ 

def upload_reset_SCH_TARGET_data(df, warehouse, database, schema):

    try: 
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        # Access the selected_option from the session state
        selected_option = st.session_state.selected_option
        st.write(f"You selected: {selected_option}")

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} delete from reset table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''
        # Remove data from the table where STORE_NAME is equal to selected_option
        remove_query = f"""
        DELETE FROM {schema}.RESET_SCHEDULE
        WHERE CHAIN_NAME = '{selected_option}'
        """
        description = f"Completed {selected_option} delete from reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        cursor = conn.cursor()
        cursor.execute(remove_query)
        cursor.close()
     

       # Write DataFrame to Snowflake
        cursor = conn.cursor()

        ## Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)

        ## Convert timestamp values to strings
        df = df.astype({'RESET_DATE': str, 'RESET_TIME': str})

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Generate the SQL query for INSERT
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
        INSERT INTO {schema}.RESET_SCHEDULE
        VALUES ({placeholders})
        """

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Completed {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Execute the query with parameterized values
        cursor.executemany(insert_query, df.values.tolist())
        cursor.close()


        # Log the successful completion of the SQL activity
        #insert_log_entry("SQL Activity", "Completed FOODMAXX upload", True, selected_option)

        conn.close()

        st.success("Data has been successfully written to Snowflake.")
    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while writing to Snowflake: {str(e)}")

#=============================================================================================================================
# End Function to load data into Snowflake reset_schedule table for TARGET
# ============================================================================================================================ 

#----------------------------------------------------------------------------------------------------------------------------


#=============================================================================================================================
# Function to load data into Snowflake reset_schedule table for WHOLEFOODS
# ============================================================================================================================ 

def upload_reset_SCH_WHOLEFOODS_data(df, warehouse, database, schema):

    try: 
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        # Access the selected_option from the session state
        selected_option = st.session_state.selected_option
        st.write(f"You selected: {selected_option}")

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} delete from reset table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''
        # Remove data from the table where STORE_NAME is equal to selected_option
        remove_query = f"""
        DELETE FROM {schema}.RESET_SCHEDULE
        WHERE CHAIN_NAME = '{selected_option}'
        """
        description = f"Completed {selected_option} delete from reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        cursor = conn.cursor()
        cursor.execute(remove_query)
        cursor.close()
     

       # Write DataFrame to Snowflake
        cursor = conn.cursor()

        ## Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)

        ## Convert timestamp values to strings
        df = df.astype({'RESET_DATE': str, 'RESET_TIME': str})

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Generate the SQL query for INSERT
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
        INSERT INTO {schema}.RESET_SCHEDULE
        VALUES ({placeholders})
        """

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Completed {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Execute the query with parameterized values
        cursor.executemany(insert_query, df.values.tolist())
        cursor.close()


        # Log the successful completion of the SQL activity
        #insert_log_entry("SQL Activity", "Completed FOODMAXX upload", True, selected_option)

        conn.close()

        st.success("Data has been successfully written to Snowflake.")
    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while writing to Snowflake: {str(e)}")

#=============================================================================================================================
# End Function to load data into Snowflake reset_schedule table for WHOLEFOODS
# ============================================================================================================================ 


#----------------------------------------------------------------------------------------------------------------------------


#=============================================================================================================================
# Function to load data into Snowflake reset_schedule table for SAVEMART
# ============================================================================================================================ 

def upload_reset_SCH_SAVEMART_data(df, warehouse, database, schema):

    try: 
        
        user_id = getpass.getuser()
        local_ip = get_local_ip()

        # Access the selected_option from the session state
        selected_option = st.session_state.selected_option
        st.write(f"You selected: {selected_option}")

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} delete from reset table"

        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        description = ''
        # Remove data from the table where STORE_NAME is equal to selected_option
        remove_query = f"""
        DELETE FROM {schema}.RESET_SCHEDULE
        WHERE CHAIN_NAME = '{selected_option}'
        """
        description = f"Completed {selected_option} delete from reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        cursor = conn.cursor()
        cursor.execute(remove_query)
        cursor.close()
     

       # Write DataFrame to Snowflake
        cursor = conn.cursor()

        ## Replace 'NAN' values with NULL
        df = df.replace('NAN', np.nan).fillna(value='', method=None)

        ## Convert timestamp values to strings
        df = df.astype({'RESET_DATE': str, 'RESET_TIME': str})

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Started {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Generate the SQL query for INSERT
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"""
        INSERT INTO {schema}.RESET_SCHEDULE
        VALUES ({placeholders})
        """

        # Log the start of the SQL activity
        #description = "Started Safeway delete from reset table"
        description = f"Completed {selected_option} insert into reset table"
        insert_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)
        # Execute the query with parameterized values
        cursor.executemany(insert_query, df.values.tolist())
        cursor.close()


        # Log the successful completion of the SQL activity
        #insert_log_entry("SQL Activity", "Completed FOODMAXX upload", True, selected_option)

        conn.close()

        st.success("Data has been successfully written to Snowflake.")
    except Exception as e:
        st.exception(e)  # This will display the full traceback of the exception
        st.error(f"An error occurred while writing to Snowflake: {str(e)}")

#=============================================================================================================================
# End Function to load data into Snowflake reset_schedule table for SAVEMART
# ============================================================================================================================ 











