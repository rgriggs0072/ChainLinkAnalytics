import ipaddress
import streamlit as st
import snowflake.connector
import numpy as np
import getpass
import socket
from datetime import datetime
from Home import create_snowflake_connection


def current_timestamp():
    return datetime.now()




# --------------------------------------------------------------------------------------------------------------------

def create_log_entry(user_id, activity_type, description, success, local_ip, selected_option):
    try:
        # Log the SQL activity
        insert_log_entry(user_id, activity_type, description, success, local_ip, selected_option)
    except Exception as log_error:
        st.exception(log_error)
        st.error(f"An error occurred while creating a log entry: {str(log_error)}")


# ====================================================================================================================
# The follwoing function is called to insert information into the log for latter trouble shooting
# ====================================================================================================================

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


# ====================================================================================================================
# END function is called to insert information into the log for later trouble shooting
# ====================================================================================================================

# --------------------------------------------------------------------------------------------------------------------

# ====================================================================================================================
# Function to get the users IP address for later inserting into the log table for trouble shooting
# ====================================================================================================================
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


# ====================================================================================================================
# END OF Function to get the users IP address for later inserting into the log table for trouble shooting
# ====================================================================================================================

# --------------------------------------------------------------------------------------------------------------------

# ====================================================================================================================
# Function to upload Reset Schedule Data into Snowflake for all stores and chains
# ====================================================================================================================

def upload_reset_data(df, selected_chain):
    # Check for empty values in the CHAIN_NAME column and the STORE_NAME column
    if df['CHAIN_NAME'].isnull().any():
        st.warning("CHAIN_NAME field cannot be empty. Please provide a value for CHAIN_NAME empty cell and try again.")
    elif df['STORE_NAME'].isnull().any():
        st.warning(
            "STORE_NAME field cannot be empty.  Please privide a value for the STORE_NAME empty cell and try again.")


    else:
        # Check if the selected chain matches the names in the CHAIN_NAME column
        selected_chain = selected_chain.upper()
        chain_name_matches = df['CHAIN_NAME'].str.upper().eq(selected_chain)
        # st.write('what chain is it',selected_chain)
        # Count the number of mismatches
        num_mismatches = len(chain_name_matches) - chain_name_matches.sum()

        if num_mismatches == 0:
            # All chain names match
            # Continue with the Snowflake upload
            try:
                # Load Snowflake credentials from the secrets.toml file
                conn, connection_id = create_snowflake_connection()

                

                # Get User ID and local ip address to write to log
                user_id = getpass.getuser()
                local_ip = get_local_ip()

                # Access the selected_option from the session state
                selected_option = st.session_state.selected_option
                # st.write(f"You selected who?: {selected_option}")

                # Log the start of the SQL activity
                description = f"Started {selected_option} delete from reset table"
                create_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)

                # Remove data from the table where STORE_NAME is equal to selected_store
                remove_query = f"""
                DELETE FROM RESET_SCHEDULE
                WHERE CHAIN_NAME = '{selected_chain}'
                """

                # Log the completion of the SQL activity
                description = f"Completed {selected_option} delete from reset table"
                create_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)

                cursor = conn.cursor()
                cursor.execute(remove_query)
                cursor.close()

                # Write DataFrame to Snowflake
                cursor = conn.cursor()

                ## Replace 'NAN' values with NULL
                df = df.replace('NAN', np.nan).fillna(value='', method=None)

                ## Convert timestamp values to strings
                df = df.astype({'RESET_DATE': str, 'TIME': str})

                # Log the start of the SQL activity
                description = f"Started {selected_option} insert into reset table"
                create_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)

                # Generate the SQL query for INSERT into the Reset_Schedule table
                placeholders = ', '.join(['%s'] * len(df.columns))
                insert_query = f"""
            INSERT INTO RESET_SCHEDULE
            VALUES ({placeholders})
            """

                # Log the completion of the SQL activity
                description = f"Completed {selected_option} insert into reset table"
                create_log_entry(user_id, "SQL Activity", description, True, local_ip, selected_option)

                # Execute the query with parameterized values
                cursor.executemany(insert_query, df.values.tolist())
                cursor.close()

                # Commit the transaction
                conn.commit()

                # Log the successful completion of the SQL activity
                create_log_entry(user_id, "SQL Activity", "Transaction committed", True, local_ip, selected_option)

                st.success("Data has been successfully written to Snowflake.")
            # except Exception as e:
            except snowflake.connector.errors.ProgrammingError as pe:
                st.error(f"An error occurred while writing to Snowflake: {str(pe)}")

                # Check if the error message contains the string indicating a date, time, or CHAIN_NAME format issue
                if 'Date' in str(pe) and 'is not recognized' in str(pe):
                    st.warning(
                        "Invalid date format in the data. Please ensure all date values are formatted correctly.")
                elif 'Time' in str(pe) and 'is not recognized' in str(pe):
                    st.warning(
                        "Invalid time format in the data. Please ensure all time values are formatted correctly.")
                else:
                    # Log the full error traceback for further investigation
                    st.exception(pe)
            finally:
                # Close the connection
                if conn:
                    conn.close()
        else:
            # There are mismatches, inform the user
            st.warning(
                f"The selected chain ({selected_chain}) does not match {num_mismatches} name(s) in the CHAIN_NAME column. "
                "Please select the correct chain and try again.")
# =============================================================================================================================
# End Function to load data into Snowflake reset_schedule table for FoodMaxx
# ============================================================================================================================ 




